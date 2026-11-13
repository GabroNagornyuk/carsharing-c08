"""C08, частина DPC, ЛР1: приймання команд старту оренди в обмежену чергу й пул обробників.

POST /v1/rental-starts -> 202 (queued) або 429 з Retry-After, якщо черга повна.
Обробник: queued -> processing -> повільний виклик (перевірка посвідчення, поза критичною
секцією) -> критична секція над ресурсом zone_cars -> done | rejected | failed.

Синхронізація:
  * черга – asyncio.Queue(maxsize=QUEUE_CAPACITY). Обробник чекає на `await queue.get()`:
    корутина призупиняється на внутрішньому Future черги (умовна змінна циклу подій) і не
    займає потік, доки put() її не розбудить;
  * місце в черзі резервується лічильником _reserved без жодного await між перевіркою та
    резервуванням – у циклі подій ця ділянка атомарна, тож дві паралельні відповіді не займуть
    одне місце;
  * ресурс zone_cars – блокування рядка PostgreSQL (SELECT ... FOR UPDATE) в одній транзакції
    з перевіркою «rented < cars_total», збільшенням лічильника та записом ефекту. Блокування в
    базі, а не в процесі, діє й для інших процесів (задачі Ray у ЛР3).
"""

import asyncio
import random
import time
from typing import Annotated, Any

from fastapi import APIRouter, Depends, Response
from fastapi.responses import JSONResponse
from pydantic import BaseModel, ConfigDict, Field

from . import auth, config, db, metrics
from .jsonlog import log
from .problems import Problem

router = APIRouter(prefix="/v1", dependencies=[Depends(auth.require_token)])

FINAL = {"done", "rejected", "failed"}
ID = Annotated[str, Field(min_length=1, max_length=128, pattern=r"^[A-Za-z0-9._:-]+$")]

_queue: asyncio.Queue[str] | None = None
_reserved = 0  # місця, зарезервовані запитами, які ще не поставили id у чергу
_workers: list[asyncio.Task[None]] = []
_busy = 0
_counters = {"rejected_429": 0, "rejected_domain": 0, "done": 0, "failed": 0}


class RentalStartCreate(BaseModel):
    model_config = ConfigDict(extra="forbid")
    command_id: ID
    car_id: ID
    driver_id: ID
    zone: ID


def _state_json(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": row["id"],
        "command_id": row["id"],
        "car_id": row["car_id"],
        "driver_id": row["driver_id"],
        "zone": row["zone_id"],
        "state": row["state"],
        "reason": row["reason"],
        "arrived_at": row["arrived_at"].isoformat(),
        "started_at": row["started_at"].isoformat() if row["started_at"] else None,
        "finished_at": row["finished_at"].isoformat() if row["finished_at"] else None,
    }


def _queue_length() -> int:
    return _queue.qsize() if _queue else 0


# ---------------------------------------------------------------- приймання


@router.post("/rental-starts", status_code=202)
async def accept(body: RentalStartCreate, response: Response) -> Any:
    global _reserved
    assert _queue is not None

    # перевірка місця й резервування – без await між ними
    if _queue.qsize() + _reserved >= config.QUEUE_CAPACITY:
        _counters["rejected_429"] += 1
        raise Problem(429, "queue-full", "Queue is full", "try again later", headers={"Retry-After": "1"})
    _reserved += 1
    try:
        async with db.connection() as conn, conn.transaction():
            cur = await conn.execute("SELECT 1 FROM zone_cars WHERE zone_id = %s", (body.zone,))
            if await cur.fetchone() is None:
                raise Problem(422, "unknown-zone", "Unknown zone", f"zone {body.zone!r} has no zone_cars resource")
            cur = await conn.execute(
                """
                INSERT INTO rental_start (id, car_id, driver_id, zone_id, state)
                VALUES (%s, %s, %s, %s, 'queued')
                ON CONFLICT (id) DO NOTHING
                RETURNING *
                """,
                (body.command_id, body.car_id, body.driver_id, body.zone),
            )
            row = await cur.fetchone()
            if row is None:
                # повтор тієї самої команди: стан без другого запису; інший зміст – конфлікт
                cur = await conn.execute("SELECT * FROM rental_start WHERE id = %s", (body.command_id,))
                old = await cur.fetchone()
                same = (old["car_id"], old["driver_id"], old["zone_id"]) == (body.car_id, body.driver_id, body.zone)
                if not same:
                    raise Problem(409, "command-id-reused", "command_id already used with other data")
                return JSONResponse(
                    _state_json(old), status_code=200, headers={"Location": f"/v1/rental-starts/{old['id']}"}
                )
        _queue.put_nowait(row["id"])  # місце зарезервоване, QueueFull неможливий
    finally:
        _reserved -= 1

    response.headers["Location"] = f"/v1/rental-starts/{row['id']}"
    return _state_json(row)


@router.get("/rental-starts/{command_id}")
async def get_state(command_id: str) -> dict[str, Any]:
    async with db.connection() as conn:
        cur = await conn.execute("SELECT * FROM rental_start WHERE id = %s", (command_id,))
        row = await cur.fetchone()
    if row is None:
        raise Problem(404, "rental-start-not-found", "Rental start not found")
    return _state_json(row)


@router.get("/zone-cars/{zone_id}")
async def get_zone(zone_id: str) -> dict[str, Any]:
    async with db.connection() as conn:
        cur = await conn.execute(
            """
            SELECT z.zone_id, z.cars_total, z.rented,
                   (SELECT count(*) FROM rental_start_effect e WHERE e.zone_id = z.zone_id) AS effects
            FROM zone_cars z WHERE z.zone_id = %s
            """,
            (zone_id,),
        )
        row = await cur.fetchone()
    if row is None:
        raise Problem(404, "zone-not-found", "Zone not found")
    return {
        "zone": row["zone_id"],
        "cars_total": row["cars_total"],
        "rented": row["rented"],
        "effects": row["effects"],
        "free": row["cars_total"] - row["rented"],
        # інваріант: лічильник дорівнює кількості ефектів і не перевищує кількість авто
        "invariant_ok": row["rented"] == row["effects"] and row["effects"] <= row["cars_total"],
    }


# ---------------------------------------------------------------- обробники


def _license_check_seconds() -> float:
    low, _, high = config.LICENSE_CHECK_MS.partition("-")
    return random.uniform(int(low), int(high or low)) / 1000


async def _license_check(driver_id: str) -> bool:
    """Повільний виклик картки: перевірка посвідчення водія (300–900 мс). Навчальне правило:
    посвідчення водіїв з id на 'nolicense' не підтверджується."""
    await asyncio.sleep(_license_check_seconds())
    return not driver_id.startswith("nolicense")


async def _finish(conn: Any, command_id: str, state: str, reason: str | None) -> None:
    await conn.execute(
        "UPDATE rental_start SET state = %s, reason = %s, finished_at = now() WHERE id = %s",
        (state, reason, command_id),
    )


async def _apply_locked(command_id: str, zone_id: str, car_id: str) -> str:
    """Критична секція: перевірка й зміна ресурсу та запис ефекту – одна транзакція під FOR UPDATE."""
    async with db.connection() as conn, conn.transaction():
        cur = await conn.execute(
            "SELECT cars_total, rented FROM zone_cars WHERE zone_id = %s FOR UPDATE", (zone_id,)
        )
        zone = await cur.fetchone()
        if zone["rented"] >= zone["cars_total"]:
            await _finish(conn, command_id, "rejected", "no free cars in zone")
            return "rejected"
        await conn.execute("UPDATE zone_cars SET rented = rented + 1 WHERE zone_id = %s", (zone_id,))
        await conn.execute(
            "INSERT INTO rental_start_effect (command_id, zone_id, car_id) VALUES (%s, %s, %s)",
            (command_id, zone_id, car_id),
        )
        await _finish(conn, command_id, "done", None)
        return "done"


async def _apply_unprotected(command_id: str, zone_id: str, car_id: str) -> str:
    """Режим досліду О1.4 (RESOURCE_MODE=unprotected): ті самі кроки без блокування й без
    спільної транзакції – «перевірити, потім записати» з паузою між ними. Лише на тестових даних."""
    async with db.connection() as conn:
        await conn.set_autocommit(True)
        try:
            cur = await conn.execute("SELECT cars_total, rented FROM zone_cars WHERE zone_id = %s", (zone_id,))
            zone = await cur.fetchone()
            if zone["rented"] >= zone["cars_total"]:
                await _finish(conn, command_id, "rejected", "no free cars in zone")
                return "rejected"
            await asyncio.sleep(config.UNPROTECTED_GAP_MS / 1000)
            await conn.execute(
                "UPDATE zone_cars SET rented = %s WHERE zone_id = %s", (zone["rented"] + 1, zone_id)
            )
            await conn.execute(
                "INSERT INTO rental_start_effect (command_id, zone_id, car_id) VALUES (%s, %s, %s)",
                (command_id, zone_id, car_id),
            )
            await _finish(conn, command_id, "done", None)
            return "done"
        finally:
            await conn.set_autocommit(False)


async def _process(command_id: str) -> None:
    started = time.monotonic()
    async with db.connection() as conn, conn.transaction():
        cur = await conn.execute(
            "UPDATE rental_start SET state = 'processing', started_at = now()"
            " WHERE id = %s AND state = 'queued' RETURNING car_id, driver_id, zone_id",
            (command_id,),
        )
        req = await cur.fetchone()
    if req is None:
        return  # уже оброблено

    # повільний виклик – поза критичною секцією, не тримає ні блокування, ні з'єднання з базою
    if not await _license_check(req["driver_id"]):
        async with db.connection() as conn, conn.transaction():
            await _finish(conn, command_id, "rejected", "driver license not confirmed")
        outcome = "rejected"
    elif config.RESOURCE_MODE == "unprotected":
        outcome = await _apply_unprotected(command_id, req["zone_id"], req["car_id"])
    else:
        outcome = await _apply_locked(command_id, req["zone_id"], req["car_id"])

    _counters["done" if outcome == "done" else "rejected_domain"] += 1
    log("rental_start.finished", command_id=command_id, state=outcome,
        processing_ms=round((time.monotonic() - started) * 1000, 1))


async def _worker(n: int) -> None:
    global _busy
    assert _queue is not None
    while True:
        command_id = await _queue.get()
        _busy += 1
        try:
            await _process(command_id)
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001 - запит має дійти до кінцевого стану
            _counters["failed"] += 1
            log("rental_start.failed", command_id=command_id, worker=n, error=type(exc).__name__)
            try:
                async with db.connection() as conn, conn.transaction():
                    await _finish(conn, command_id, "failed", type(exc).__name__)
            except Exception:  # noqa: BLE001
                pass
        finally:
            _busy -= 1
            _queue.task_done()


async def start() -> None:
    global _queue
    _queue = asyncio.Queue(maxsize=config.QUEUE_CAPACITY)
    # черга живе в пам'яті процесу: запити, що лишилися незавершеними після аварії, позначаються failed
    try:
        async with db.connection() as conn, conn.transaction():
            await conn.execute(
                "UPDATE rental_start SET state = 'failed', reason = 'lost on api restart', finished_at = now()"
                " WHERE state IN ('queued', 'processing')"
            )
    except Exception as exc:  # noqa: BLE001 - база ще може стартувати; /health має працювати
        log("rental_start.recover_skipped", error=type(exc).__name__)
    _workers.extend(asyncio.create_task(_worker(i)) for i in range(config.WORKERS))
    metrics.gauge("queue_length", _queue_length)
    metrics.gauge("queue_capacity", lambda: config.QUEUE_CAPACITY)
    metrics.gauge("workers", lambda: config.WORKERS)
    metrics.gauge("processing", lambda: _busy)
    metrics.gauge("rejected", lambda: _counters["rejected_429"] + _counters["rejected_domain"])
    metrics.gauge("rejected_queue_full", lambda: _counters["rejected_429"])
    log("rental_start.pool_started", workers=config.WORKERS, capacity=config.QUEUE_CAPACITY,
        resource_mode=config.RESOURCE_MODE)


async def stop() -> None:
    for task in _workers:
        task.cancel()
    await asyncio.gather(*_workers, return_exceptions=True)
    _workers.clear()
