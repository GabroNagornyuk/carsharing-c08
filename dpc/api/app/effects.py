"""ЛР3 DPC, О3.3: приймач результатів задач Ray – POST /v1/rental-start-effects.

Одна транзакція робить три речі:
  1. захист від повтору: рядок запиту rental_start блокується (FOR UPDATE), тож дві доставки
     того самого command_id не пройдуть одночасно; якщо рішення для command_id уже є в
     rental_start_decision (первинний ключ – ключ ефекту), повертаємо його й нічого не змінюємо;
  2. зміна ресурсу: рядок zone_cars під FOR UPDATE, перевірка «rented < cars_total», rented + 1,
     рядок ефекту rental_start_effect;
  3. запис рішення: rental_start_decision і кінцевий стан rental_start.
Або все разом, або нічого. Виконавець отримує відповідь лише після фіксації.
"""

import asyncio
from typing import Annotated, Any

from fastapi import APIRouter, Depends
from pydantic import BaseModel, ConfigDict, Field

from . import auth, config, db
from .jsonlog import log
from .problems import Problem

router = APIRouter(prefix="/v1", dependencies=[Depends(auth.require_token)])

_stats = {"accepted": 0, "replayed": 0}


class EffectDelivery(BaseModel):
    model_config = ConfigDict(extra="forbid")
    command_id: Annotated[str, Field(min_length=1, max_length=128)]
    license_ok: bool
    executor: Annotated[str, Field(min_length=1, max_length=200)]


def _decision_json(row: dict[str, Any], replayed: bool) -> dict[str, Any]:
    return {
        "command_id": row["command_id"],
        "decision": row["decision"],
        "reason": row["reason"],
        "deliveries": row["deliveries"],
        "replayed": replayed,
    }


@router.post("/rental-start-effects")
async def receive(body: EffectDelivery) -> dict[str, Any]:
    if config.RECEIVER_DELAY_MS:  # Д3.4: повільний приймач (тут лише параметр, дослід не виконувався)
        await asyncio.sleep(config.RECEIVER_DELAY_MS / 1000)
    async with db.connection() as conn, conn.transaction():
        cur = await conn.execute("SELECT * FROM rental_start WHERE id = %s FOR UPDATE", (body.command_id,))
        req = await cur.fetchone()
        if req is None:
            raise Problem(404, "rental-start-not-found", "Rental start not found")

        # 1. повтор того самого ефекту: збережене рішення, без предметної зміни
        cur = await conn.execute(
            "UPDATE rental_start_decision SET deliveries = deliveries + 1 WHERE command_id = %s RETURNING *",
            (body.command_id,),
        )
        old = await cur.fetchone()
        if old is not None:
            _stats["replayed"] += 1
            log("effect.replayed", command_id=body.command_id, executor=body.executor, deliveries=old["deliveries"])
            return _decision_json(old, replayed=True)

        # 2. зміна ресурсу – лише тут, у приймачі
        decision, reason = "done", None
        if not body.license_ok:
            decision, reason = "rejected", "driver license not confirmed"
        else:
            cur = await conn.execute(
                "SELECT cars_total, rented FROM zone_cars WHERE zone_id = %s FOR UPDATE", (req["zone_id"],)
            )
            zone = await cur.fetchone()
            if zone["rented"] >= zone["cars_total"]:
                decision, reason = "rejected", "no free cars in zone"
            else:
                await conn.execute("UPDATE zone_cars SET rented = rented + 1 WHERE zone_id = %s", (req["zone_id"],))
                await conn.execute(
                    "INSERT INTO rental_start_effect (command_id, zone_id, car_id) VALUES (%s, %s, %s)",
                    (body.command_id, req["zone_id"], req["car_id"]),
                )

        # 3. рішення й кінцевий стан запиту – у тій самій транзакції
        cur = await conn.execute(
            "INSERT INTO rental_start_decision (command_id, decision, reason, executor)"
            " VALUES (%s, %s, %s, %s) RETURNING *",
            (body.command_id, decision, reason, body.executor),
        )
        row = await cur.fetchone()
        await conn.execute(
            "UPDATE rental_start SET state = %s, reason = %s, finished_at = now() WHERE id = %s",
            (decision, reason, body.command_id),
        )
    _stats["accepted"] += 1
    log("effect.accepted", command_id=body.command_id, decision=decision, executor=body.executor)
    return _decision_json(row, replayed=False)


@router.get("/rental-start-effects/{command_id}")
async def get_decision(command_id: str) -> dict[str, Any]:
    """Рішення приймача й кількість доставок – для перевірки О3.4."""
    async with db.connection() as conn:
        cur = await conn.execute(
            """
            SELECT d.*, (SELECT count(*) FROM rental_start_effect e WHERE e.command_id = d.command_id) AS effects
            FROM rental_start_decision d WHERE d.command_id = %s
            """,
            (command_id,),
        )
        row = await cur.fetchone()
    if row is None:
        raise Problem(404, "decision-not-found", "No decision for this command yet")
    return _decision_json(row, replayed=False) | {
        "executor": row["executor"],
        "effects": row["effects"],
        "acked_at": row["acked_at"].isoformat() if row["acked_at"] else None,
        "acked_by": row["acked_by"],
    }
