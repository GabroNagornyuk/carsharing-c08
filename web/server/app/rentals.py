"""Головна операція C08: POST /v1/rentals – забронювати автомобіль на HOLD_MINUTES хвилин.

Перевірка й зміна ресурсу та запис оренди – в одній транзакції; відповідь 201 – після фіксації.
Інваріант 1 (авто в одній незавершеній оренді) тримає блокування рядка car (FOR UPDATE) і,
як остання лінія, частковий унікальний індекс rental_one_open_per_car.
Інваріант 2 (авто на обслуговуванні не бронюється) перевіряється під тим самим блокуванням.

ЛР2: заголовок Idempotency-Key обов'язковий; ключ, відбиток тіла й відповідь фіксуються в тій
самій транзакції, що й оренда (idempotency.py). Чому блокування рядка, а не умовний запис чи
SERIALIZABLE – у README, розділ «Web · ЛР2».
"""

import uuid
from datetime import timedelta
from typing import Annotated, Any

import psycopg
from fastapi import APIRouter, Depends, Header, Response
from fastapi.responses import JSONResponse
from psycopg import AsyncConnection
from pydantic import BaseModel, ConfigDict, Field

from . import config, db, idempotency
from .problems import Problem
from .users import User, current_user

router = APIRouter(prefix="/v1")


class RentalCreate(BaseModel):
    model_config = ConfigDict(extra="forbid")
    car_id: str = Field(min_length=1, max_length=64)


def rental_json(row: dict[str, Any]) -> dict[str, Any]:
    amount = None
    if row["amount_minor"] is not None:
        amount = {"amount_minor": row["amount_minor"], "currency": row["currency"]}
    return {
        "id": row["id"],
        "car_id": row["car_id"],
        "status": row["status"],
        "version": row["version"],
        "created_at": row["created_at"].isoformat(),
        "expires_at": row["expires_at"].isoformat(),
        "started_at": row["started_at"].isoformat() if row["started_at"] else None,
        "finished_at": row["finished_at"].isoformat() if row["finished_at"] else None,
        "km": row["km"],
        "amount": amount,
    }


@router.post("/rentals", status_code=201)
async def create_rental(
    body: RentalCreate,
    user: Annotated[User, Depends(current_user)],
    idempotency_key: Annotated[str | None, Header()] = None,
) -> JSONResponse:
    key = idempotency.require_key(idempotency_key)

    async def book(conn: AsyncConnection) -> idempotency.Outcome:
        cur = await conn.execute("SELECT id, status FROM car WHERE id = %s FOR UPDATE", (body.car_id,))
        car = await cur.fetchone()
        if car is None:
            raise Problem(404, "car-not-found", "Car not found", f"car {body.car_id!r} does not exist")
        if car["status"] == "service":
            raise Problem(409, "car-in-service", "Car is in service", "a car in service cannot be booked")

        # бронювання, строк якого минув, переходить у expired і звільняє авто
        await conn.execute(
            "UPDATE rental SET status = 'expired', version = version + 1"
            " WHERE car_id = %s AND status = 'held' AND expires_at <= now()",
            (body.car_id,),
        )
        cur = await conn.execute(
            "SELECT 1 FROM rental WHERE car_id = %s AND status IN ('held', 'active')", (body.car_id,)
        )
        if await cur.fetchone() is not None:
            raise Problem(409, "car-busy", "Car is already rented", "the car is in another active rental")

        rental_id = str(uuid.uuid4())
        try:
            async with conn.transaction():
                cur = await conn.execute(
                    """
                    INSERT INTO rental (id, user_id, car_id, status, created_at, expires_at)
                    VALUES (%s, %s, %s, 'held', now(), now() + %s)
                    RETURNING *
                    """,
                    (rental_id, user.id, body.car_id, timedelta(minutes=config.HOLD_MINUTES)),
                )
                row = await cur.fetchone()
        except psycopg.errors.UniqueViolation:
            # другий рубіж інваріанта: індекс rental_one_open_per_car
            raise Problem(409, "car-busy", "Car is already rented", "the car is in another active rental") from None
        return idempotency.Outcome(201, rental_json(row), f"/v1/rentals/{rental_id}")

    return await idempotency.run(
        user_id=user.id, operation="POST /v1/rentals", key=key, body=body.model_dump(),
        path="/v1/rentals", action=book,
    )


@router.get("/rentals")
async def my_rentals(user: Annotated[User, Depends(current_user)]) -> dict[str, Any]:
    """ЛР3, О3.2: мої оренди. Власник – умова самого запиту, тож чужі рядки в список не потрапляють."""
    async with db.connection() as conn:
        cur = await conn.execute(
            "SELECT * FROM rental WHERE user_id = %s ORDER BY created_at DESC, id DESC LIMIT 20", (user.id,)
        )
        rows = await cur.fetchall()
    return {"items": [rental_json(r) for r in rows]}


@router.get("/rentals/{rental_id}")
async def get_rental(
    rental_id: str, user: Annotated[User, Depends(current_user)], response: Response
) -> dict[str, Any]:
    async with db.connection() as conn:
        cur = await conn.execute("SELECT * FROM rental WHERE id = %s AND user_id = %s", (rental_id, user.id))
        row = await cur.fetchone()
    if row is None:  # чужа оренда – так само 404, щоб не розкривати її існування
        raise Problem(404, "rental-not-found", "Rental not found")
    response.headers["ETag"] = etag(row["version"])
    return rental_json(row)


# ---------------------------------------------------------------- ЛР2 Web, Д2.4: продовження з If-Match


class RentalExtend(BaseModel):
    """Змінити можна лише строк бронювання; власник, стан і версія – не поля запиту (інакше 422)."""

    model_config = ConfigDict(extra="forbid")
    extend_minutes: int = Field(ge=1, le=15)


def etag(version: int) -> str:
    return f'"{version}"'


def _parse_if_match(raw: str | None) -> int:
    if raw is None:
        raise Problem(428, "precondition-required", "If-Match header required",
                      "send If-Match with the ETag from GET /v1/rentals/{id}")
    value = raw.strip().removeprefix("W/").strip('"')
    if not value.isdigit():
        raise Problem(412, "precondition-failed", "Rental was changed", "If-Match does not match the current ETag")
    return int(value)


@router.patch("/rentals/{rental_id}")
async def extend_rental(
    rental_id: str,
    body: RentalExtend,
    user: Annotated[User, Depends(current_user)],
    response: Response,
    if_match: Annotated[str | None, Header()] = None,
) -> dict[str, Any]:
    version = _parse_if_match(if_match)
    async with db.connection() as conn:
        # перевірка версії й запис – ОДНА інструкція: між ними ніхто не вклиниться
        cur = await conn.execute(
            """
            UPDATE rental SET expires_at = expires_at + %s, version = version + 1
            WHERE id = %s AND user_id = %s AND version = %s AND status = 'held' AND expires_at > now()
            RETURNING *
            """,
            (timedelta(minutes=body.extend_minutes), rental_id, user.id, version),
        )
        row = await cur.fetchone()
        if row is None:  # чому не вдалося – читаємо поточний стан лише для відповіді
            cur = await conn.execute("SELECT * FROM rental WHERE id = %s AND user_id = %s", (rental_id, user.id))
            current = await cur.fetchone()
    if row is None:
        if current is None:
            raise Problem(404, "rental-not-found", "Rental not found")
        if current["version"] != version:
            raise Problem(412, "precondition-failed", "Rental was changed",
                          f"current ETag is {etag(current['version'])}", headers={"ETag": etag(current["version"])})
        raise Problem(409, "rental-not-held", "Only a valid hold can be extended",
                      f"rental is {current['status']}" + (" (hold expired)" if current["status"] == "held" else ""))
    response.headers["ETag"] = etag(row["version"])
    return rental_json(row)


# ---------------------------------------------------------------- ЛР2 Web, Д2.5: повторне скасування


@router.post("/rentals/{rental_id}/cancel")
async def cancel_rental(rental_id: str, user: Annotated[User, Depends(current_user)], response: Response) -> dict[str, Any]:
    """held -> cancelled умовним записом. Повтор бачить уже зафіксований cancelled і повертає його
    без жодної зміни: авто не «звільняється» вдруге й чужа нова оренда цього авто не зачіпається."""
    async with db.connection() as conn:
        cur = await conn.execute(
            """
            UPDATE rental SET status = 'cancelled', version = version + 1
            WHERE id = %s AND user_id = %s AND status = 'held' AND expires_at > now()
            RETURNING *
            """,
            (rental_id, user.id),
        )
        row = await cur.fetchone()
        repeated = row is None
        if repeated:
            cur = await conn.execute("SELECT * FROM rental WHERE id = %s AND user_id = %s", (rental_id, user.id))
            row = await cur.fetchone()
    if row is None:
        raise Problem(404, "rental-not-found", "Rental not found")
    if repeated and row["status"] != "cancelled":
        raise Problem(409, "rental-not-cancellable", "Rental cannot be cancelled",
                      f"rental is {row['status']}" + (" (hold expired)" if row["status"] == "held" else ""))
    response.headers["ETag"] = etag(row["version"])
    response.headers["X-Already-Cancelled"] = "true" if repeated else "false"
    return rental_json(row)
