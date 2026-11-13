"""Головна операція C08: POST /v1/rentals – забронювати автомобіль на HOLD_MINUTES хвилин.

Перевірка й зміна ресурсу та запис оренди – в одній транзакції; відповідь 201 – після фіксації.
Інваріант 1 (авто в одній незавершеній оренді) тримає блокування рядка car (FOR UPDATE) і,
як остання лінія, частковий унікальний індекс rental_one_open_per_car.
Інваріант 2 (авто на обслуговуванні не бронюється) перевіряється під тим самим блокуванням.
"""

import uuid
from datetime import timedelta
from typing import Annotated, Any

import psycopg
from fastapi import APIRouter, Depends, Response
from pydantic import BaseModel, ConfigDict, Field

from . import config, db
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
    body: RentalCreate, user: Annotated[User, Depends(current_user)], response: Response
) -> dict[str, Any]:
    rental_id = str(uuid.uuid4())
    try:
        async with db.connection() as conn, conn.transaction():
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

    response.headers["Location"] = f"/v1/rentals/{rental_id}"
    return rental_json(row)


@router.get("/rentals/{rental_id}")
async def get_rental(rental_id: str, user: Annotated[User, Depends(current_user)]) -> dict[str, Any]:
    async with db.connection() as conn:
        cur = await conn.execute("SELECT * FROM rental WHERE id = %s AND user_id = %s", (rental_id, user.id))
        row = await cur.fetchone()
    if row is None:  # чужа оренда – так само 404, щоб не розкривати її існування
        raise Problem(404, "rental-not-found", "Rental not found")
    return rental_json(row)
