"""GET /v1/cars?zone= – вільні автомобілі зони (не на обслуговуванні, без незавершеної оренди).

ЛР3 Web, О3.2: POST /v1/cars/{id}/service – функція персоналу з картки (оператор переводить авто
на обслуговування або повертає з нього). Доступна лише ролі staff, іншим – 403.
"""

from typing import Annotated, Any

from fastapi import APIRouter, Depends, Query
from pydantic import BaseModel, ConfigDict

from . import db
from .problems import Problem
from .users import User, current_user, staff_user

router = APIRouter(prefix="/v1")


def car_json(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": row["id"],
        "zone": row["zone_id"],
        "plate": row["plate"],
        "class": row["class"],
        "charge_pct": row["charge_pct"],
        "status": row["status"],
        "tariff": {
            "per_minute_minor": row["price_min_minor"],
            "per_km_minor": row["price_km_minor"],
            "currency": row["currency"],
        },
    }


@router.get("/cars")
async def free_cars(
    _: Annotated[User, Depends(current_user)],
    zone: Annotated[str, Query(min_length=1, max_length=64)],
) -> dict[str, Any]:
    async with db.connection() as conn:
        cur = await conn.execute("SELECT 1 FROM zone WHERE id = %s", (zone,))
        if await cur.fetchone() is None:
            raise Problem(404, "zone-not-found", "Zone not found", f"zone {zone!r} does not exist")
        # held з минулим expires_at уже не тримає авто, навіть якщо ще не позначене expired
        cur = await conn.execute(
            """
            SELECT c.* FROM car c
            WHERE c.zone_id = %s AND c.status = 'available'
              AND NOT EXISTS (
                SELECT 1 FROM rental r
                WHERE r.car_id = c.id
                  AND (r.status = 'active' OR (r.status = 'held' AND r.expires_at > now())))
            ORDER BY c.id
            """,
            (zone,),
        )
        rows = await cur.fetchall()
    return {"zone": zone, "items": [car_json(r) for r in rows]}


class CarService(BaseModel):
    """in_service: true – на обслуговування (типово), false – повернути в роботу."""

    model_config = ConfigDict(extra="forbid")
    in_service: bool = True


@router.post("/cars/{car_id}/service")
async def set_service(
    car_id: str, staff: Annotated[User, Depends(staff_user)], body: CarService | None = None
) -> dict[str, Any]:
    in_service = True if body is None else body.in_service
    async with db.connection() as conn, conn.transaction():
        # те саме блокування рядка car, що й у бронюванні: інваріант 2 не порушиться між перевіркою й записом
        cur = await conn.execute("SELECT * FROM car WHERE id = %s FOR UPDATE", (car_id,))
        car = await cur.fetchone()
        if car is None:
            raise Problem(404, "car-not-found", "Car not found", f"car {car_id!r} does not exist")
        if in_service:
            cur = await conn.execute(
                "SELECT 1 FROM rental WHERE car_id = %s"
                " AND (status = 'active' OR (status = 'held' AND expires_at > now()))",
                (car_id,),
            )
            if await cur.fetchone() is not None:
                raise Problem(409, "car-busy", "Car is in an open rental", "finish or cancel the rental first")
        cur = await conn.execute(
            "UPDATE car SET status = %s, version = version + 1 WHERE id = %s RETURNING *",
            ("service" if in_service else "available", car_id),
        )
        row = await cur.fetchone()
    return car_json(row) | {"version": row["version"], "changed_by": staff.id}
