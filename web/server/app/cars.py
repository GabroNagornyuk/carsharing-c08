"""GET /v1/cars?zone= – вільні автомобілі зони (не на обслуговуванні, без незавершеної оренди)."""

from typing import Annotated, Any

from fastapi import APIRouter, Depends, Query

from . import db
from .problems import Problem
from .users import User, current_user

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
