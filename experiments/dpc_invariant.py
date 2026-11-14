# /// script
# requires-python = ">=3.12"
# dependencies = ["httpx>=0.28", "psycopg[binary]>=3.2"]
# ///
"""DPC О1.4: захист ресурсу zone_cars. Лише тестова зона test-unprotected (cars_total = 5).

Сценарій: скинути зону, одночасно надіслати N команд старту оренди в цю зону, дочекатися
кінцевих станів і прочитати ресурс. Режим задає сервер (RESOURCE_MODE у compose):

    RESOURCE_MODE=unprotected docker compose up -d api   # без синхронізації
    uv run experiments/dpc_invariant.py --label unprotected
    docker compose up -d api                              # робочий режим locked
    uv run experiments/dpc_invariant.py --label locked

Очікування: unprotected – ефектів більше, ніж авто, і rented != effects (invariant_ok=false);
locked – рівно cars_total стартів done, решта rejected, invariant_ok=true.
ЛР3: те саме із задачами Ray (EXECUTION=ray) – мітка --label ray; ресурс змінює приймач.
"""

import argparse
import asyncio
import json
import time
import uuid
from pathlib import Path

import httpx
import psycopg

from common import DB, DPC, DPC_AUTH

ZONE = "test-unprotected"


def reset() -> None:
    with psycopg.connect(DB) as conn:
        conn.execute(
            "DELETE FROM rental_start_effect WHERE zone_id = %s", (ZONE,)
        )
        # ЛР3: рішення приймача й ключі ідемпотентності посилаються на rental_start
        sub = "SELECT id FROM rental_start WHERE zone_id = %s"
        conn.execute(f"DELETE FROM rental_start_decision WHERE command_id IN ({sub})", (ZONE,))
        conn.execute(f"DELETE FROM dpc_idempotency_key WHERE command_id IN ({sub})", (ZONE,))
        conn.execute("DELETE FROM rental_start WHERE zone_id = %s", (ZONE,))
        conn.execute("UPDATE zone_cars SET rented = 0 WHERE zone_id = %s", (ZONE,))


async def run(n: int) -> dict:
    run_id = uuid.uuid4().hex[:6]
    async with httpx.AsyncClient(base_url=DPC, headers=DPC_AUTH, timeout=30) as client:
        bodies = [
            {"command_id": f"inv-{run_id}-{i}", "car_id": f"test-car-{i}", "driver_id": f"d-{i}", "zone": ZONE}
            for i in range(n)
        ]
        replies = await asyncio.gather(*(client.post("/v1/rental-starts", json=b) for b in bodies))
        accepted = [b["command_id"] for b, r in zip(bodies, replies) if r.status_code == 202]

        states: dict[str, str] = {}
        deadline = time.monotonic() + 60
        while len(states) < len(accepted) and time.monotonic() < deadline:
            for cid in accepted:
                if cid not in states:
                    s = (await client.get(f"/v1/rental-starts/{cid}")).json()["state"]
                    if s in ("done", "rejected", "failed"):
                        states[cid] = s
            await asyncio.sleep(0.3)
        zone = (await client.get(f"/v1/zone-cars/{ZONE}")).json()

    counts = {s: list(states.values()).count(s) for s in ("done", "rejected", "failed")}
    return {"requests": n, "accepted": len(accepted), "final": counts, "zone": zone}


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--requests", type=int, default=20)
    ap.add_argument("--repeats", type=int, default=3)
    ap.add_argument("--label", default="run")
    args = ap.parse_args()

    results = []
    for i in range(args.repeats):
        reset()
        res = asyncio.run(run(args.requests))
        results.append(res)
        z = res["zone"]
        print(
            f"[{args.label} #{i + 1}] done={res['final']['done']} rejected={res['final']['rejected']} "
            f"failed={res['final']['failed']} | cars_total={z['cars_total']} rented={z['rented']} "
            f"effects={z['effects']} invariant_ok={z['invariant_ok']}"
        )
    reset()
    out = Path("results") / f"dpc-o1.4-{args.label}.json"
    out.parent.mkdir(exist_ok=True)
    out.write_text(json.dumps(results, indent=2, ensure_ascii=False))
    print(f"saved {out}")


if __name__ == "__main__":
    main()
