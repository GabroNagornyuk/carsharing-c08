# /// script
# requires-python = ">=3.12"
# dependencies = ["httpx>=0.28"]
# ///
"""DPC О3.2: обмеження кількості незавершених задач Ray (IN_FLIGHT_LIMIT).

Сповільнюємо виконання (перевірка посвідчення 2 с замість 0,3–0,9 с) і одночасно надсилаємо
N команд у зону loadtest. Кожні 100 мс читаємо /metrics: скільки задач Ray зараз не завершено,
скільки подано всього, довжину черги ЛР1. Очікування: незавершених задач ніколи не більше за
IN_FLIGHT_LIMIT, решта прийнятих чекає в черзі, а коли черга повна – 429.

    EXECUTION=ray IN_FLIGHT_LIMIT=4 LICENSE_CHECK_MS=2000 docker compose up -d api
    uv run experiments/dpc_in_flight.py --requests 100
"""

import argparse
import asyncio
import json
import time
import uuid
from pathlib import Path

import httpx

from common import DPC, DPC_AUTH


async def run(n: int, seconds: float) -> dict:
    run_id = uuid.uuid4().hex[:6]
    samples: list[dict] = []
    async with httpx.AsyncClient(base_url=DPC, headers=DPC_AUTH, timeout=30) as client:
        m0 = (await client.get("/metrics")).json()
        stop = asyncio.Event()

        async def sample() -> None:
            t0 = time.monotonic()
            while not stop.is_set():
                m = (await client.get("/metrics")).json()
                samples.append({"t": round(time.monotonic() - t0, 2), "in_flight": m["tasks_in_flight"],
                                "submitted": m["tasks_submitted"] - m0["tasks_submitted"], "queue": m["queue_length"]})
                await asyncio.sleep(0.1)

        sampler = asyncio.create_task(sample())
        bodies = [{"command_id": f"if-{run_id}-{i}", "car_id": f"lt-car-{i}", "driver_id": f"d-{i}", "zone": "loadtest"}
                  for i in range(n)]
        replies = await asyncio.gather(*(client.post("/v1/rental-starts", json=b) for b in bodies))
        await asyncio.sleep(seconds)
        stop.set()
        await sampler
        m1 = (await client.get("/metrics")).json()
    codes = [r.status_code for r in replies]
    return {
        "requests": n,
        "accepted_202": codes.count(202),
        "rejected_429": codes.count(429),
        "in_flight_limit": m1["tasks_in_flight_limit"],
        "max_in_flight_seen": max(s["in_flight"] for s in samples),
        "max_in_flight_server": m1["tasks_max_in_flight"],
        "max_queue": max(s["queue"] for s in samples),
        "submitted_after_1s": next((s["submitted"] for s in samples if s["t"] >= 1), None),
        "submitted_after_5s": next((s["submitted"] for s in samples if s["t"] >= 5), None),
        "submitted_total": m1["tasks_submitted"] - m0["tasks_submitted"],
        "samples": samples,
    }


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--requests", type=int, default=100)
    ap.add_argument("--seconds", type=float, default=12)
    ap.add_argument("--label", default="slow")
    args = ap.parse_args()
    res = asyncio.run(run(args.requests, args.seconds))
    print({k: v for k, v in res.items() if k != "samples"})
    out = Path("results") / f"dpc-o3.2-in-flight-{args.label}.json"
    out.parent.mkdir(exist_ok=True)
    out.write_text(json.dumps(res, indent=2, ensure_ascii=False))
    print(f"saved {out}")


if __name__ == "__main__":
    main()
