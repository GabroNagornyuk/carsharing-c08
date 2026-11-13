# /// script
# requires-python = ">=3.12"
# dependencies = ["httpx>=0.28"]
# ///
"""DPC О1.2: затримка /health за повної черги.

CLIENTS одночасних клієнтів безперервно надсилають команди старту оренди в зону loadtest
(після 429 – одразу наступна спроба), тож черга місткістю QUEUE_CAPACITY увесь час повна.
Паралельно окремий клієнт кожні 20 мс викликає GET /health і міряє час відповіді HTTP
(від надсилання до отримання повної відповіді, на стороні клієнта). Раз на секунду читається
/metrics (довжина черги).

    uv run experiments/dpc_queue_health.py --clients 50 --seconds 30
"""

import argparse
import asyncio
import json
import time
import uuid
from collections import Counter
from pathlib import Path

import httpx

from common import DPC, DPC_AUTH, percentile


async def main(clients: int, seconds: float) -> dict:
    stop = time.monotonic() + seconds
    statuses: Counter[int] = Counter()
    health_ms: list[float] = []
    queue_samples: list[int] = []
    run_id = uuid.uuid4().hex[:6]

    limits = httpx.Limits(max_connections=clients + 4)
    async with httpx.AsyncClient(base_url=DPC, headers=DPC_AUTH, timeout=30, limits=limits) as client:

        async def producer(k: int) -> None:
            i = 0
            while time.monotonic() < stop:
                body = {"command_id": f"load-{run_id}-{k}-{i}", "car_id": f"lt-{k}", "driver_id": f"d-{k}",
                        "zone": "loadtest"}
                r = await client.post("/v1/rental-starts", json=body)
                statuses[r.status_code] += 1
                i += 1
                if r.status_code == 429:
                    await asyncio.sleep(0.01)

        async def prober() -> None:
            await asyncio.sleep(2)  # дати черзі заповнитися
            async with httpx.AsyncClient(base_url=DPC, timeout=10) as own:
                while time.monotonic() < stop:
                    t0 = time.perf_counter()
                    await own.get("/health")
                    health_ms.append((time.perf_counter() - t0) * 1000)
                    await asyncio.sleep(0.02)

        async def sampler() -> None:
            async with httpx.AsyncClient(base_url=DPC, timeout=10) as own:
                while time.monotonic() < stop:
                    queue_samples.append((await own.get("/metrics")).json().get("queue_length", -1))
                    await asyncio.sleep(1)

        await asyncio.gather(*(producer(k) for k in range(clients)), prober(), sampler())
        metrics = (await client.get("/metrics")).json()

    return {
        "clients": clients,
        "seconds": seconds,
        "queue_capacity": metrics.get("queue_capacity"),
        "workers": metrics.get("workers"),
        "post_statuses": dict(statuses),
        "queue_length_samples": queue_samples,
        "health_samples": len(health_ms),
        "health_ms": {q: round(percentile(health_ms, q), 1) for q in (50, 95, 99)} | {"max": round(max(health_ms), 1)},
    }


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--clients", type=int, default=50)
    ap.add_argument("--seconds", type=float, default=30)
    args = ap.parse_args()
    res = asyncio.run(main(args.clients, args.seconds))
    print(json.dumps(res, indent=2))
    out = Path("results") / "dpc-o1.2-health-under-full-queue.json"
    out.parent.mkdir(exist_ok=True)
    out.write_text(json.dumps(res, indent=2))
    print(f"saved {out}")
