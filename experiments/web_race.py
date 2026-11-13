# /// script
# requires-python = ">=3.12"
# dependencies = ["httpx>=0.28", "psycopg[binary]>=3.2"]
# ///
"""Web О2.1: «двоє на один автомобіль» через ОБИДВА екземпляри, перевірка підсумкового стану.

    uv run experiments/web_race.py --rounds 5 --clients 20

У кожному раунді – перше вільне авто аеропорту; --clients одночасних бронювань двох водіїв,
половина напряму на app-a (:8001), половина на app-b (:8002), кожне зі своїм Idempotency-Key.
Підсумковий стан читається з бази: незавершених оренд цього авто має бути рівно 1, і авто
зникає зі списку вільних. Створені оренди наприкінці переводяться в cancelled.
"""

import argparse
import asyncio
import json
from collections import Counter
from pathlib import Path

import httpx
import psycopg
from common import DB, WEB_A, WEB_B, idem, user


async def one_round(car: str, clients: int) -> dict:
    async with httpx.AsyncClient(timeout=15) as c:
        async def book(i: int) -> tuple[str, httpx.Response]:
            base = WEB_A if i % 2 == 0 else WEB_B
            r = await c.post(f"{base}/v1/rentals", json={"car_id": car},
                             headers={**user(f"u-driver-{1 + i // 2 % 2}"), **idem()})
            return r.headers.get("x-instance", base), r

        replies = await asyncio.gather(*(book(i) for i in range(clients)))
    with psycopg.connect(DB) as conn:
        open_rentals = conn.execute(
            "SELECT count(*) FROM rental WHERE car_id = %s AND status IN ('held', 'active')", (car,)
        ).fetchone()[0]
    with httpx.Client(base_url=WEB_A) as c:
        free = {x["id"] for x in c.get("/v1/cars", params={"zone": "airport"}, headers=user("u-driver-1")).json()["items"]}
    created = [r.json()["id"] for _, r in replies if r.status_code == 201]
    return {
        "car": car,
        "codes": dict(Counter(r.status_code for _, r in replies)),
        "by_instance": dict(Counter(f"{inst}:{r.status_code}" for inst, r in replies)),
        "open_rentals_in_db": open_rentals,
        "car_still_listed_free": car in free,
        "ok": len(created) == 1 and open_rentals == 1 and car not in free
              and all(r.status_code in (201, 409) for _, r in replies),
        "created": created,
    }


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--rounds", type=int, default=5)
    ap.add_argument("--clients", type=int, default=20)
    ap.add_argument("--out", default="results/web-o2.1-race.json")
    args = ap.parse_args()

    rounds, created = [], []
    try:
        for _ in range(args.rounds):
            with httpx.Client(base_url=WEB_A) as c:
                car = c.get("/v1/cars", params={"zone": "airport"}, headers=user("u-driver-1")).json()["items"][0]["id"]
            res = asyncio.run(one_round(car, args.clients))
            created += res.pop("created")
            rounds.append(res)
            print(f"{car}: {res['codes']}  в базі незавершених оренд: {res['open_rentals_in_db']}  "
                  f"{'OK' if res['ok'] else 'ПОРУШЕННЯ'}")
    finally:
        with psycopg.connect(DB) as conn:
            conn.execute("UPDATE rental SET status = 'cancelled', version = version + 1 WHERE id = ANY(%s)", (created,))
    summary = {"experiment": "web-o2.1-race", "clients": args.clients, "rounds": rounds,
               "violations": sum(not r["ok"] for r in rounds)}
    Path(args.out).write_text(json.dumps(summary, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(f"порушень: {summary['violations']} з {args.rounds} → {args.out}")


if __name__ == "__main__":
    main()
