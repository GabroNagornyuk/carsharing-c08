# /// script
# requires-python = ">=3.12"
# dependencies = ["httpx>=0.28", "psycopg[binary]>=3.2"]
# ///
"""Сценарії перевірки ЛР1 обох частин на запущеному стенді: uv run experiments/check_lab1.py
(з ЛР2 Web кожне бронювання надсилає Idempotency-Key; усі перевірки разом – check_lab2.py)

Друкує PASS/FAIL на кожну перевірку; код виходу 1, якщо є FAIL. Створені перевіркою оренди
наприкінці переводяться в cancelled, щоб повторний запуск мав ті самі вільні авто.
"""

import asyncio
import sys
import time
import uuid

import httpx
import psycopg
from common import DB, DPC, DPC_AUTH, WEB, idem, user

failures = 0
created: list[str] = []


def check(name: str, ok: bool, info: object = "") -> None:
    global failures
    failures += 0 if ok else 1
    print(f"{'PASS' if ok else 'FAIL'}  {name}" + (f"  ({info})" if info and not ok else ""))


def free_car(c: httpx.Client, zone: str) -> str:
    return c.get("/v1/cars", params={"zone": zone}, headers=user("u-driver-1")).json()["items"][0]["id"]


def web_checks() -> None:
    print("— Web (через nginx", WEB + ")")
    with httpx.Client(base_url=WEB, timeout=10) as c:
        seen = {c.get("/health").headers.get("x-instance") for _ in range(6)}
        check("О1.6 /health через nginx відповідають обидва екземпляри", seen == {"app-a", "app-b"}, seen)
        r = c.get("/health")
        check("О1.6 відповідь має X-Request-ID", bool(r.headers.get("x-request-id")))

        car = free_car(c, "center")
        r = c.post("/v1/rentals", json={"car_id": car}, headers={**user("u-driver-1"), **idem()})
        ok = r.status_code == 201 and r.headers.get("location") == f"/v1/rentals/{r.json().get('id')}"
        check("О1.4 бронювання вільного авто – 201 і Location", ok, r.status_code)
        if r.status_code == 201:
            body = r.json()
            created.append(body["id"])
            check("О1.4 ідентифікатор – рядок, стан held", isinstance(body["id"], str) and body["status"] == "held")
            g = c.get(r.headers["location"], headers=user("u-driver-1"))
            check("     власник читає оренду – 200", g.status_code == 200 and g.json()["id"] == body["id"], g.status_code)
            g = c.get(r.headers["location"], headers=user("u-driver-2"))
            check("     чужа оренда – 404", g.status_code == 404, g.status_code)
            r2 = c.post("/v1/rentals", json={"car_id": car}, headers={**user("u-driver-2"), **idem()})
            check("О1.4 те саме авто вдруге – 409 car-busy", r2.status_code == 409, r2.status_code)
            free = [x["id"] for x in c.get("/v1/cars", params={"zone": "center"}, headers=user("u-driver-1")).json()["items"]]
            check("     заброньоване авто зникло зі списку вільних", car not in free)

        cases = [
            ("відсутнє авто – 404", {"json": {"car_id": "no-such-car"}}, 404),
            ("авто на обслуговуванні – 409 (інваріант 2)", {"json": {"car_id": "car-pod-004"}}, 409),
            ("пошкоджений JSON – 400", {"content": b'{"car_id":', "headers": {"Content-Type": "application/json"}}, 400),
            ("неправильні дані – 422", {"json": {"car": 1}}, 422),
        ]
        for name, kw, want in cases:
            headers = {**user("u-driver-1"), **idem(), **kw.pop("headers", {})}
            r = c.post("/v1/rentals", headers=headers, **kw)
            is_problem = r.headers.get("content-type", "").startswith("application/problem+json")
            check(f"О1.4 {name}, Problem Details", r.status_code == want and is_problem, r.status_code)
        r = c.post("/v1/rentals", json={"car_id": "car-cen-001"}, headers=idem())
        check("     без входу (cookie сесії) – 401", r.status_code == 401, r.status_code)

        # двоє (і більше) на одне авто: успіх лише один (інваріант 1)
        car = free_car(c, "airport")

    async def race() -> list[httpx.Response]:
        async with httpx.AsyncClient(base_url=WEB, timeout=10) as ac:
            return await asyncio.gather(*(
                ac.post("/v1/rentals", json={"car_id": car}, headers={**user(f"u-driver-{1 + i % 2}"), **idem()})
                for i in range(10)
            ))

    replies = asyncio.run(race())
    codes = sorted(r.status_code for r in replies)
    created.extend(r.json()["id"] for r in replies if r.status_code == 201)
    check("О1.4 10 одночасних бронювань одного авто – один 201, решта 409", codes == [201] + [409] * 9, codes)


def dpc_checks() -> None:
    print("— DPC (api", DPC + ")")
    with httpx.Client(base_url=DPC, headers=DPC_AUTH, timeout=10) as c:
        check("О1.1 без токена – 401", httpx.get(DPC + "/v1/zone-cars/airport").status_code == 401)
        cid = f"chk-{uuid.uuid4().hex[:8]}"
        body = {"command_id": cid, "car_id": "car-lt-1", "driver_id": "u-driver-1", "zone": "loadtest"}
        r = c.post("/v1/rental-starts", json=body)
        check("О1.1 приймання – 202 і Location, стан queued", r.status_code == 202 and r.json()["state"] == "queued"
              and r.headers.get("location") == f"/v1/rental-starts/{cid}", r.status_code)
        r = c.post("/v1/rental-starts", json=body)
        check("     повтор тієї самої команди – 200 без другого запису", r.status_code == 200, r.status_code)
        r = c.post("/v1/rental-starts", json={**body, "car_id": "other"})
        check("     той самий command_id з іншими даними – 409", r.status_code == 409, r.status_code)

        def wait(command_id: str) -> dict:
            for _ in range(40):
                s = c.get(f"/v1/rental-starts/{command_id}").json()
                if s["state"] in ("done", "rejected", "failed"):
                    return s
                time.sleep(0.25)
            return s

        s = wait(cid)
        check("О1.1/О1.3 команда доходить до done", s["state"] == "done", s["state"])
        nid = f"chk-{uuid.uuid4().hex[:8]}"
        c.post("/v1/rental-starts", json={**body, "command_id": nid, "driver_id": "nolicense-1"})
        s = wait(nid)
        check("О1.1 посвідчення не підтверджено – rejected", s["state"] == "rejected", s)
        r = c.post("/v1/rental-starts", json={**body, "command_id": "x" + nid, "zone": "nowhere"})
        check("     невідома зона – 422", r.status_code == 422, r.status_code)
        r = c.post("/v1/rental-starts", content=b"{", headers={"Content-Type": "application/json"})
        check("     пошкоджений JSON – 400", r.status_code == 400, r.status_code)
        z = c.get("/v1/zone-cars/loadtest").json()
        check("О1.4 ресурс loadtest: invariant_ok", z["invariant_ok"] is True, z)
        m = c.get("/metrics").json()
        check("     /metrics: queue_length, processing, rejected", {"queue_length", "processing", "rejected"} <= m.keys())


def cleanup() -> None:
    if created:
        with psycopg.connect(DB) as conn:
            conn.execute("UPDATE rental SET status = 'cancelled', version = version + 1 WHERE id = ANY(%s)", (created,))


if __name__ == "__main__":
    try:
        web_checks()
        dpc_checks()
    finally:
        cleanup()
    print(f"\n{'OK' if failures == 0 else f'{failures} FAIL'}")
    sys.exit(1 if failures else 0)
