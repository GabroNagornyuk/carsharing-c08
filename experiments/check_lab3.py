# /// script
# requires-python = ">=3.12"
# dependencies = ["httpx>=0.28", "psycopg[binary]>=3.2"]
# ///
"""Перевірки всіх виконаних пунктів ЛР1–ЛР3 обох частин: uv run experiments/check_lab3.py

Спершу – перевірки ЛР1 і ЛР2 (check_lab1.py, check_lab2.py), далі – ЛР3 Web і DPC.
DPC ЛР3 очікує api в режимі EXECUTION=ray (типовий у compose.yaml).
Керовані відмови вмикаються через `docker compose exec -T <сервіс> touch /faults/...` (або файл у
FAULT_DIR_LOCAL, якщо стенд запущено поза Docker); --no-faults їх пропускає.
"""

import argparse
import asyncio
import os
import sys
import time
import uuid
from pathlib import Path

import check_lab1 as lab1
import check_lab2 as lab2
import httpx
import psycopg
from common import DB, DPC, DPC_AUTH, WEB, WEB_A, WEB_B, idem, login, user

check = lab1.check
created = lab1.created
CLIENT = "http://localhost:8081"
EVIL = "http://localhost:8082"


def cookie(token: str) -> dict[str, str]:
    return {"Cookie": f"session={token}"}


# ---------------------------------------------------------------- Web ЛР3


def web_login() -> None:
    print("— Web ЛР3: О3.1 вхід, вихід, строк дії")
    r = httpx.post(f"{WEB_A}/v1/auth/login", json={"login": "driver1", "password": "driver1-pass"})
    set_cookie = r.headers.get("set-cookie", "")
    check("О3.1 вхід через app-a – 200, роль user", r.status_code == 200 and r.json().get("role") == "user", r.text)
    check("     cookie session: HttpOnly, SameSite=Lax, строк дії",
          "session=" in set_cookie and "httponly" in set_cookie.lower() and "samesite=lax" in set_cookie.lower()
          and "max-age=" in set_cookie.lower(), set_cookie)
    token = r.cookies["session"]
    a = httpx.get(f"{WEB_A}/v1/me", headers=cookie(token))
    b = httpx.get(f"{WEB_B}/v1/me", headers=cookie(token))
    check("     та сама сесія діє на app-a і app-b", a.status_code == b.status_code == 200
          and a.json()["id"] == b.json()["id"] == "u-driver-1", (a.status_code, b.status_code))
    with psycopg.connect(DB) as conn:
        row = conn.execute("SELECT count(*), max(expires_at - created_at) FROM session WHERE user_id = 'u-driver-1'"
                           " AND token_hash = encode(sha256(%s::bytea), 'hex')", (token.encode(),)).fetchone()
    check("     у базі лише sha256 токена, строк сесії 60 хв", row[0] == 1 and row[1].total_seconds() == 3600, row)

    r = httpx.post(f"{WEB}/v1/auth/login", json={"login": "driver1", "password": "wrong"})
    r2 = httpx.post(f"{WEB}/v1/auth/login", json={"login": "nobody", "password": "wrong"})
    check("     неправильний пароль і невідомий логін – однаковий 401",
          r.status_code == r2.status_code == 401 and r.json()["type"] == r2.json()["type"], (r.status_code, r2.status_code))
    r = httpx.get(f"{WEB}/v1/me", headers={"X-User-Id": "u-driver-1"})
    check("     X-User-Id більше не приймається – 401", r.status_code == 401, r.status_code)

    r = httpx.post(f"{WEB_B}/v1/auth/logout", headers=cookie(token))
    gone = r.headers.get("set-cookie", "").lower()
    check("О3.1 вихід через app-b – 204, cookie знято",
          r.status_code == 204 and ("max-age=0" in gone or 'session=""' in gone), (r.status_code, gone))
    r = httpx.get(f"{WEB_A}/v1/me", headers=cookie(token))
    check("     старий cookie після виходу – 401 (сесію знищено на сервері)", r.status_code == 401, r.status_code)

    token2 = login("driver2")
    with psycopg.connect(DB) as conn:
        conn.execute("UPDATE session SET expires_at = created_at + interval '1 second', created_at = created_at"
                     " WHERE token_hash = encode(sha256(%s::bytea), 'hex')", (token2.encode(),))
    time.sleep(1.1)
    r = httpx.get(f"{WEB}/v1/me", headers=cookie(token2))
    check("     сесія з минулим строком – 401", r.status_code == 401, r.status_code)


def web_access() -> None:
    print("— Web ЛР3: О3.2 доступ до об'єктів і функція персоналу")
    with httpx.Client(base_url=WEB, timeout=10) as c:
        car = lab2.free_car("center")
        r = lab2.book(c, car, who="u-driver-1")
        lab2.remember(r)
        rid = r.json()["id"]
        own = c.get(f"/v1/rentals/{rid}", headers=user("u-driver-1"))
        alien = c.get(f"/v1/rentals/{rid}", headers=user("u-driver-2"))
        missing = c.get(f"/v1/rentals/{uuid.uuid4()}", headers=user("u-driver-2"))
        check("О3.2 чужа оренда – 404, як і відсутня (той самий type)",
              own.status_code == 200 and alien.status_code == missing.status_code == 404
              and alien.json()["type"] == missing.json()["type"], (own.status_code, alien.status_code))
        mine1 = [x["id"] for x in c.get("/v1/rentals", headers=user("u-driver-1")).json()["items"]]
        mine2 = [x["id"] for x in c.get("/v1/rentals", headers=user("u-driver-2")).json()["items"]]
        check("     список «мої оренди»: своя є, у чужому списку її немає", rid in mine1 and rid not in mine2)
        x = c.post(f"/v1/rentals/{rid}/cancel", headers=user("u-driver-2"))
        check("     скасування чужої оренди – 404, оренда не змінилась",
              x.status_code == 404 and c.get(f"/v1/rentals/{rid}", headers=user("u-driver-1")).json()["status"] == "held",
              x.status_code)
        tag = own.headers["etag"]
        for field, value in (("user_id", "u-driver-2"), ("status", "active"), ("version", 7)):
            p = c.patch(f"/v1/rentals/{rid}", json={"extend_minutes": 5, field: value},
                        headers={**user("u-driver-1"), "If-Match": tag})
            check(f"     зміна поля {field} – 422", p.status_code == 422, p.status_code)

        r = c.post("/v1/cars/car-pod-003/service", headers=user("u-driver-1"))
        check("О3.2 функція персоналу водієм – 403", r.status_code == 403, r.status_code)
        r = c.post("/v1/cars/car-pod-003/service", json={"in_service": True}, headers=user("u-operator"))
        check("     оператор (staff) переводить авто на обслуговування – 200",
              r.status_code == 200 and r.json()["status"] == "service", r.text)
        b = lab2.book(c, "car-pod-003", who="u-driver-2")
        check("     бронювання авто на обслуговуванні – 409 car-in-service",
              b.status_code == 409 and b.json()["type"].endswith("car-in-service"), b.status_code)
        r = c.post(f"/v1/cars/{car}/service", headers=user("u-operator"))
        check("     авто в незавершеній оренді на обслуговування не переводиться – 409", r.status_code == 409,
              r.status_code)
        r = c.post("/v1/cars/car-pod-003/service", json={"in_service": False}, headers=user("u-operator"))
        check("     повернення в роботу – available", r.status_code == 200 and r.json()["status"] == "available")


def web_browser() -> None:
    print("— Web ЛР3: О3.3 CORS і захист від стороннього сайту")
    with httpx.Client(base_url=WEB, timeout=10) as c:
        pre = c.options("/v1/rentals", headers={"Origin": CLIENT, "Access-Control-Request-Method": "POST",
                                                 "Access-Control-Request-Headers": "content-type,idempotency-key"})
        check("О3.3 preflight клієнта: Allow-Origin = походження клієнта, з обліковими даними",
              pre.status_code == 200 and pre.headers.get("access-control-allow-origin") == CLIENT
              and pre.headers.get("access-control-allow-credentials") == "true", dict(pre.headers))
        bad = c.options("/v1/rentals", headers={"Origin": EVIL, "Access-Control-Request-Method": "POST"})
        check("     preflight стороннього сайту – без Access-Control-Allow-Origin (не * і не віддзеркалення)",
              "access-control-allow-origin" not in bad.headers, dict(bad.headers))
        g = c.get("/v1/me", headers={**user("u-driver-1"), "Origin": EVIL})
        check("     GET зі стороннім Origin: відповідь без Allow-Origin – браузер не дасть її прочитати",
              "access-control-allow-origin" not in g.headers)

        car = lab2.free_car("airport")

        def open_rentals() -> int:
            with psycopg.connect(DB) as conn:
                return conn.execute("SELECT count(*) FROM rental WHERE car_id = %s AND status IN ('held','active')",
                                    (car,)).fetchone()[0]

        evil = c.post("/v1/rentals", json={"car_id": car},
                      headers={**user("u-driver-1"), **idem(), "Origin": EVIL, "Sec-Fetch-Site": "same-site"})
        check("     POST зі стороннім Origin (cookie додано) – 403 до виконання дії, оренди немає",
              evil.status_code == 403 and open_rentals() == 0, evil.status_code)
        form = c.post("/v1/rentals", content=b'{"car_id": "' + car.encode() + b'", "pad": "="}',
                      headers={**user("u-driver-1"), "Content-Type": "text/plain", "Sec-Fetch-Site": "cross-site"})
        check("     форма без Origin, Sec-Fetch-Site: cross-site – 403", form.status_code == 403 and open_rentals() == 0,
              form.status_code)
        ok = c.post("/v1/rentals", json={"car_id": car},
                    headers={**user("u-driver-1"), **idem(), "Origin": CLIENT, "Sec-Fetch-Site": "same-site"})
        lab2.remember(ok)
        check("     той самий запит із походження клієнта – 201", ok.status_code == 201 and open_rentals() == 1,
              ok.status_code)


def web_login_limit() -> None:
    print("— Web ЛР3: Д3.3 обмеження спроб входу")
    who = "driver2"
    with psycopg.connect(DB) as conn:
        conn.execute("DELETE FROM login_failure WHERE login = %s", (who,))
    codes = []
    for i in range(5):  # невдалі спроби по черзі через обидва екземпляри
        base = WEB_A if i % 2 == 0 else WEB_B
        codes.append(httpx.post(f"{base}/v1/auth/login", json={"login": who, "password": "bad"}).status_code)
    sixth = httpx.post(f"{WEB_B}/v1/auth/login", json={"login": who, "password": "bad"})
    right = httpx.post(f"{WEB_A}/v1/auth/login", json={"login": who, "password": "driver2-pass"})
    other = httpx.post(f"{WEB_A}/v1/auth/login", json={"login": "driver1", "password": "driver1-pass"})
    check("Д3.3 5 невдалих спроб (app-a і app-b) – 401", codes == [401] * 5, codes)
    check("     6-та – 429 з Retry-After", sixth.status_code == 429 and int(sixth.headers.get("retry-after", 0)) > 0,
          sixth.status_code)
    check("     навіть правильний пароль у межах вікна – 429", right.status_code == 429, right.status_code)
    check("     інший логін не заблоковано", other.status_code == 200, other.status_code)
    with psycopg.connect(DB) as conn:
        conn.execute("DELETE FROM login_failure WHERE login = %s", (who,))
    ok = httpx.post(f"{WEB_A}/v1/auth/login", json={"login": who, "password": "driver2-pass"})
    check("     після закінчення вікна (рядки спроб прибрано) – вхід 200", ok.status_code == 200, ok.status_code)


# ---------------------------------------------------------------- DPC ЛР3


def _wait_acked(c: httpx.Client, cid: str, seconds: float = 40) -> dict:
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        r = c.get(f"/v1/rental-start-effects/{cid}")
        if r.status_code == 200 and r.json().get("acked_at"):
            return r.json()
        if c.get(f"/v1/rental-starts/{cid}").json()["state"] == "failed":
            return {}
        time.sleep(0.25)
    return {}


def dpc_ray(fault_dir: str | None, faults: bool) -> None:
    print("— DPC ЛР3: задачі Ray, приймач, відмова виконавця")
    with httpx.Client(base_url=DPC, headers=DPC_AUTH, timeout=30) as c:
        m = c.get("/metrics").json()
        if not m.get("tasks_in_flight_limit"):
            lab2.skip("DPC ЛР3", "api запущено не в режимі EXECUTION=ray")
            return
        cid = f"l3-{uuid.uuid4().hex[:8]}"
        c.post("/v1/rental-starts", json={"command_id": cid, "car_id": "lt-l3", "driver_id": "u-driver-1",
                                          "zone": "loadtest"})
        d = _wait_acked(c, cid)
        check("О3.1 команду виконала задача Ray – done, підтверджено, виконавець – вузол Ray",
              d.get("decision") == "done" and str(d.get("acked_by", "")).startswith("ray-"), d)
        nid = f"l3n-{uuid.uuid4().hex[:8]}"
        c.post("/v1/rental-starts", json={"command_id": nid, "car_id": "lt-l3", "driver_id": "nolicense-7",
                                          "zone": "loadtest"})
        d = _wait_acked(c, nid)
        check("     посвідчення не підтверджено – рішення rejected, ефекту немає",
              d.get("decision") == "rejected" and d.get("effects") == 0, d)

        # О3.3: повторна доставка того самого ефекту
        z0 = c.get("/v1/zone-cars/loadtest").json()
        again = c.post("/v1/rental-start-effects", json={"command_id": cid, "license_ok": True, "executor": "check"})
        z1 = c.get("/v1/zone-cars/loadtest").json()
        check("О3.3 повторна доставка – збережене рішення (replayed), ресурс не змінився",
              again.json().get("replayed") is True and again.json()["decision"] == "done" and z1["rented"] == z0["rented"],
              again.text)

        # одночасні доставки ефекту для команди, якої Ray ще не бачив: одне приймання
        pid = f"l3p-{uuid.uuid4().hex[:8]}"
        with psycopg.connect(DB) as conn:
            conn.execute("INSERT INTO rental_start (id, car_id, driver_id, zone_id, state, started_at)"
                         " VALUES (%s, 'lt-par', 'u-driver-1', 'loadtest', 'processing', now())", (pid,))

        async def deliver() -> list[httpx.Response]:
            async with httpx.AsyncClient(base_url=DPC, headers=DPC_AUTH, timeout=30) as ac:
                return await asyncio.gather(*(ac.post("/v1/rental-start-effects", json={
                    "command_id": pid, "license_ok": True, "executor": f"check-{i}"}) for i in range(10)))

        replies = asyncio.run(deliver())
        z2 = c.get("/v1/zone-cars/loadtest").json()
        fresh = [r for r in replies if r.status_code == 200 and r.json()["replayed"] is False]
        dec = c.get(f"/v1/rental-start-effects/{pid}").json()
        check("     10 одночасних доставок – одне приймання, 9 збережених, один ефект",
              len(fresh) == 1 and dec["deliveries"] == 10 and dec["effects"] == 1 and z2["rented"] == z1["rented"] + 1
              and z2["invariant_ok"], (len(fresh), dec))

        # О3.2: межа незавершених задач
        burst = f"l3b-{uuid.uuid4().hex[:6]}"

        async def flood() -> None:
            async with httpx.AsyncClient(base_url=DPC, headers=DPC_AUTH, timeout=30) as ac:
                await asyncio.gather(*(ac.post("/v1/rental-starts", json={
                    "command_id": f"{burst}-{i}", "car_id": f"lt-{i}", "driver_id": "u-driver-1", "zone": "loadtest"})
                    for i in range(40)))

        asyncio.run(flood())
        seen = []
        for _ in range(20):
            seen.append(c.get("/metrics").json()["tasks_in_flight"])
            time.sleep(0.1)
        m = c.get("/metrics").json()
        check(f"О3.2 сплеск 40 команд: незавершених задач не більше IN_FLIGHT_LIMIT ({m['tasks_in_flight_limit']})",
              max(seen) <= m["tasks_in_flight_limit"] and m["tasks_max_in_flight"] <= m["tasks_in_flight_limit"],
              (max(seen), m["tasks_max_in_flight"]))
        time.sleep(6)

        if not faults:
            lab2.skip("О3.4", "--no-faults")
        else:
            fid = f"l3f-{uuid.uuid4().hex[:8]}"
            place = "after_effect_before_ack"
            if fault_dir:
                Path(fault_dir, place).touch()
            else:
                import subprocess

                subprocess.run(["docker", "compose", "exec", "-T", "api", "touch", f"/faults/{place}"], check=True)
            z3 = c.get("/v1/zone-cars/loadtest").json()
            c.post("/v1/rental-starts", json={"command_id": fid, "car_id": "lt-f", "driver_id": "u-driver-1",
                                              "zone": "loadtest"})
            d = _wait_acked(c, fid, 60)
            z4 = c.get("/v1/zone-cars/loadtest").json()
            check("О3.4 відмова після запису ефекту: задачу повторено (2 доставки), ефект один",
                  d.get("deliveries") == 2 and d.get("effects") == 1 and d.get("executor") != d.get("acked_by"), d)
            check("     ресурс змінився рівно на 1, інваріант ЛР1 збережено",
                  z4["rented"] - z3["rented"] == 1 and z4["invariant_ok"], (z3["rented"], z4["rented"]))


def dpc_idempotency() -> None:
    print("— DPC ЛР3: Д3.3 Idempotency-Key")
    with httpx.Client(base_url=DPC, headers=DPC_AUTH, timeout=30) as c:
        key = f"k-{uuid.uuid4()}"
        body = {"command_id": f"l3i-{uuid.uuid4().hex[:8]}", "car_id": "lt-i", "driver_id": "u-driver-1",
                "zone": "loadtest"}
        first = c.post("/v1/rental-starts", json=body, headers={"Idempotency-Key": key})
        again = c.post("/v1/rental-starts", json=body, headers={"Idempotency-Key": key})
        check("Д3.3 повтор з тим самим ключем і тілом – той самий id",
              first.status_code == 202 and again.status_code == 200 and again.json()["id"] == first.json()["id"]
              and again.headers.get("idempotent-replayed") == "true", (first.status_code, again.status_code))
        other = c.post("/v1/rental-starts", json={**body, "command_id": body["command_id"] + "x"},
                       headers={"Idempotency-Key": key})
        check("     той самий ключ з іншим тілом – 409", other.status_code == 409, other.status_code)

        key2 = f"k-{uuid.uuid4()}"
        body2 = {**body, "command_id": f"l3j-{uuid.uuid4().hex[:8]}"}

        async def dup() -> list[httpx.Response]:
            async with httpx.AsyncClient(base_url=DPC, headers=DPC_AUTH, timeout=30) as ac:
                return await asyncio.gather(*(ac.post("/v1/rental-starts", json=body2, headers={"Idempotency-Key": key2})
                                              for _ in range(10)))

        codes = sorted(r.status_code for r in asyncio.run(dup()))
        with psycopg.connect(DB) as conn:
            n = conn.execute("SELECT count(*) FROM rental_start WHERE id = %s", (body2["command_id"],)).fetchone()[0]
        check("     10 одночасних дублікатів – одне приймання (202), решта 200", codes.count(202) == 1 and n == 1, codes)


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--no-faults", action="store_true", help="не вмикати керовані відмови (О2.3 Web, О3.4 DPC)")
    args = ap.parse_args()
    fault_dir = os.environ.get("FAULT_DIR_LOCAL")
    try:
        lab1.web_checks()
        lab1.dpc_checks()
        lab2.web_race()
        lab2.web_idempotency()
        lab2.web_if_match_and_cancel()
        lab2.web_client_retries()
        if not args.no_faults:
            lab2.web_lost_response(fault_dir)
        lab2.dpc_compute()
        web_login()
        web_access()
        web_browser()
        web_login_limit()
        dpc_ray(fault_dir, not args.no_faults)
        dpc_idempotency()
    finally:
        lab1.cleanup()
    print(f"\n{'OK' if lab1.failures == 0 else f'{lab1.failures} FAIL'}")
    sys.exit(1 if lab1.failures else 0)
