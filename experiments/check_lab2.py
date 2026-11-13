# /// script
# requires-python = ">=3.12"
# dependencies = ["httpx>=0.28", "psycopg[binary]>=3.2"]
# ///
"""Сценарії перевірки всіх виконаних пунктів (ЛР1 і ЛР2 обох частин): uv run experiments/check_lab2.py

Спершу – перевірки ЛР1 (check_lab1.py), далі – ЛР2. Друкує PASS/FAIL/SKIP; код виходу 1, якщо є FAIL.
Керована відмова О2.3 вмикається через `docker compose exec -T app-a touch /faults/...`
(або файл у FAULT_DIR_LOCAL, якщо сервер запущено поза Docker); --no-faults її пропускає.
Створені оренди наприкінці переводяться в cancelled.
"""

import argparse
import asyncio
import shutil
import subprocess
import sys
import time
import uuid
from pathlib import Path

import check_lab1 as lab1
import httpx
import psycopg
from common import DB, DPC, DPC_AUTH, WEB, WEB_A, WEB_B, idem, user

check = lab1.check
created = lab1.created
ROOT = Path(__file__).resolve().parent.parent


def skip(name: str, why: str) -> None:
    print(f"SKIP  {name}  ({why})")


def free_car(zone: str, base: str = WEB) -> str:
    items = httpx.get(f"{base}/v1/cars", params={"zone": zone}, headers=user("u-driver-1")).json()["items"]
    return items[0]["id"]


def book(c: httpx.Client, car: str, key: str | None = None, who: str = "u-driver-1", base: str = "") -> httpx.Response:
    return c.post(f"{base}/v1/rentals", json={"car_id": car}, headers={**user(who), **idem(key)})


def remember(r: httpx.Response) -> None:
    if r.status_code == 201:
        created.append(r.json()["id"])


# ================================================================ Web


def web_idempotency() -> None:
    print("— Web ЛР2: О2.2 ідемпотентність")
    with httpx.Client(base_url=WEB, timeout=10) as c:
        car = free_car("center")
        r = c.post("/v1/rentals", json={"car_id": car}, headers=user("u-driver-1"))
        problem = r.headers.get("content-type", "").startswith("application/problem+json")
        check("О2.2 без Idempotency-Key – 400 Problem Details", r.status_code == 400 and problem, r.status_code)
        r = book(c, car, key="short")
        check("     ключ коротший за 8 символів – 400", r.status_code == 400, r.status_code)

        key = f"k-chk-{uuid.uuid4().hex[:10]}"
        r1 = book(c, car, key, base=WEB_A)
        remember(r1)
        check("     перша спроба – 201, Idempotent-Replayed: false", r1.status_code == 201
              and r1.headers.get("idempotent-replayed") == "false", r1.status_code)
        r2 = book(c, car, key, base=WEB_B)
        check("О2.2 повтор з тим самим тілом через інший екземпляр – та сама відповідь (201, id, Location)",
              r2.status_code == 201 and r2.json().get("id") == r1.json().get("id")
              and r2.headers.get("location") == r1.headers.get("location")
              and r2.headers.get("idempotent-replayed") == "true", (r2.status_code, r2.headers.get("idempotent-replayed")))
        r3 = c.post(f"{WEB_A}/v1/rentals", content=f'{{ "car_id" : "{car}" }}'.encode(),
                    headers={**user("u-driver-1"), **idem(key), "Content-Type": "application/json"})
        check("     інші пробіли в JSON – той самий відбиток, та сама відповідь", r3.status_code == 201
              and r3.json().get("id") == r1.json().get("id"), r3.status_code)
        r4 = book(c, free_car("center"), key)
        check("О2.2 той самий ключ з іншим тілом – 422", r4.status_code == 422, r4.status_code)
        r5 = book(c, car, key, who="u-driver-2")
        remember(r5)
        check("     той самий ключ іншого користувача – окрема область ключа (409 car-busy, а не чужа відповідь)",
              r5.status_code == 409 and r5.json().get("id") is None, r5.status_code)

        # предметна відмова теж зберігається: повтор отримує той самий 409
        busy_key = f"k-chk-{uuid.uuid4().hex[:10]}"
        b1, b2 = book(c, car, busy_key, who="u-driver-2"), book(c, car, busy_key, who="u-driver-2")
        check("     збережена й предметна відмова: повтор 409 car-busy – той самий 409 (replayed)",
              b1.status_code == b2.status_code == 409 and b2.headers.get("idempotent-replayed") == "true",
              (b1.status_code, b2.status_code))

        # «ключ ще обробляється»: інша транзакція тримає блокування цього ключа
        car2 = free_car("center")
        key2 = f"k-chk-{uuid.uuid4().hex[:10]}"
        with psycopg.connect(DB, autocommit=True) as conn:
            conn.execute("SELECT pg_advisory_lock(hashtextextended(%s, 0))",
                         (f"u-driver-1\x1fPOST /v1/rentals\x1f{key2}",))
            r6 = book(c, car2, key2)
            conn.execute("SELECT pg_advisory_unlock_all()")
        check("О2.2 ключ ще обробляється – 409 з Retry-After", r6.status_code == 409 and r6.headers.get("retry-after")
              and r6.json().get("type", "").endswith("idempotency-key-in-progress"), r6.status_code)
        r7 = book(c, car2, key2)
        remember(r7)
        check("     після завершення тієї транзакції – 201", r7.status_code == 201, r7.status_code)

        with psycopg.connect(DB) as conn:
            row = conn.execute(
                "SELECT state, status_code, response->>'id' AS id, request_hash, expires_at > now() AS alive"
                " FROM idempotency_key WHERE user_id = 'u-driver-1' AND key = %s", (key,)).fetchone()
            n = conn.execute("SELECT count(*) FROM rental WHERE car_id = %s AND status = 'held'", (car,)).fetchone()[0]
        check("О2.2 у базі: стан completed, код, відповідь, відбиток, строк; оренда одна",
              row is not None and row[0] == "completed" and row[1] == 201 and row[2] == r1.json()["id"]
              and len(row[3]) == 64 and row[4] and n == 1, (row, n))

        # прострочений ключ більше не діє
        with psycopg.connect(DB) as conn:
            conn.execute("UPDATE idempotency_key SET expires_at = now() - interval '1 second'"
                         " WHERE user_id = 'u-driver-1' AND key = %s", (busy_key,))
            conn.execute("UPDATE idempotency_key SET expires_at = now() - interval '1 second'"
                         " WHERE user_id = 'u-driver-2' AND key = %s", (busy_key,))
        r8 = book(c, free_car("center"), busy_key, who="u-driver-2")
        remember(r8)
        check("     прострочений ключ можна використати знову (не 422)", r8.status_code == 201, r8.status_code)

    # той самий ключ одночасно: предметна зміна одна
    car3 = free_car("airport")
    key3 = f"k-chk-{uuid.uuid4().hex[:10]}"

    async def same_key() -> list[httpx.Response]:
        async with httpx.AsyncClient(timeout=10) as ac:
            return await asyncio.gather(*(
                ac.post(f"{WEB_A if i % 2 else WEB_B}/v1/rentals", json={"car_id": car3},
                        headers={**user("u-driver-1"), **idem(key3)}) for i in range(10)))

    replies = asyncio.run(same_key())
    ids = {r.json()["id"] for r in replies if r.status_code == 201}
    created.extend(ids)
    with psycopg.connect(DB) as conn:
        n = conn.execute("SELECT count(*) FROM rental WHERE car_id = %s AND status = 'held'", (car3,)).fetchone()[0]
    check("О2.2 10 одночасних запитів з одним ключем через обидва екземпляри – одна оренда, відповіді 201 (той самий id) або 409 in-progress",
          len(ids) == 1 and n == 1 and {r.status_code for r in replies} <= {201, 409},
          sorted(r.status_code for r in replies))


def web_race() -> None:
    print("— Web ЛР2: О2.1 інваріант за одночасних запитів")
    car = free_car("airport")

    async def race() -> list[httpx.Response]:
        async with httpx.AsyncClient(timeout=10) as ac:
            return await asyncio.gather(*(
                ac.post(f"{WEB_A if i % 2 else WEB_B}/v1/rentals", json={"car_id": car},
                        headers={**user(f"u-driver-{1 + i // 2 % 2}"), **idem()}) for i in range(20)))

    replies = asyncio.run(race())
    for r in replies:
        remember(r)
    codes = sorted(r.status_code for r in replies)
    insts = {r.headers.get("x-instance") for r in replies}
    with psycopg.connect(DB) as conn:
        n = conn.execute("SELECT count(*) FROM rental WHERE car_id = %s AND status IN ('held', 'active')",
                         (car,)).fetchone()[0]
    check("О2.1 20 одночасних бронювань одного авто через app-a і app-b – один 201, дев'ятнадцять 409",
          codes == [201] + [409] * 19 and insts == {"app-a", "app-b"}, (codes, insts))
    check("     підсумковий стан у базі: одна незавершена оренда авто", n == 1, n)


def web_lost_response(fault_dir: str | None) -> None:
    print("— Web ЛР2: О2.3 повтор після втраченої відповіді")
    place = "after_commit_before_response"
    try:
        if fault_dir:
            (Path(fault_dir) / place).touch()
        else:
            subprocess.run(["docker", "compose", "exec", "-T", "app-a", "touch", f"/faults/{place}"],
                           check=True, cwd=ROOT, capture_output=True, timeout=30)
    except (OSError, subprocess.SubprocessError) as exc:
        skip("О2.3", f"не вдалося ввімкнути відмову: {type(exc).__name__}")
        return
    car = free_car("center")
    key = f"k-chk-{uuid.uuid4().hex[:10]}"
    with httpx.Client(timeout=10) as c:
        try:
            first = c.post(f"{WEB_A}/v1/rentals", json={"car_id": car}, headers={**user("u-driver-1"), **idem(key)})
            lost = False
            remember(first)
        except httpx.TransportError:
            lost = True
        check("О2.3 після COMMIT app-a гине до відповіді – клієнт відповіді не отримав", lost)
        r = c.post(f"{WEB_B}/v1/rentals", json={"car_id": car}, headers={**user("u-driver-1"), **idem(key)})
        remember(r)
        with psycopg.connect(DB) as conn:
            n = conn.execute("SELECT count(*) FROM rental WHERE car_id = %s AND status = 'held'", (car,)).fetchone()[0]
        check("О2.3 повтор з тим самим ключем через app-b – збережений 201, оренда в базі одна",
              r.status_code == 201 and r.headers.get("idempotent-replayed") == "true" and n == 1,
              (r.status_code, r.headers.get("idempotent-replayed"), n))
    for _ in range(60):  # app-a перезапускається (restart: unless-stopped)
        try:
            if httpx.get(f"{WEB_A}/health", timeout=1).status_code == 200:
                break
        except httpx.TransportError:
            pass
        time.sleep(0.5)
    check("     app-a перезапустився й відповідає /health", httpx.get(f"{WEB_A}/health", timeout=2).status_code == 200)


def web_client_retries() -> None:
    print("— Web ЛР2: О2.4 повтори в клієнті")
    node = shutil.which("node")
    if not node:
        skip("О2.4 сценарій web_client_retry.mjs", "немає Node.js; можна перевірити вручну в клієнті")
        return
    p = subprocess.run([node, str(ROOT / "experiments/web_client_retry.mjs")], capture_output=True, text=True,
                       check=False)
    for line in p.stdout.splitlines():
        if line.startswith(("PASS", "FAIL")):
            check("О2.4 " + line[6:], line.startswith("PASS"))


def web_if_match_and_cancel() -> None:
    print("— Web ЛР2: Д2.4 продовження з If-Match, Д2.5 повторне скасування")
    with httpx.Client(base_url=WEB, timeout=10) as c:
        r = book(c, free_car("podil"))
        remember(r)
        rid = r.json()["id"]
        g = c.get(f"/v1/rentals/{rid}", headers=user("u-driver-1"))
        tag = g.headers.get("etag")
        check("Д2.4 GET оренди повертає ETag", tag == '"1"', tag)
        p = c.patch(f"/v1/rentals/{rid}", json={"extend_minutes": 10}, headers=user("u-driver-1"))
        check("Д2.4 PATCH без If-Match – 428", p.status_code == 428, p.status_code)
        p = c.patch(f"/v1/rentals/{rid}", json={"extend_minutes": 10}, headers={**user("u-driver-1"), "If-Match": tag})
        ok = p.status_code == 200 and p.headers.get("etag") == '"2"' and p.json()["expires_at"] > g.json()["expires_at"]
        check("Д2.4 PATCH з актуальним ETag – 200, строк +10 хв, новий ETag", ok, p.status_code)
        p = c.patch(f"/v1/rentals/{rid}", json={"extend_minutes": 10}, headers={**user("u-driver-1"), "If-Match": tag})
        check("Д2.4 застарілий ETag – 412", p.status_code == 412, p.status_code)
        p = c.patch(f"/v1/rentals/{rid}", json={"status": "active"}, headers={**user("u-driver-1"), "If-Match": '"2"'})
        check("     зміна стану через PATCH – 422", p.status_code == 422, p.status_code)

        car = c.get(f"/v1/rentals/{rid}", headers=user("u-driver-1")).json()["car_id"]
        x1 = c.post(f"/v1/rentals/{rid}/cancel", headers=user("u-driver-1"))
        check("Д2.5 скасування – 200 cancelled", x1.status_code == 200 and x1.json()["status"] == "cancelled", x1.status_code)
        other = book(c, car, who="u-driver-2")
        remember(other)
        x2 = c.post(f"/v1/rentals/{rid}/cancel", headers=user("u-driver-1"))
        still = c.get(f"/v1/rentals/{other.json().get('id')}", headers=user("u-driver-2")).json().get("status")
        check("Д2.5 повторне скасування – той самий cancelled, версія не змінилась",
              x2.status_code == 200 and x2.json() == x1.json() and x2.headers.get("x-already-cancelled") == "true",
              x2.status_code)
        check("Д2.5 нова оренда того самого авто іншим водієм не зачеплена (held)",
              other.status_code == 201 and still == "held", (other.status_code, still))


# ================================================================ DPC


def dpc_compute() -> None:
    print("— DPC ЛР2: розрахунок POST /v1/compute")
    with httpx.Client(base_url=DPC, headers=DPC_AUTH, timeout=600) as c:
        check("О2.1 без токена – 401", httpx.post(DPC + "/v1/compute", json={"n": 10}).status_code == 401)
        values = {}
        for p in (1, 2, 4):
            r = c.post("/v1/compute", json={"a": 0, "b": 12, "n": 2_000_000, "workers": p})
            res = r.json().get("result", {})
            values[p] = res.get("value")
            keys = {"value", "reference", "error", "wall_s", "workers"} <= res.keys()
            check(f"О2.1 workers={p}: 200, value/reference/error/wall_s/workers, у межах допуску {res.get('tolerance')}",
                  r.status_code == 200 and keys and res["within_tolerance"] and res["workers"] == p, res)
        spread = max(values.values()) - min(values.values())
        check("     результати для 1, 2 і 4 процесів збігаються в межах допуску", spread <= 1e-6 * abs(values[1]), spread)
        loc = r.headers.get("location", "")
        check("     Location → GET /v1/compute/{id} – 200", c.get(loc).status_code == 200, loc)
        r = c.post("/v1/compute", json={"n": 11, "method": "simpson"})
        check("     Сімпсон з непарним n – 400", r.status_code == 400, r.status_code)
        r = c.post("/v1/compute", json={"a": 5, "b": 1, "n": 10})
        check("     b ≤ a – 400", r.status_code == 400, r.status_code)

        print("— DPC ЛР2: О2.4 спільна сума")
        res = {m: c.post("/v1/compute", json={"n": 400_000, "workers": 4, "mode": m}).json()["result"]
               for m in ("none", "lock", "local")}
        check("О2.4 none – втрачені оновлення: значення менше за суму частин, поза допуском",
              res["none"]["lost_value"] > 0 and not res["none"]["within_tolerance"], res["none"]["lost_value"])
        check("О2.4 lock і local – у межах допуску", res["lock"]["within_tolerance"] and res["local"]["within_tolerance"])

        print("— DPC ЛР2: О2.2 точність, Д2.1 Сімпсон, Д2.4 сумування")
        err = {(m, n): c.post("/v1/compute", json={"n": n, "method": m}).json()["result"]["error"]
               for m in ("midpoint", "simpson") for n in (64, 128)}
        q_mid = err["midpoint", 64] / err["midpoint", 128]
        q_simp = err["simpson", 64] / err["simpson", 128]
        check("О2.2 середні прямокутники: err(n)/err(2n) ≈ 4 (порядок 2)", 3.9 < q_mid < 4.1, q_mid)
        check("Д2.1 Сімпсон: err(n)/err(2n) ≈ 16 (порядок 4)", 15.5 < q_simp < 16.5, q_simp)
        s = {m: c.post("/v1/compute", json={"n": 20_000_000, "summation": m}).json()["result"]["rel_error"]
             for m in ("naive", "kahan", "pairwise")}
        check("Д2.4 n=2·10^7: компенсоване й попарне сумування точніші за послідовне",
              s["kahan"] < s["naive"] and s["pairwise"] < s["naive"], s)


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--no-faults", action="store_true", help="не вмикати керовану відмову О2.3")
    args = ap.parse_args()
    import os

    try:
        lab1.web_checks()
        lab1.dpc_checks()
        web_race()
        web_idempotency()
        web_if_match_and_cancel()
        web_client_retries()
        if not args.no_faults:
            web_lost_response(os.environ.get("FAULT_DIR_LOCAL"))
        dpc_compute()
    finally:
        lab1.cleanup()
    print(f"\n{'OK' if lab1.failures == 0 else f'{lab1.failures} FAIL'}")
    sys.exit(1 if lab1.failures else 0)
