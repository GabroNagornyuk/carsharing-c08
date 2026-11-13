# /// script
# requires-python = ">=3.12"
# dependencies = ["httpx>=0.28", "psycopg[binary]>=3.2"]
# ///
"""Web О2.3: повтор після втраченої відповіді (керована відмова after_commit_before_response).

    uv run experiments/web_lost_response.py

1. Вмикає відмову: `docker compose exec -T app-a touch /faults/after_commit_before_response`
   (або файл у --fault-dir, якщо сервер запущено поза Docker).
2. POST /v1/rentals з ключем K напряму на app-a. Транзакція фіксується, процес гине до
   відповіді – клієнт отримує обрив з'єднання.
3. Той самий запит з ключем K – на app-b. Очікування: 201 з тим самим id і Location,
   Idempotent-Replayed: true.
4. База: для цього ключа одна оренда; повтор не створив другої.
Відновлення: Docker перезапускає app-a (restart: unless-stopped); прапорець уже забрано.
"""

import argparse
import json
import subprocess
import time
import uuid
from pathlib import Path

import httpx
import psycopg
from common import DB, WEB_A, WEB_B, idem, user

PLACE = "after_commit_before_response"


def arm(fault_dir: str | None) -> None:
    if fault_dir:
        (Path(fault_dir) / PLACE).touch()
    else:
        subprocess.run(["docker", "compose", "exec", "-T", "app-a", "touch", f"/faults/{PLACE}"], check=True)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--fault-dir", help="каталог прапорців, якщо сервер працює поза Docker")
    ap.add_argument("--out", default="results/web-o2.3-lost-response.json")
    args = ap.parse_args()

    started = time.time()
    with httpx.Client(timeout=10) as c:
        car = c.get(f"{WEB_B}/v1/cars", params={"zone": "center"}, headers=user("u-driver-1")).json()["items"][0]["id"]
        key = f"k-lost-{uuid.uuid4().hex[:12]}"
        headers = {**user("u-driver-1"), **idem(key)}
        arm(args.fault_dir)

        try:
            r1 = c.post(f"{WEB_A}/v1/rentals", json={"car_id": car}, headers=headers)
            first = f"{r1.status_code} (відмова не спрацювала?)"
        except httpx.TransportError as exc:
            first = f"no response: {type(exc).__name__}"
        print("1-а спроба через app-a:", first)

        r2 = c.post(f"{WEB_B}/v1/rentals", json={"car_id": car}, headers=headers)
        body = r2.json()
        print("повтор через", r2.headers.get("x-instance"), "→", r2.status_code,
              "Idempotent-Replayed:", r2.headers.get("idempotent-replayed"), "Location:", r2.headers.get("location"))
        r3 = c.post(f"{WEB_B}/v1/rentals", json={"car_id": car}, headers=headers)

    with psycopg.connect(DB) as conn:
        rentals = conn.execute(
            "SELECT id FROM rental WHERE car_id = %s AND user_id = 'u-driver-1' AND created_at >= to_timestamp(%s)",
            (car, started - 1),
        ).fetchall()
        stored = conn.execute(
            "SELECT state, status_code, location FROM idempotency_key WHERE key = %s", (key,)
        ).fetchone()
        conn.execute("UPDATE rental SET status = 'cancelled', version = version + 1 WHERE id = ANY(%s)",
                     ([r[0] for r in rentals],))

    result = {
        "experiment": "web-o2.3-lost-response", "car": car, "key": key, "first_attempt": first,
        "retry": {"instance": r2.headers.get("x-instance"), "status": r2.status_code,
                  "replayed": r2.headers.get("idempotent-replayed"), "location": r2.headers.get("location"),
                  "rental_id": body.get("id")},
        "second_retry_same_id": r3.json().get("id") == body.get("id"),
        "rentals_created_in_db": len(rentals),
        "stored_key": list(stored) if stored else None,
    }
    result["ok"] = (first.startswith("no response") and r2.status_code == 201
                    and r2.headers.get("idempotent-replayed") == "true" and len(rentals) == 1
                    and result["second_retry_same_id"])
    Path(args.out).write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print("оренд у базі:", len(rentals), "→", "OK" if result["ok"] else "FAIL", "→", args.out)


if __name__ == "__main__":
    main()
