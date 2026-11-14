# /// script
# requires-python = ">=3.12"
# dependencies = ["httpx>=0.28"]
# ///
"""DPC О3.4: відмова виконавця Ray після приймання ефекту (after_effect_before_ack).

Кожен повтор:
1. Вмикає відмову: `docker compose exec -T api touch /faults/after_effect_before_ack`
   (або файл у FAULT_DIR_LOCAL, якщо стенд запущено поза Docker). Каталог /faults спільний для
   api і вузлів Ray.
2. Надсилає одну команду старту оренди в зону loadtest і чекає кінцевого стану.
   Задача доставляє ефект приймачу, той фіксує його, а процес виконавця аварійно завершується
   до повернення результату. Ray повторює задачу з тим самим command_id.
3. Читає рішення приймача й ресурс зони: доставок 2, ефектів 1, invariant_ok.

    EXECUTION=ray docker compose up -d api ray-head ray-worker
    uv run experiments/dpc_ray_fault.py --repeats 3
"""

import argparse
import json
import os
import subprocess
import time
import uuid
from pathlib import Path

import httpx

from common import DPC, DPC_AUTH

PLACE = "after_effect_before_ack"


def arm(fault_dir: str | None) -> None:
    if fault_dir:
        Path(fault_dir, PLACE).touch()
    else:
        subprocess.run(["docker", "compose", "exec", "-T", "api", "touch", f"/faults/{PLACE}"], check=True)


def once(c: httpx.Client, fault_dir: str | None) -> dict:
    before = c.get("/v1/zone-cars/loadtest").json()
    cid = f"flt-{uuid.uuid4().hex[:8]}"
    arm(fault_dir)
    t0 = time.monotonic()
    r = c.post("/v1/rental-starts", json={"command_id": cid, "car_id": "lt-car-fault", "driver_id": "u-driver-1",
                                          "zone": "loadtest"})
    assert r.status_code == 202, r.text
    # стан done з'являється вже після першої доставки; чекаємо підтвердження задачі (acked_at)
    decision: dict = {}
    for _ in range(160):
        state = c.get(f"/v1/rental-starts/{cid}").json()
        if state["state"] == "failed":
            break
        r = c.get(f"/v1/rental-start-effects/{cid}")
        if r.status_code == 200 and r.json().get("acked_at"):
            decision = r.json()
            break
        time.sleep(0.25)
    after = c.get("/v1/zone-cars/loadtest").json()
    fault_left = Path(fault_dir, PLACE).exists() if fault_dir else None
    return {
        "command_id": cid,
        "state": state["state"],
        "seconds": round(time.monotonic() - t0, 2),
        "deliveries": decision.get("deliveries"),
        "effects_of_command": decision.get("effects"),
        "first_executor": decision.get("executor"),
        "acked_by": decision.get("acked_by"),
        "rented_before": before["rented"],
        "rented_after": after["rented"],
        "invariant_ok": after["invariant_ok"],
        "fault_consumed": None if fault_left is None else not fault_left,
    }


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--repeats", type=int, default=3)
    args = ap.parse_args()
    fault_dir = os.environ.get("FAULT_DIR_LOCAL")
    results = []
    with httpx.Client(base_url=DPC, headers=DPC_AUTH, timeout=30) as c:
        for i in range(args.repeats):
            res = once(c, fault_dir)
            results.append(res)
            print(f"#{i + 1} {res}")
    out = Path("results") / "dpc-o3.4-after-effect-before-ack.json"
    out.parent.mkdir(exist_ok=True)
    out.write_text(json.dumps(results, indent=2, ensure_ascii=False))
    print(f"saved {out}")


if __name__ == "__main__":
    main()
