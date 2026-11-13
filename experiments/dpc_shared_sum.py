# /// script
# requires-python = ">=3.12"
# dependencies = ["httpx>=0.28"]
# ///
"""DPC О2.4: спільна сума без синхронізації, з блокуванням і з локальними сумами.

    uv run experiments/dpc_shared_sum.py --n 400000 --workers 4 --runs 3

Режим задає параметр запиту `mode` (none | lock | local); той самий розрахунок, ті самі n і p.
У none і lock кожен процес після кожного вузла (portion = 1) додає доданок до спільної змінної
в пам'яті, спільній для процесів (multiprocessing.RawValue). `shared += x` – це читання,
додавання й запис; без блокування процеси перезаписують додавання один одного.
Очікування: none – значення менше за еталон, lost_value > 0, поза допуском; lock і local – у
межах допуску 1e-6; lock повільніший за local через тисячі захоплень блокування.
"""

import argparse
import json
from pathlib import Path

import httpx
from common import DPC, DPC_AUTH


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--n", type=int, default=400_000)
    ap.add_argument("--workers", type=int, default=4)
    ap.add_argument("--portion", type=int, default=1)
    ap.add_argument("--runs", type=int, default=3)
    ap.add_argument("--out", default="results/dpc-o2.4-shared-sum.json")
    args = ap.parse_args()

    rows = []
    with httpx.Client(base_url=DPC, headers=DPC_AUTH, timeout=600) as c:
        for mode in ("none", "lock", "local"):
            for i in range(args.runs):
                r = c.post("/v1/compute", json={"n": args.n, "workers": args.workers, "mode": mode,
                                                 "portion": args.portion}).json()["result"]
                row = {"mode": mode, "run": i + 1, "value": r["value"], "reference": r["reference"],
                       "rel_error": r["rel_error"], "within_tolerance": r["within_tolerance"],
                       "lost_value": r.get("lost_value"), "wall_s": r["wall_s"]}
                rows.append(row)
                lost = f"загублено {row['lost_value']:.4f}" if row["lost_value"] is not None else ""
                print(f"{mode:5} #{i + 1}: {r['value']:.9f} (еталон {r['reference']:.9f}), "
                      f"rel_error {r['rel_error']:.2e}, допуск {'так' if r['within_tolerance'] else 'НІ'} "
                      f"{lost}  {r['wall_s']:.3f} с")
    Path(args.out).write_text(json.dumps({"experiment": "dpc-o2.4-shared-sum", "n": args.n, "workers": args.workers,
                                          "portion": args.portion, "rows": rows}, ensure_ascii=False, indent=2) + "\n",
                              encoding="utf-8")
    print("→", args.out)


if __name__ == "__main__":
    main()
