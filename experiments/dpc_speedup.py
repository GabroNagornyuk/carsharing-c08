# /// script
# requires-python = ">=3.12"
# dependencies = ["httpx>=0.28"]
# ///
"""DPC О2.3: прискорення для 1, 2 і 4 процесів (медіана, розкид, S_p = T_1/T_p, E_p = S_p/p).

    uv run experiments/dpc_speedup.py --n 40000000 --runs 5 --label fresh

Вимірюваний час T_p – `result.wall_s` сервера: від отримання параметрів до готового результату,
тобто запуск пулу процесів (COMPUTE_POOL=fresh), передавання завдань, обчислення частин,
зупинка пулу й об'єднання. Окремо зберігається розклад часу (`timing`) – за ним формулюється
гіпотеза про обмеження прискорення. Порядок запусків перемішано (1,2,4,1,2,4,…), щоб фонове
навантаження не лягало на одну кількість процесів.
"""

import argparse
import json
import statistics
from pathlib import Path

import httpx
from common import DPC, DPC_AUTH


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--n", type=int, default=40_000_000)
    ap.add_argument("--runs", type=int, default=5)
    ap.add_argument("--workers", default="1,2,4")
    ap.add_argument("--parts-per-worker", type=int, default=1, help="частин на процес (1 – статичний поділ)")
    ap.add_argument("--label", default="fresh")
    ap.add_argument("--out", default=None)
    args = ap.parse_args()
    workers = [int(x) for x in args.workers.split(",")]

    runs: list[dict] = []
    with httpx.Client(base_url=DPC, headers=DPC_AUTH, timeout=600) as c:
        c.post("/v1/compute", json={"n": 1000, "workers": max(workers)})  # прогрів імпортів, не враховується
        for i in range(args.runs):
            for p in workers:
                job = c.post("/v1/compute", json={"a": 0, "b": 12, "n": args.n, "workers": p,
                                                  "parts": p * args.parts_per_worker}).json()
                r, t = job["result"], job["timing"]
                runs.append({"workers": p, "seconds": r["wall_s"], "rel_error": r["rel_error"],
                             "within_tolerance": r["within_tolerance"], **{k: t[k] for k in (
                                 "pool", "pool_start_s", "parts_s", "pool_shutdown_s", "slowest_part_s",
                                 "fastest_part_s", "cpu_count", "start_method")}})
                print(f"run {i + 1} p={p}: {r['wall_s']:.3f} s  (пул {t['pool_start_s'] * 1000:.0f} мс, "
                      f"частини {t['fastest_part_s']:.3f}–{t['slowest_part_s']:.3f} с)  rel_error={r['rel_error']:.1e}")

    t1 = statistics.median(x["seconds"] for x in runs if x["workers"] == 1)
    table = []
    for p in workers:
        times = [x["seconds"] for x in runs if x["workers"] == p]
        med = statistics.median(times)
        row = {"p": p, "median_s": med, "spread_s": max(times) - min(times), "runs": len(times),
               "S_p": t1 / med, "E_p": t1 / med / p,
               "pool_start_ms_median": 1000 * statistics.median(x["pool_start_s"] for x in runs if x["workers"] == p),
               "imbalance_ms_median": 1000 * statistics.median(
                   x["slowest_part_s"] - x["fastest_part_s"] for x in runs if x["workers"] == p)}
        table.append(row)
    print("\n| p | T_p, с (медіана) | розкид, с | запусків | S_p | E_p | запуск пулу, мс | нерівномірність частин, мс |")
    print("|---|---|---|---|---|---|---|---|")
    for r in table:
        print(f"| {r['p']} | {r['median_s']:.3f} | {r['spread_s']:.3f} | {r['runs']} | {r['S_p']:.2f} | {r['E_p']:.2f} "
              f"| {r['pool_start_ms_median']:.1f} | {r['imbalance_ms_median']:.1f} |")
    out = Path(args.out or f"results/dpc-o2.3-speedup-{args.label}.json")
    out.write_text(json.dumps({"experiment": "dpc-o2.3-speedup", "label": args.label, "n": args.n,
                               "parts_per_worker": args.parts_per_worker,
                               "table": table, "runs": runs}, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print("→", out)


if __name__ == "__main__":
    main()
