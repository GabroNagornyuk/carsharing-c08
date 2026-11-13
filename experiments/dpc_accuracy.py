# /// script
# requires-python = ">=3.12"
# dependencies = ["httpx>=0.28"]
# ///
"""DPC О2.2, Д2.1, Д2.4: точність розрахунку.

    uv run experiments/dpc_accuracy.py

Частина 1 (О2.2, Д2.1). Малі n, де похибка методу значно більша за похибку округлення:
похибка для n і 2n трьома методами, відношення err(n)/err(2n) і теоретична межа.
Очікування: середні прямокутники й трапеції – відношення ≈ 4 (порядок 2), Сімпсон – ≈ 16
(порядок 4); фактична похибка не більша за межу.

Частина 2 (Д2.4). Великі n, де похибка методу (~1e-15 відносної) менша за похибку
округлення: послідовне, попарне й компенсоване (Кехен) сумування, n і 2n, один процес.
"""

import argparse
import json
import math
from pathlib import Path

import httpx
from common import DPC, DPC_AUTH

A, B, CARS, TAU = 0.0, 12.0, 10.0, 1.5


def bound(method: str, n: int) -> float:
    """Межі похибки складених формул; похідні f(t) = N(1 − e^(−t/τ)) спадають, max |f^(k)| – у t = a."""
    h = (B - A) / n
    m2 = CARS / TAU**2 * math.exp(-A / TAU)
    m4 = CARS / TAU**4 * math.exp(-A / TAU)
    return {"midpoint": (B - A) * h**2 / 24 * m2,
            "trapezoid": (B - A) * h**2 / 12 * m2,
            "simpson": (B - A) * h**4 / 180 * m4}[method]


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default="results/dpc-o2.2-accuracy.json")
    ap.add_argument("--big", default="10000000,20000000")
    args = ap.parse_args()
    out: dict = {"experiment": "dpc-o2.2-accuracy", "a": A, "b": B, "cars": CARS, "tau": TAU, "methods": [], "summation": []}

    with httpx.Client(base_url=DPC, headers=DPC_AUTH, timeout=900) as c:
        def run(**body):
            return c.post("/v1/compute", json={"a": A, "b": B, "cars": CARS, "tau": TAU, **body}).json()["result"]

        print("| метод | n | похибка | межа | err(n)/err(2n) |\n|---|---|---|---|---|")
        for method in ("midpoint", "trapezoid", "simpson"):
            prev = None
            for n in (16, 32, 64, 128, 256):
                r = run(n=n, workers=1, method=method)
                ratio = prev / r["error"] if prev else None
                row = {"method": method, "n": n, "value": r["value"], "error": r["error"],
                       "bound": bound(method, n), "ratio": ratio}
                out["methods"].append(row)
                print(f"| {method} | {n} | {r['error']:.3e} | {row['bound']:.3e} | {f'{ratio:.3f}' if ratio else '–'} |")
                prev = r["error"]

        print("\n| сумування | n | відносна похибка | час, с |\n|---|---|---|---|")
        for summation in ("naive", "pairwise", "kahan"):
            for n in (int(x) for x in args.big.split(",")):
                r = run(n=n, workers=1, summation=summation)
                out["summation"].append({"summation": summation, "n": n, "error": r["error"],
                                         "rel_error": r["rel_error"], "wall_s": r["wall_s"]})
                print(f"| {summation} | {n} | {r['rel_error']:.2e} | {r['wall_s']:.2f} |")
    Path(args.out).write_text(json.dumps(out, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print("→", args.out)


if __name__ == "__main__":
    main()
