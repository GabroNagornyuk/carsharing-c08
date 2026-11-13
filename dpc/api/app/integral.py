"""ЛР2 DPC: чисельне інтегрування функції картки C08. Цей модуль імпортують процеси пулу, тому
в ньому лише стандартна бібліотека (швидкий запуск процесу).

Функція картки: n(t) = N·(1 − e^(−t/τ)) – скільки авто зони в оренді через t годин від 6:00
(N – парк зони, τ – стала «розгону» попиту, год). Інтеграл на [a; b] – машино-години оренди, за
якими рахують виручку. Еталон – через первісну F(t) = N·(t + τ·e^(−t/τ)).

Поділ: вузли 0..m−1 (m = n для midpoint, n+1 для trapezoid/simpson) ріжуться на `workers`
суцільних відрізків [k·m/p; (k+1)·m/p) – без пропусків і перекриття. Кожен процес рахує
локальну суму ваг·f(x) своїх вузлів; батьківський процес додає локальні суми в порядку номерів
частин (детерміновано, незалежно від того, хто закінчив першим).
"""

import math
import time
from typing import Any

METHODS = ("midpoint", "trapezoid", "simpson")
SUMMATION = ("naive", "pairwise", "kahan")  # Д2.4
ORDERS = ("forward", "reverse")


def f(t: float, cars: float, tau: float) -> float:
    return cars * (1.0 - math.exp(-t / tau))


def antiderivative(t: float, cars: float, tau: float) -> float:
    return cars * (t + tau * math.exp(-t / tau))


def reference(a: float, b: float, cars: float, tau: float) -> float:
    return antiderivative(b, cars, tau) - antiderivative(a, cars, tau)


def nodes(method: str, n: int) -> int:
    return n if method == "midpoint" else n + 1


def split(total: int, parts: int) -> list[tuple[int, int]]:
    """Суцільні відрізки індексів, розміри відрізняються не більше ніж на 1."""
    return [(k * total // parts, (k + 1) * total // parts) for k in range(parts)]


def _weight(method: str, i: int, n: int) -> float:
    """Вага вузла i в одиницях h (midpoint – 1; трапеції – 1/2 на кінцях; Сімпсон – 1,4,2,…,4,1 /3)."""
    if method == "midpoint":
        return 1.0
    if method == "trapezoid":
        return 0.5 if i in (0, n) else 1.0
    if i in (0, n):
        return 1.0 / 3.0
    return 4.0 / 3.0 if i % 2 else 2.0 / 3.0


def _terms(p: dict[str, Any], lo: int, hi: int):
    a, h, cars, tau, method, n = p["a"], p["h"], p["cars"], p["tau"], p["method"], p["n"]
    shift = 0.5 if method == "midpoint" else 0.0
    rng = range(lo, hi) if p["order"] == "forward" else range(hi - 1, lo - 1, -1)
    for i in rng:
        yield _weight(method, i, n) * cars * (1.0 - math.exp(-(a + (i + shift) * h) / tau))


def _naive_midpoint(p: dict[str, Any], lo: int, hi: int) -> float:
    """Гаряча петля основного розрахунку (midpoint, послідовне сумування) – без зайвих викликів."""
    a, h, inv_tau = p["a"], p["h"], 1.0 / p["tau"]
    exp = math.exp
    s = 0.0
    rng = range(lo, hi) if p["order"] == "forward" else range(hi - 1, lo - 1, -1)
    for i in rng:
        s += 1.0 - exp(-(a + (i + 0.5) * h) * inv_tau)
    return s * p["cars"]


def _pairwise(terms) -> float:
    """Попарне сумування: блоки по 64 складаються послідовно, далі суми – деревом (стек рівнів)."""
    stack: list[tuple[int, float]] = []  # (рівень, сума)
    block, count = 0.0, 0
    for x in terms:
        block += x
        count += 1
        if count == 64:
            level, value = 0, block
            while stack and stack[-1][0] == level:
                value += stack.pop()[1]
                level += 1
            stack.append((level, value))
            block, count = 0.0, 0
    total = block
    while stack:
        total = stack.pop()[1] + total
    return total


def _kahan(terms) -> float:
    """Компенсоване сумування Кехена: c накопичує втрачені молодші розряди."""
    s, c = 0.0, 0.0
    for x in terms:
        y = x - c
        t = s + y
        c = (t - s) - y
        s = t
    return s


def local_sum(p: dict[str, Any], lo: int, hi: int) -> float:
    """Локальна сума ваг·f(x) вузлів [lo; hi) у одиницях h (множення на h – після об'єднання)."""
    if p["summation"] == "naive":
        if p["method"] == "midpoint":
            return _naive_midpoint(p, lo, hi)
        s = 0.0
        for x in _terms(p, lo, hi):
            s += x
        return s
    if p["summation"] == "kahan":
        return _kahan(_terms(p, lo, hi))
    return _pairwise(_terms(p, lo, hi))


# ---------------------------------------------------------------- задачі процесів пулу


def part_local(p: dict[str, Any], index: int, lo: int, hi: int) -> dict[str, Any]:
    """Режим local: процес повертає свою суму, спільної пам'яті немає."""
    started = time.perf_counter()
    s = local_sum(p, lo, hi)
    return {"index": index, "from": lo, "to": hi, "sum": s, "seconds": time.perf_counter() - started}


_shared: Any = None  # спільна сума (режими none і lock), передається ініціалізатором пулу
_lock: Any = None


def init_shared(shared: Any, lock: Any) -> None:
    global _shared, _lock
    _shared, _lock = shared, lock


def part_shared(p: dict[str, Any], index: int, lo: int, hi: int) -> dict[str, Any]:
    """Режими none і lock: після кожної порції з `portion` вузлів процес додає її суму до СПІЛЬНОЇ
    змінної. `shared.value += x` – це читання, додавання й запис; без блокування два процеси
    читають те саме значення, і одне з додавань губиться (втрачене оновлення)."""
    assert _shared is not None
    started = time.perf_counter()
    step = p["portion"]
    added = 0.0
    for start in range(lo, hi, step):
        portion = local_sum(p, start, min(start + step, hi))
        added += portion
        if p["mode"] == "lock":
            with _lock:
                _shared.value += portion
        else:
            _shared.value += portion  # навмисно без синхронізації
    return {"index": index, "from": lo, "to": hi, "sum": added, "seconds": time.perf_counter() - started}


def noop() -> int:
    return 0
