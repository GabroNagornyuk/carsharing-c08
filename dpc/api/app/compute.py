"""ЛР2 DPC: POST /v1/compute – паралельне інтегрування функції картки, GET /v1/compute/{id} – збережений результат.

Схема розрахунку (README, розділ «DPC · ЛР2»):
  послідовна частина (батьківський процес): перевірка параметрів, поділ вузлів на `workers`
  відрізків, запуск пулу (режим COMPUTE_POOL=fresh) і передавання завдань, об'єднання локальних
  сум у порядку номерів частин, множення на h, еталон і похибка;
  паралельна частина: кожен процес пулу рахує локальну суму свого відрізка (integral.py).

Режими спільної суми (`mode`, О2.4): local – локальні суми повертаються й додаються в кінці;
lock – процеси додають порції до спільної змінної під блокуванням; none – те саме без
блокування (втрачені оновлення, лише для досліду).
"""

import asyncio
import multiprocessing as mp
import os
import time
import uuid
from collections import OrderedDict
from concurrent.futures import ProcessPoolExecutor, wait
from typing import Annotated, Any, Literal

from fastapi import APIRouter, Depends, Response
from pydantic import BaseModel, ConfigDict, Field, model_validator

from . import auth, config, integral
from .jsonlog import log
from .problems import Problem

router = APIRouter(prefix="/v1", dependencies=[Depends(auth.require_token)])

_one_at_a_time = asyncio.Lock()  # розрахунки не змагаються за ядра один з одним: чисті вимірювання
_results: OrderedDict[str, dict[str, Any]] = OrderedDict()
_warm: ProcessPoolExecutor | None = None


class ComputeRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    a: float = Field(0.0, ge=0, le=24, description="початок, години від 6:00")
    b: float = Field(12.0, ge=0, le=24, description="кінець, години від 6:00")
    n: int = Field(ge=1, le=400_000_000, description="кількість частин поділу")
    workers: int = Field(1, ge=1, le=8)
    parts: int | None = Field(None, ge=1, le=4096, description="кількість частин поділу; типово = workers (О2.3)")
    method: Literal["midpoint", "trapezoid", "simpson"] = "midpoint"
    order: Literal["forward", "reverse"] = "forward"
    summation: Literal["naive", "pairwise", "kahan"] = "naive"
    mode: Literal["local", "lock", "none"] = "local"
    portion: int = Field(1, ge=1, le=10_000_000, description="вузлів між додаваннями до спільної суми (lock, none)")
    cars: float = Field(10.0, gt=0, le=100_000, description="N – парк зони, авто")
    tau: float = Field(1.5, gt=0, le=24, description="τ – стала розгону попиту, год")

    @model_validator(mode="after")
    def _check(self) -> ComputeRequest:
        if self.b <= self.a:
            raise ValueError("b must be greater than a")
        if self.method == "simpson" and self.n % 2:
            raise ValueError("simpson needs an even n")
        return self


def _executor(workers: int, shared: Any = None, lock: Any = None) -> ProcessPoolExecutor:
    ctx = mp.get_context(config.MP_START) if config.MP_START else mp.get_context()
    if shared is not None:
        return ProcessPoolExecutor(workers, mp_context=ctx, initializer=integral.init_shared, initargs=(shared, lock))
    return ProcessPoolExecutor(workers, mp_context=ctx)


def _run(req: ComputeRequest) -> dict[str, Any]:
    """Виконується в окремому потоці, щоб цикл подій api відповідав і під час розрахунку."""
    t0 = time.perf_counter()
    h = (req.b - req.a) / req.n
    p = {**req.model_dump(), "h": h}
    m = integral.nodes(req.method, req.n)
    ranges = integral.split(m, req.parts or req.workers)  # частин більше за процеси – пул роздає їх вільним

    shared = lock = None
    if req.mode == "local" and config.COMPUTE_POOL == "warm" and _warm is not None and req.workers <= config.WARM_POOL_SIZE:
        pool, own = _warm, False
    else:
        if req.mode != "local":
            ctx = mp.get_context(config.MP_START) if config.MP_START else mp.get_context()
            shared, lock = ctx.RawValue("d", 0.0), ctx.Lock()
        pool, own = _executor(req.workers, shared, lock), True
    try:
        # запуск пулу: процеси створюються під час перших завдань, тож «прогріваємо» кожен
        wait([pool.submit(integral.noop) for _ in range(req.workers)])
        t1 = time.perf_counter()
        task = integral.part_local if req.mode == "local" else integral.part_shared
        futures = [pool.submit(task, p, k, lo, hi) for k, (lo, hi) in enumerate(ranges)]
        parts = [fut.result() for fut in futures]  # порядок номерів частин, не порядок завершення
        t2 = time.perf_counter()
    finally:
        if own:
            pool.shutdown(wait=True)
    t3 = time.perf_counter()

    sum_of_parts = 0.0
    for part in parts:  # об'єднання – послідовна частина, фіксований порядок
        sum_of_parts += part["sum"]
    total = sum_of_parts if req.mode == "local" else shared.value
    value = total * h
    ref = integral.reference(req.a, req.b, req.cars, req.tau)
    error = value - ref
    t4 = time.perf_counter()

    result = {
        "value": value,
        "reference": ref,
        "error": error,
        "rel_error": abs(error) / abs(ref),
        "tolerance": config.COMPUTE_TOLERANCE,
        "within_tolerance": abs(error) <= config.COMPUTE_TOLERANCE * abs(ref),
        "wall_s": t4 - t0,
        "workers": req.workers,
        "parts": len(ranges),
        "method": req.method,
        "n": req.n,
        "mode": req.mode,
        "summation": req.summation,
        "order": req.order,
        "unit": "машино-години",
    }
    if req.mode != "local":  # що «мало б бути» за сумою частин і скільки загублено
        result["sum_of_parts_value"] = sum_of_parts * h
        result["lost_value"] = (sum_of_parts - total) * h
    return {
        "state": "done",
        "active_parts": 0,
        "result": result,
        "parts": [{**x, "seconds": round(x["seconds"], 6)} for x in parts],
        "timing": {
            "pool": "warm" if not own else "fresh",
            "pool_start_s": t1 - t0,
            "parts_s": t2 - t1,
            "pool_shutdown_s": t3 - t2,
            "merge_s": t4 - t3,
            "slowest_part_s": max(x["seconds"] for x in parts),
            "fastest_part_s": min(x["seconds"] for x in parts),
            "cpu_count": os.cpu_count(),
            "start_method": (mp.get_context(config.MP_START) if config.MP_START else mp.get_context()).get_start_method(),
        },
    }


@router.post("/compute")
async def compute(body: ComputeRequest, response: Response) -> dict[str, Any]:
    job_id = str(uuid.uuid4())
    async with _one_at_a_time:
        try:
            job = await asyncio.to_thread(_run, body)
        except Exception as exc:  # noqa: BLE001 - аварія процесу пулу тощо
            log("compute.failed", error=type(exc).__name__)
            raise Problem(500, "compute-failed", "Computation failed", type(exc).__name__) from None
    job = {"id": job_id, **job, "params": body.model_dump()}
    _results[job_id] = job
    while len(_results) > 100:
        _results.popitem(last=False)
    r = job["result"]
    log("compute.done", workers=r["workers"], n=r["n"], mode=r["mode"], wall_s=round(r["wall_s"], 4),
        rel_error=r["rel_error"])
    response.headers["Location"] = f"/v1/compute/{job_id}"
    return job


@router.get("/compute/{job_id}")
async def get_compute(job_id: Annotated[str, Field(max_length=64)]) -> dict[str, Any]:
    job = _results.get(job_id)
    if job is None:
        raise Problem(404, "compute-not-found", "Computation not found")
    return job


def start() -> None:
    """COMPUTE_POOL=warm: пул створюється один раз під час запуску api й живе до зупинки."""
    global _warm
    if config.COMPUTE_POOL == "warm":
        _warm = _executor(config.WARM_POOL_SIZE)
        wait([_warm.submit(integral.noop) for _ in range(config.WARM_POOL_SIZE)])
        log("compute.warm_pool", size=config.WARM_POOL_SIZE)


def stop() -> None:
    if _warm is not None:
        _warm.shutdown(wait=False, cancel_futures=True)
