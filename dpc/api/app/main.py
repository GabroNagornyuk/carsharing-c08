"""The `api` process: `uvicorn app.main:app --host 0.0.0.0 --port 8000`.

Where each lab plugs in (item numbers of the guide):
  Lab 1 (C08): rental_starts.py - receiver POST /v1/rental-starts, bounded queue, worker pool,
                    zone_cars resource under a row lock. TODO (O1.5, D1.4) optional items
  Lab 2 (C08): compute.py + integral.py - POST /v1/compute, GET /v1/compute/{id}, process pool,
                    shared-sum modes. TODO (D2.5, D2.6) optional items
  Lab 3 (C08): EXECUTION=ray - rental_starts.py dispatches the queue to Ray tasks (ray_tasks.py,
                    IN_FLIGHT_LIMIT), effects.py is the receiver (decision table, one transaction),
                    fault("after_effect_before_ack") in the task; D3.3 Idempotency-Key. TODO (D3.4)
  TODO (O4.2)       POST /v1/reports/main/refresh calls the given Spark runner (RUNNER_URL)
  TODO (O5.x)       GET /v1/panel, /v1/panel/batch: the windows written by your stream job
  TODO (O6.x)       GET /v1/coordinator, /v1/history; coordinators are separate processes
Protect your /v1 routers with `dependencies=[Depends(auth.require_token)]`.
"""

import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from . import compute, config, db, effects, middleware, problems, rental_starts, service
from .jsonlog import log


@asynccontextmanager
async def lifespan(_: FastAPI) -> AsyncIterator[None]:
    await db.open_pool()
    await rental_starts.start()
    await asyncio.to_thread(compute.start)
    log("api.started")

    yield

    # TODO (D1.4): stop accepting, process what was accepted, then stop the workers
    await rental_starts.stop()
    compute.stop()
    await db.close_pool()
    log("api.stopped")


app = FastAPI(lifespan=lifespan, docs_url=None, redoc_url=None, openapi_url=None)
middleware.install(app)
app.add_middleware(
    CORSMiddleware,
    allow_origins=config.CLIENT_ORIGINS,
    allow_methods=["*"],
    allow_headers=["Authorization", "Content-Type", "Idempotency-Key"],
    expose_headers=["Location", "Retry-After", "X-Request-ID", "Idempotent-Replayed"],
)
problems.install(app)
app.include_router(service.router)
app.include_router(rental_starts.router)
app.include_router(effects.router)
app.include_router(compute.router)
