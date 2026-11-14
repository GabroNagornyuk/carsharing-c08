"""The API process (app-a, app-b): `uvicorn app.main:app --host 0.0.0.0 --port 8000`.

Lab 1 (C08): cars.py (free cars of a zone), rentals.py (main operation POST /v1/rentals),
users.py (lab 3: cookie session, /v1/auth/*, /v1/me). Places for the next labs:
  TODO (O1.5)       list of rentals with a cursor (optional item)
  Lab 2 (C08): idempotency.py (Idempotency-Key, one transaction with the rental,
                    fault("after_commit_before_response")), cleanup of expired keys
  Lab 3 (C08): users.py (login, logout, /v1/me, staff role, D3.3 login limit), cars.py (staff function),
                    browser.py (O3.3: Origin / Sec-Fetch-Site check), CORS for CLIENT_ORIGIN with credentials
  TODO (O4.1-O4.4)  long operation, outbox in the same transaction (publisher and handler are
                    separate processes: app/publisher.py, app/worker.py)
  TODO (O5.1)       POST /v1/ws-tickets; the gateway itself is app/gateway.py
  TODO (O6.3)       SIGTERM: lifecycle.start_draining(), finish started requests
The contract of all routes is your openapi.yaml (start from openapi.example.yaml).
"""

import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from . import (
    browser,
    cache,
    cars,
    config,
    db,
    idempotency,
    metrics,
    middleware,
    problems,
    rentals,
    service,
    telemetry,
    users,
)
from .jsonlog import log


@asynccontextmanager
async def lifespan(_: FastAPI) -> AsyncIterator[None]:
    await db.open_pool()
    cleanup = asyncio.create_task(idempotency.cleanup_loop())

    log("app.started")
    yield

    cleanup.cancel()

    await cache.close()
    await db.close_pool()

    log("app.stopped")


# The contract is openapi.yaml of your repository, so the generated schema routes are off.
app = FastAPI(lifespan=lifespan, docs_url=None, redoc_url=None, openapi_url=None)
middleware.install(app)
problems.install(app)
app.include_router(service.router)
app.include_router(metrics.router)
telemetry.install(app)
app.include_router(cars.router)
app.include_router(rentals.router)
app.include_router(users.router)
# О3.3: змінювальний запит зі стороннього Origin відхиляється до виконання дії
app.add_middleware(browser.CsrfGuard)
# Клієнт працює поза Compose на CLIENT_ORIGIN і звертається через nginx. О3.3: лише перелічені
# походження (без * і без віддзеркалення будь-якого Origin), з обліковими даними – cookie сесії.
# X-User-Id більше не дозволено: користувача визначає сесія.
app.add_middleware(
    CORSMiddleware,
    allow_origins=config.CLIENT_ORIGINS,
    allow_credentials=True,
    allow_methods=["GET", "POST", "PATCH"],
    allow_headers=["Content-Type", "Idempotency-Key", "If-Match"],
    expose_headers=["Location", "ETag", "X-Request-ID", "X-Instance", "Retry-After", "Idempotent-Replayed", "X-Already-Cancelled"],
)
