"""Ключі ідемпотентності змінювальних операцій (ЛР2 Web, О2.2). Власна реалізація, без готового компонента.

Що зберігається (таблиця idempotency_key, міграція 003): користувач, операція, ключ, відбиток
тіла (sha256 канонічного JSON), стан, код і тіло відповіді, Location, строк дії.

Як виконується запит з ключем – ОДНА транзакція PostgreSQL:
  1. pg_try_advisory_xact_lock(hash(користувач, операція, ключ)). Не вдалося – цей самий ключ
     зараз обробляє інша транзакція (на будь-якому екземплярі) -> 409 key-in-progress, Retry-After: 1.
     Чекати не треба: блокування знімається фіксацією або відкатом тієї транзакції.
  2. INSERT рядка ключа зі станом processing; ON CONFLICT – ключ уже використано:
     відбиток той самий -> збережена відповідь (Idempotent-Replayed: true), інший -> 422.
  3. Предметна дія (у точці збереження). Її результат – і успіх 201, і предметна відмова
     404/409 – записується в рядок ключа (state = completed) у тій самій транзакції.
  4. COMMIT. Лише після нього надсилається відповідь.
Окремої перевірки «чи є ключ» перед вставкою немає: між перевіркою й записом була б гонка.
Аварія до COMMIT відкочує і оренду, і ключ – повтор виконає операцію заново; аварія після
COMMIT (О2.3) – повтор отримає збережену відповідь.
"""

import asyncio
import hashlib
import json
import re
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import timedelta
from typing import Any

from fastapi.responses import JSONResponse
from psycopg import AsyncConnection
from psycopg.types.json import Jsonb

from . import config, db
from .faults import fault
from .jsonlog import log
from .problems import MEDIA_TYPE, Problem

_KEY = re.compile(r"^[\x21-\x7e]{8,128}$")  # видимі символи ASCII, 8–128


@dataclass
class Outcome:
    status: int
    body: dict[str, Any]
    location: str | None = None
    problem: bool = False


def fingerprint(body: dict[str, Any]) -> str:
    """Відбиток не залежить від порядку полів і пробілів: канонічний JSON."""
    canonical = json.dumps(body, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(canonical.encode()).hexdigest()


def require_key(raw: str | None) -> str:
    if raw is None or not raw.strip():
        raise Problem(400, "idempotency-key-required", "Idempotency-Key header required",
                      "repeat every attempt of this operation with the same key")
    if not _KEY.match(raw):
        raise Problem(400, "idempotency-key-invalid", "Invalid Idempotency-Key",
                      "the key must be 8-128 visible ASCII characters")
    return raw


def _response(outcome: Outcome, replayed: bool) -> JSONResponse:
    headers = {"Idempotent-Replayed": "true" if replayed else "false"}
    if outcome.location:
        headers["Location"] = outcome.location
    media = MEDIA_TYPE if outcome.problem else "application/json"
    return JSONResponse(outcome.body, status_code=outcome.status, headers=headers, media_type=media)


async def run(
    *,
    user_id: str,
    operation: str,
    key: str,
    body: dict[str, Any],
    path: str,
    action: Callable[[AsyncConnection], Awaitable[Outcome]],
) -> JSONResponse:
    request_hash = fingerprint(body)
    async with db.connection() as conn, conn.transaction():
        cur = await conn.execute(
            "SELECT pg_try_advisory_xact_lock(hashtextextended(%s, 0)) AS got",
            (f"{user_id}\x1f{operation}\x1f{key}",),
        )
        if not (await cur.fetchone())["got"]:
            raise Problem(409, "idempotency-key-in-progress", "Request with this key is still in progress",
                          "retry later with the same key", headers={"Retry-After": "1"})

        # прострочений ключ більше не діє: його можна використати знову
        await conn.execute(
            "DELETE FROM idempotency_key WHERE user_id = %s AND operation = %s AND key = %s AND expires_at <= now()",
            (user_id, operation, key),
        )
        cur = await conn.execute(
            """
                INSERT INTO idempotency_key (user_id, operation, key, request_hash, state, expires_at)
                VALUES (%s, %s, %s, %s, 'processing', now() + %s)
                ON CONFLICT DO NOTHING
                RETURNING 1
                """,
            (user_id, operation, key, request_hash, timedelta(hours=config.IDEMPOTENCY_TTL_H)),
        )
        if await cur.fetchone() is None:
            cur = await conn.execute(
                "SELECT * FROM idempotency_key WHERE user_id = %s AND operation = %s AND key = %s",
                (user_id, operation, key),
            )
            row = await cur.fetchone()
            if row["request_hash"] != request_hash:
                raise Problem(422, "idempotency-key-reused", "Idempotency-Key was used with another request",
                              "use a new key for a different request")
            stored = Outcome(row["status_code"], row["response"], row["location"],
                             problem=row["status_code"] >= 400)
            log("idempotency.replay", operation=operation, status=row["status_code"])
            return _response(stored, replayed=True)

        try:
            async with conn.transaction():  # точка збереження: відмова дії не скасовує рядок ключа
                outcome = await action(conn)
        except Problem as problem:
            outcome = Outcome(problem.status, {
                "type": config.PROBLEM_BASE + problem.slug, "title": problem.title, "status": problem.status,
                **({"detail": problem.detail} if problem.detail else {}), "instance": path, **problem.extra,
            }, problem=True)

        await conn.execute(
            """
                UPDATE idempotency_key SET state = 'completed', status_code = %s, response = %s, location = %s
                WHERE user_id = %s AND operation = %s AND key = %s
                """,
            (outcome.status, Jsonb(outcome.body), outcome.location, user_id, operation, key),
        )
        # COMMIT виконано: оренда й збережена відповідь зафіксовані разом
    fault("after_commit_before_response")  # О2.3: процес гине до надсилання відповіді
    return _response(outcome, replayed=False)


async def cleanup_loop() -> None:
    """Прибирання прострочених ключів порціями; безпечно на кількох екземплярах (SKIP LOCKED)."""
    while True:
        await asyncio.sleep(config.IDEMPOTENCY_CLEANUP_S)
        try:
            total = 0
            while True:
                async with db.connection() as conn:
                    cur = await conn.execute(
                        """
                        DELETE FROM idempotency_key WHERE ctid IN (
                          SELECT ctid FROM idempotency_key WHERE expires_at <= now()
                          LIMIT %s FOR UPDATE SKIP LOCKED)
                        """,
                        (config.IDEMPOTENCY_CLEANUP_BATCH,),
                    )
                    total += cur.rowcount
                if cur.rowcount < config.IDEMPOTENCY_CLEANUP_BATCH:
                    break
            if total:
                log("idempotency.cleanup", deleted=total)
        except Exception as exc:  # noqa: BLE001 - прибирання не має зупиняти сервер
            log("idempotency.cleanup_failed", error=type(exc).__name__)
