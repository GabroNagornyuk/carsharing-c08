"""ЛР3 Web: вхід, вихід, поточний користувач, роль staff.

Спосіб входу – сесія в cookie (О3.1). Під час входу сервер створює випадковий токен (32 байти),
кладе його в cookie `session` (HttpOnly, SameSite=Lax, Path=/) і записує в таблицю session лише
sha256 токена разом зі строком дії. Сесія живе в базі, тому вхід через app-a працює і на app-b.
Вихід видаляє рядок сесії: старий cookie після цього більше нічого не відкриває.

Заголовок X-User-Id з ЛР1–ЛР2 більше не приймається: користувача визначає лише сесія.

Д3.3 – обмеження спроб входу: не більше LOGIN_MAX_FAILURES невдалих спроб на логін за
LOGIN_WINDOW_MIN хвилин, далі 429 з Retry-After. Лічильник у базі, тому спільний для обох
екземплярів; спроби одного логіна впорядковує pg_advisory_xact_lock.
"""

import hashlib
import hmac
import secrets
from dataclasses import dataclass
from datetime import timedelta
from typing import Annotated, Any

from fastapi import APIRouter, Depends, Request, Response
from pydantic import BaseModel, ConfigDict, Field

from . import config, db
from .jsonlog import log
from .problems import Problem

COOKIE = "session"

router = APIRouter(prefix="/v1")


@dataclass(frozen=True)
class User:
    id: str
    name: str
    role: str


# ---------------------------------------------------------------- паролі й токени


def hash_password(password: str, salt: bytes | None = None, n: int = 2**14, r: int = 8, p: int = 1) -> str:
    salt = salt or secrets.token_bytes(16)
    digest = hashlib.scrypt(password.encode(), salt=salt, n=n, r=r, p=p, dklen=32)
    return f"scrypt${n}${r}${p}${salt.hex()}${digest.hex()}"


def check_password(password: str, stored: str | None) -> bool:
    if not stored:
        # користувача немає: рахуємо хеш однаково довго, щоб час відповіді не видавав, чи існує логін
        hash_password(password, b"\0" * 16)
        return False
    _, n, r, p, salt, digest = stored.split("$")
    actual = hashlib.scrypt(password.encode(), salt=bytes.fromhex(salt), n=int(n), r=int(r), p=int(p), dklen=32)
    return hmac.compare_digest(actual.hex(), digest)


def token_hash(token: str) -> str:
    return hashlib.sha256(token.encode()).hexdigest()


# ---------------------------------------------------------------- поточний користувач


async def current_user(request: Request) -> User:
    token = request.cookies.get(COOKIE)
    if not token:
        raise Problem(401, "unauthenticated", "Login required", "log in with POST /v1/auth/login")
    async with db.connection() as conn:
        cur = await conn.execute(
            """
            SELECT u.id, u.name, u.role FROM session s JOIN app_user u ON u.id = s.user_id
            WHERE s.token_hash = %s AND s.expires_at > now()
            """,
            (token_hash(token),),
        )
        row = await cur.fetchone()
    if row is None:  # вийшов, строк минув або токен вигаданий – однаково 401
        raise Problem(401, "unauthenticated", "Session expired or invalid", "log in again")
    return User(**row)


async def staff_user(user: Annotated[User, Depends(current_user)]) -> User:
    """Функції персоналу: роль перевіряє сервер, а не кнопка в клієнті."""
    if user.role != "staff":
        raise Problem(403, "forbidden", "Staff only", "this action is available to the staff role only")
    return user


# ---------------------------------------------------------------- маршрути


class Login(BaseModel):
    model_config = ConfigDict(extra="forbid")
    login: str = Field(min_length=1, max_length=64)
    password: str = Field(min_length=1, max_length=256)


def _me_json(user: dict[str, Any] | User) -> dict[str, Any]:
    u = user if isinstance(user, dict) else user.__dict__
    return {"id": u["id"], "name": u["name"], "role": u["role"]}


@router.post("/auth/login")
async def login(body: Login, response: Response) -> dict[str, Any]:
    window = timedelta(minutes=config.LOGIN_WINDOW_MIN)
    async with db.connection() as conn, conn.transaction():
        # спроби одного логіна – по черзі, на будь-якому екземплярі
        await conn.execute("SELECT pg_advisory_xact_lock(hashtextextended('login:' || %s, 0))", (body.login,))
        cur = await conn.execute(
            "SELECT count(*) AS n, min(failed_at) AS first FROM login_failure"
            " WHERE login = %s AND failed_at > now() - %s",
            (body.login, window),
        )
        fails = await cur.fetchone()
        if fails["n"] >= config.LOGIN_MAX_FAILURES:
            cur = await conn.execute(
                "SELECT ceil(extract(epoch FROM %s - (now() - %s)))::int AS s", (fails["first"], window)
            )
            retry = max(1, (await cur.fetchone())["s"])
            log("auth.login_limited", login=body.login)
            raise Problem(
                429,
                "too-many-login-attempts",
                "Too many failed login attempts",
                f"try again in {retry} s",
                headers={"Retry-After": str(retry)},
            )

        cur = await conn.execute("SELECT id, name, role, password_hash FROM app_user WHERE login = %s", (body.login,))
        user = await cur.fetchone()
        valid = check_password(body.password, user["password_hash"] if user else None)
        if not valid:
            # невдала спроба фіксується разом із транзакцією (виняток тут скасував би запис)
            await conn.execute("INSERT INTO login_failure (login) VALUES (%s)", (body.login,))
        else:
            await conn.execute("DELETE FROM login_failure WHERE login = %s", (body.login,))
            token = secrets.token_urlsafe(32)
            cur = await conn.execute(
                "INSERT INTO session (token_hash, user_id, expires_at) VALUES (%s, %s, now() + %s) RETURNING expires_at",
                (token_hash(token), user["id"], timedelta(minutes=config.SESSION_TTL_MIN)),
            )
            expires_at = (await cur.fetchone())["expires_at"]

    if not valid:
        log("auth.login_failed", login=body.login)
        # однакова відповідь для невідомого логіна й неправильного пароля
        raise Problem(401, "invalid-credentials", "Invalid login or password")

    response.set_cookie(
        COOKIE,
        token,
        max_age=config.SESSION_TTL_MIN * 60,
        httponly=True,
        samesite="lax",
        secure=config.COOKIE_SECURE,
        path="/",
    )
    log("auth.login", user_id=user["id"])
    return {**_me_json(user), "expires_at": expires_at.isoformat()}


@router.post("/auth/logout", status_code=204)
async def logout(request: Request, user: Annotated[User, Depends(current_user)]) -> Response:
    async with db.connection() as conn:
        await conn.execute("DELETE FROM session WHERE token_hash = %s", (token_hash(request.cookies[COOKIE]),))
    response = Response(status_code=204)
    response.delete_cookie(COOKIE, path="/", httponly=True, samesite="lax", secure=config.COOKIE_SECURE)
    log("auth.logout", user_id=user.id)
    return response


@router.get("/me")
async def me(user: Annotated[User, Depends(current_user)]) -> dict[str, Any]:
    return _me_json(user)


async def cleanup(conn: Any) -> None:
    """Прибирання прострочених сесій і старих невдалих спроб (викликає фонове завдання ЛР2)."""
    await conn.execute("DELETE FROM session WHERE expires_at <= now()")
    await conn.execute(
        "DELETE FROM login_failure WHERE failed_at <= now() - %s", (timedelta(minutes=config.LOGIN_WINDOW_MIN),)
    )
