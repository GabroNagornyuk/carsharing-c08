"""Поточний користувач. До ЛР3 його ідентифікатор передає заголовок X-User-Id
(заздалегідь створений користувач з демоданих); власника визначають лише за ним, не за тілом."""

from dataclasses import dataclass

from fastapi import Request

from . import db
from .problems import Problem


@dataclass(frozen=True)
class User:
    id: str
    name: str
    role: str


async def current_user(request: Request) -> User:
    user_id = request.headers.get("x-user-id", "").strip()
    if not user_id:
        raise Problem(401, "unauthenticated", "X-User-Id header required")
    async with db.connection() as conn:
        cur = await conn.execute("SELECT id, name, role FROM app_user WHERE id = %s", (user_id,))
        row = await cur.fetchone()
    if row is None:
        raise Problem(401, "unauthenticated", "Unknown user")
    return User(**row)
