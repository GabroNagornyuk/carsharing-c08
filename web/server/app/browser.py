"""ЛР3 Web, О3.3: захист змінювальних запитів браузера від стороннього сайту (CSRF).

Cookie сесії браузер додає сам, навіть коли запит ініціює чужа сторінка. SameSite=Lax тут не
рятує: http://localhost:8082 і клієнт http://localhost:8081 – той самий «сайт» (порт не
враховується), тож cookie піде і з чужої форми. CORS теж не захист: він лише забороняє чужій
сторінці ПРОЧИТАТИ відповідь, а простий POST форми сервер однаково отримає.

Тому кожен змінювальний запит (POST, PUT, PATCH, DELETE) перевіряється ДО виконання дії:
  * є заголовок Origin – він має бути в списку CLIENT_ORIGIN, інакше 403;
  * Origin немає – дивимося Sec-Fetch-Site: «cross-site» чи «same-site» без Origin не
    пропускаємо (403); same-origin, none або відсутній заголовок (curl, сценарії) – пропускаємо.
Читання (GET, HEAD) і попередні запити OPTIONS не перевіряються: вони нічого не змінюють.
"""

from starlette.types import ASGIApp, Receive, Scope, Send

from . import config
from .jsonlog import log
from .problems import Problem

UNSAFE = {"POST", "PUT", "PATCH", "DELETE"}


def refused(origin: str | None, fetch_site: str | None) -> str | None:
    """Причина відмови або None, якщо запит дозволено."""
    if origin is not None:
        return None if origin in config.CLIENT_ORIGINS else f"origin {origin} is not allowed"
    if fetch_site in ("cross-site", "same-site"):
        return f"Sec-Fetch-Site: {fetch_site} without an allowed Origin"
    return None


class CsrfGuard:
    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] == "http" and scope["method"] in UNSAFE:
            headers = {k.decode().lower(): v.decode() for k, v in scope["headers"]}
            reason = refused(headers.get("origin"), headers.get("sec-fetch-site"))
            if reason:
                log("csrf.refused", method=scope["method"], path=scope["path"], reason=reason)
                response = Problem(403, "cross-site-request", "Cross-site request refused", reason).response(
                    scope["path"]
                )
                await response(scope, receive, send)
                return
        await self.app(scope, receive, send)
