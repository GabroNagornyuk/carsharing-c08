"""Спільне для сценаріїв перевірки й дослідів: адреси, заголовки, підключення до бази."""

import os

WEB = os.environ.get("WEB_URL", "http://localhost:8080")
DPC = os.environ.get("DPC_URL", "http://localhost:8000")
DB = os.environ.get("DATABASE_URL", "postgresql://app:app@localhost:5432/app")
DPC_AUTH = {"Authorization": f"Bearer {os.environ.get('API_TOKEN', 'dev-token')}"}


def user(user_id: str) -> dict[str, str]:
    return {"X-User-Id": user_id}


def percentile(values: list[float], q: float) -> float:
    ordered = sorted(values)
    if not ordered:
        return float("nan")
    k = max(0, min(len(ordered) - 1, round(q / 100 * len(ordered) + 0.5) - 1))  # nearest-rank
    return ordered[k]


# Web ЛР2: прямі адреси екземплярів (дослід «через обидва екземпляри» й повтор через інший)
WEB_A = os.environ.get("WEB_A_URL", "http://localhost:8001")
WEB_B = os.environ.get("WEB_B_URL", "http://localhost:8002")


def idem(key: str | None = None) -> dict[str, str]:
    """Заголовок Idempotency-Key: новий ключ на кожну НОВУ операцію, той самий – на її повтори."""
    import uuid

    return {"Idempotency-Key": key or f"k-{uuid.uuid4()}"}
