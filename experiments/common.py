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
