"""ЛР3 DPC, О3.1: задача Ray «старт оренди» – виконавець.

Виконавець робить повільну частину (перевірка посвідчення водія, 300–900 мс) і доставляє
результат ПРИЙМАЧУ – маршруту api POST /v1/rental-start-effects. Ресурс zone_cars змінює лише
приймач, виконавець бази даних не торкається.

Ключ ефекту – command_id: він приходить у задачу аргументом, тому однаковий у всіх спробах.
Якщо процес виконавця аварійно завершується (системна відмова), Ray повторює задачу
(max_retries) з тими самими аргументами, і повторна доставка приходить з тим самим ключем.

О3.4: керована відмова after_effect_before_ack стоїть після того, як приймач записав ефект і
відповів, але до повернення результату з задачі – Ray ще не отримав підтвердження завершення.
"""

import os
import random
import socket
import time

import httpx
import ray

from . import config
from .faults import fault


def _license_seconds(range_ms: str) -> float:
    low, _, high = range_ms.partition("-")
    return random.uniform(int(low), int(high or low)) / 1000


@ray.remote(
    num_cpus=config.RAY_TASK_CPUS,
    max_retries=config.RAY_MAX_RETRIES,  # повтор після системної відмови: процес виконавця загинув
    retry_exceptions=[httpx.TransportError],  # приймач тимчасово недоступний – теж повторюємо
)
def start_rental(command_id: str, driver_id: str, license_ms: str) -> dict:
    # повільний виклик картки; навчальне правило – посвідчення водіїв з id на 'nolicense' не підтверджується.
    # Тривалість (LICENSE_CHECK_MS) передає api, щоб налаштування досліду було в одному місці.
    time.sleep(_license_seconds(license_ms))
    license_ok = not driver_id.startswith("nolicense")

    executor = f"{config.INSTANCE}@{socket.gethostname()}:{os.getpid()}"
    reply = httpx.post(
        f"{config.RECEIVER_URL}/v1/rental-start-effects",
        json={"command_id": command_id, "license_ok": license_ok, "executor": executor},
        headers={"Authorization": f"Bearer {config.API_TOKEN}"},
        timeout=10,
    )
    reply.raise_for_status()
    decision = reply.json()

    fault("after_effect_before_ack")  # ефект уже прийнято, а задача ще не завершилась
    return decision | {"executor": executor}
