# Частина DPC

Каталог починається з комплекту курсу «Розподілені та паралельні обчислення»
([a-kovt/distributed-parallel-computing-course](https://github.com/a-kovt/distributed-parallel-computing-course), MIT).
Опис платформи, запуск і результати – у [кореневому README](../README.md).

| Шлях | Що це |
|---|---|
| `api/app/rental_starts.py` | наш код: приймання команд, обмежена черга, пул обробників, ресурс зони; диспетчер задач Ray з `IN_FLIGHT_LIMIT` і `Idempotency-Key` (ЛР3) |
| `api/app/ray_tasks.py`, `effects.py` | наш код (ЛР3): задача Ray `start_rental` (виконавець) і приймач ефекту в одній транзакції |
| `api/app/compute.py`, `integral.py` | наш код (ЛР2): `POST /v1/compute`, пул процесів, поділ і об'єднання, методи й сумування, режими спільної суми |
| `api/app/*` (інші модулі) | каркас комплекту: токен, журнал, Problem Details, /health, /metrics |
| `openapi.yaml` | контракт сервісу `api` |
| `client/` | вебклієнт платформи |
| `spark/`, `coordinator/`, `tools/`, `docs/`, `configs/` | засоби комплекту для наступних ЛР |
