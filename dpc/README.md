# Частина DPC

Каталог починається з комплекту курсу «Розподілені та паралельні обчислення»
([a-kovt/distributed-parallel-computing-course](https://github.com/a-kovt/distributed-parallel-computing-course), MIT).
Опис платформи, запуск і результати – у [кореневому README](../README.md).

| Шлях | Що це |
|---|---|
| `api/app/rental_starts.py` | наш код: приймання команд, обмежена черга, пул обробників, ресурс зони |
| `api/app/*` (інші модулі) | каркас комплекту: токен, журнал, Problem Details, /health, /metrics |
| `openapi.yaml` | контракт сервісу `api` |
| `client/` | вебклієнт платформи |
| `spark/`, `coordinator/`, `tools/`, `docs/`, `configs/` | засоби комплекту для наступних ЛР |
