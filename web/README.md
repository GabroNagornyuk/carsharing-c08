# Частина Web

Каталог починається з комплекту курсу «Обмін даними у Web-застосунках»
([a-kovt/web-interactions-course](https://github.com/a-kovt/web-interactions-course), MIT).
Опис системи, запуск і результати – у [кореневому README](../README.md).

| Шлях | Що це |
|---|---|
| `server/app/rentals.py`, `cars.py`, `users.py` | наш код: головна операція, вільні авто, поточний користувач |
| `server/app/*` (інші модулі) | каркас комплекту: журнал, X-Request-ID, Problem Details, /health, пул БД |
| `openapi.yaml` | контракт частини Web |
| `client/` | вебклієнт |
| `configs/nginx/nginx.conf` | балансувальник (ЛР1 – лише app-a, app-b) |
| `configs/migrate/migrate.sh` | застосування SQL-міграцій з кореневого `migrations/` |
| `tools/`, `docs/`, `configs/*` | засоби комплекту для наступних ЛР |
