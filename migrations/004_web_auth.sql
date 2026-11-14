-- Вхід і доступ Web (ЛР3 Web, О3.1, Д3.3).
-- Пароль зберігається лише як хеш scrypt із сіллю. Сесія – рядок у базі: її бачать обидва
-- екземпляри, а вихід знищує рядок. У cookie лежить випадковий токен, у базі – лише його sha256,
-- тож витік таблиці не дає готових cookie.

ALTER TABLE app_user
  ADD COLUMN login         text UNIQUE CHECK (login ~ '^[a-z0-9._-]{3,64}$'),
  ADD COLUMN password_hash text,
  ADD CHECK ((login IS NULL) = (password_hash IS NULL));

CREATE TABLE session (
  token_hash  text PRIMARY KEY,                       -- sha256(токен з cookie), hex
  user_id     text NOT NULL REFERENCES app_user (id) ON DELETE CASCADE,
  created_at  timestamptz NOT NULL DEFAULT now(),
  expires_at  timestamptz NOT NULL,
  CHECK (expires_at > created_at)
);

CREATE INDEX session_expires ON session (expires_at);

-- Д3.3: невдалі спроби входу за логіном; рахуються лише ті, що в межах вікна
CREATE TABLE login_failure (
  login      text NOT NULL,
  failed_at  timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX login_failure_login ON login_failure (login, failed_at);
