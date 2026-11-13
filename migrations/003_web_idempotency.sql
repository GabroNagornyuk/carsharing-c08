-- Ключі ідемпотентності змінювальних операцій Web (ЛР2 Web, О2.2).
-- Рядок записується в ТІЙ САМІЙ транзакції, що й предметна зміна: або є і оренда, і збережена
-- відповідь, або немає нічого. Область ключа – (користувач, операція, ключ).

CREATE TABLE idempotency_key (
  user_id      text NOT NULL REFERENCES app_user (id),
  operation    text NOT NULL,                       -- напр. 'POST /v1/rentals'
  key          text NOT NULL CHECK (length(key) BETWEEN 8 AND 128),
  request_hash text NOT NULL,                       -- sha256 канонічного JSON тіла
  state        text NOT NULL CHECK (state IN ('processing', 'completed')),
  status_code  integer CHECK (status_code BETWEEN 100 AND 599),
  response     jsonb,                               -- тіло відповіді
  location     text,                                -- заголовок Location відповіді 201
  created_at   timestamptz NOT NULL DEFAULT now(),
  expires_at   timestamptz NOT NULL,
  PRIMARY KEY (user_id, operation, key),
  CHECK (state <> 'completed' OR (status_code IS NOT NULL AND response IS NOT NULL))
);

-- прибирання прострочених ключів порціями
CREATE INDEX idempotency_key_expires ON idempotency_key (expires_at);
