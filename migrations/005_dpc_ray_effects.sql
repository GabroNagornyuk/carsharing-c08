-- Частина DPC (ЛР3): приймання результатів задач Ray і ключі ідемпотентності API.
--
-- О3.3: рішення приймача за ключем ефекту command_id. Первинний ключ не дає записати друге
-- рішення для тієї самої команди: повторна доставка (повтор задачі Ray після відмови виконавця)
-- отримує збережене рішення й нічого не змінює. Рішення, зміна zone_cars і рядок
-- rental_start_effect записуються в одній транзакції.
CREATE TABLE rental_start_decision (
  command_id   text PRIMARY KEY REFERENCES rental_start (id),
  decision     text NOT NULL CHECK (decision IN ('done', 'rejected')),
  reason       text,
  executor     text NOT NULL,                       -- хто доставив уперше: вузол і pid виконавця
  deliveries   integer NOT NULL DEFAULT 1 CHECK (deliveries >= 1),  -- скільки разів доставляли
  decided_at   timestamptz NOT NULL DEFAULT now(),
  acked_at     timestamptz,                         -- задача Ray завершилась і api отримав її результат
  acked_by     text                                 -- виконавець спроби, що завершилась
);

-- Д3.3: ключ ідемпотентності POST /v1/rental-starts. Ключ -> відбиток тіла й прийнята команда.
CREATE TABLE dpc_idempotency_key (
  key         text PRIMARY KEY CHECK (length(key) BETWEEN 8 AND 128),
  body_hash   text NOT NULL,                         -- sha256 канонічного JSON тіла
  command_id  text NOT NULL REFERENCES rental_start (id) DEFERRABLE INITIALLY DEFERRED,  -- ключ пишеться першим
  created_at  timestamptz NOT NULL DEFAULT now()
);
