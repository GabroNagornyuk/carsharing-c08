-- Частина DPC (ЛР1): ресурс зони, запити старту оренди в черзі, ефекти старту.
-- Інваріант ресурсу: rented = кількість ефектів зони <= cars_total. CHECK (rented <= cars_total)
-- навмисно немає: дослід О1.4 без синхронізації має показати порушення на тестовій зоні.

CREATE TABLE zone_cars (
  zone_id    text PRIMARY KEY REFERENCES zone (id),
  cars_total integer NOT NULL CHECK (cars_total >= 0),
  rented     integer NOT NULL DEFAULT 0 CHECK (rented >= 0)
);

CREATE TABLE rental_start (
  id           text PRIMARY KEY,                  -- command_id = rental_id
  car_id       text NOT NULL,
  driver_id    text NOT NULL,
  zone_id      text NOT NULL REFERENCES zone_cars (zone_id),
  state        text NOT NULL CHECK (state IN ('queued', 'processing', 'done', 'rejected', 'failed')),
  reason       text,
  arrived_at   timestamptz NOT NULL DEFAULT now(),
  started_at   timestamptz,
  finished_at  timestamptz,
  CHECK (state NOT IN ('done', 'rejected', 'failed') OR finished_at IS NOT NULL)
);

CREATE INDEX rental_start_state ON rental_start (state);

-- Предметний ефект «старт оренди»: один рядок на command_id.
CREATE TABLE rental_start_effect (
  command_id  text PRIMARY KEY REFERENCES rental_start (id),
  zone_id     text NOT NULL REFERENCES zone_cars (zone_id),
  car_id      text NOT NULL,
  applied_at  timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX rental_start_effect_zone ON rental_start_effect (zone_id);
