-- Спільна предметна модель: зони, користувачі, автомобілі, оренди (частина Web, ЛР1).
-- Гроші – цілі копійки + валюта; моменти часу – timestamptz; назва зони часу – окремим полем.

CREATE TABLE zone (
  id        text PRIMARY KEY,
  name      text NOT NULL,
  time_zone text NOT NULL DEFAULT 'Europe/Kyiv'
);

CREATE TABLE app_user (
  id   text PRIMARY KEY,
  name text NOT NULL,
  role text NOT NULL CHECK (role IN ('user', 'staff'))
);

CREATE TABLE car (
  id                text PRIMARY KEY,
  zone_id           text NOT NULL REFERENCES zone (id),
  plate             text NOT NULL UNIQUE,
  class             text NOT NULL CHECK (class IN ('economy', 'comfort', 'business')),
  charge_pct        integer NOT NULL CHECK (charge_pct BETWEEN 0 AND 100),
  status            text NOT NULL DEFAULT 'available' CHECK (status IN ('available', 'service')),
  price_min_minor   integer NOT NULL CHECK (price_min_minor >= 0),  -- копійки за хвилину
  price_km_minor    integer NOT NULL CHECK (price_km_minor >= 0),   -- копійки за кілометр
  currency          char(3) NOT NULL DEFAULT 'UAH',
  version           integer NOT NULL DEFAULT 1 CHECK (version > 0)
);

CREATE INDEX car_zone ON car (zone_id, id);

-- held → active → finished; held → expired | cancelled
CREATE TABLE rental (
  id            text PRIMARY KEY,                 -- спільний rental_id = command_id DPC
  user_id       text NOT NULL REFERENCES app_user (id),
  car_id        text NOT NULL REFERENCES car (id),
  status        text NOT NULL CHECK (status IN ('held', 'active', 'finished', 'expired', 'cancelled')),
  version       integer NOT NULL DEFAULT 1 CHECK (version > 0),
  created_at    timestamptz NOT NULL DEFAULT now(),
  expires_at    timestamptz NOT NULL,
  started_at    timestamptz,
  finished_at   timestamptz,
  km            integer CHECK (km >= 0),
  amount_minor  bigint CHECK (amount_minor >= 0),
  currency      char(3) NOT NULL DEFAULT 'UAH',
  CHECK (expires_at > created_at),
  CHECK (status <> 'active' OR started_at IS NOT NULL),
  CHECK (status <> 'finished' OR (finished_at IS NOT NULL AND km IS NOT NULL AND amount_minor IS NOT NULL))
);

-- Інваріант 1: автомобіль не більше ніж в одній незавершеній оренді (held або active).
CREATE UNIQUE INDEX rental_one_open_per_car ON rental (car_id) WHERE status IN ('held', 'active');
CREATE INDEX rental_user_recent ON rental (user_id, created_at DESC, id DESC);
