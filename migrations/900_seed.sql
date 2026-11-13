-- Демодані (навчальні припущення). Ідентифікатори збігаються з прикладами openapi.yaml.

INSERT INTO zone (id, name) VALUES
  ('airport', 'Аеропорт Бориспіль'),
  ('center',  'Центр'),
  ('podil',   'Поділ'),
  ('loadtest', 'Зона навантажувальної перевірки DPC'),
  ('test-unprotected', 'Тестова зона досліду О1.4 DPC');

INSERT INTO app_user (id, name, role) VALUES
  ('u-driver-1', 'Олена Коваль',   'user'),
  ('u-driver-2', 'Максим Бондар',  'user'),
  ('u-operator', 'Оператор Ірина', 'staff');

-- Автомобілі: по 10 у зоні аеропорту, 6 у центрі, 4 на Подолі.
INSERT INTO car (id, zone_id, plate, class, charge_pct, price_min_minor, price_km_minor)
SELECT format('car-%s-%s', z.short, lpad(n::text, 3, '0')),
       z.id,
       format('AA%s%s', lpad((z.ord * 100 + n)::text, 4, '0'), z.suffix),
       (ARRAY['economy', 'comfort', 'business'])[1 + n % 3],
       40 + (n * 7) % 60,
       (ARRAY[450, 650, 990])[1 + n % 3],
       (ARRAY[300, 400, 600])[1 + n % 3]
FROM (VALUES ('airport', 'air', 1, 'KB', 10), ('center', 'cen', 2, 'KC', 6), ('podil', 'pod', 3, 'KP', 4))
       AS z(id, short, ord, suffix, cnt),
     generate_series(1, z.cnt) AS n;

-- Автомобіль на обслуговуванні (інваріант 2) і «останнє авто» для досліду гонки.
UPDATE car SET status = 'service' WHERE id = 'car-pod-004';

-- Ресурс DPC: кількість авто зони збігається з парком Web; окремі зони для перевірок DPC.
INSERT INTO zone_cars (zone_id, cars_total)
SELECT zone_id, count(*) FROM car GROUP BY zone_id;
INSERT INTO zone_cars (zone_id, cars_total) VALUES
  ('loadtest', 100000),
  ('test-unprotected', 5);
