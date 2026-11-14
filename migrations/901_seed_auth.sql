-- Облікові записи демоданих для входу (ЛР3 Web): логін / пароль –
--   driver1 / driver1-pass, driver2 / driver2-pass (водії), operator / operator-pass (персонал).
-- Хеші – scrypt (N = 16384, r = 8, p = 1), сіль 16 байт; формат scrypt$N$r$p$сіль$хеш.

UPDATE app_user SET login = 'driver1',
  password_hash = 'scrypt$16384$8$1$d7caa6b774ce8ad6651291203b8f4f1a$721d956a4081f9153534ef24ebce0f49719a410818c7e448a0a5e86b2eab3438'
WHERE id = 'u-driver-1';
UPDATE app_user SET login = 'driver2',
  password_hash = 'scrypt$16384$8$1$2bce3a30ab6d9992f11a7ccde7858691$8b792796cb2c68490338ddd8fcfede7e5b049eabe6f7d5580e806d4020b757cc'
WHERE id = 'u-driver-2';
UPDATE app_user SET login = 'operator',
  password_hash = 'scrypt$16384$8$1$040bd8b47ebe2ab3a3aab2848e6a8766$c8647bed52c32d98fd1bb6b424638ef34e7e0286255284277fef98348a765193'
WHERE id = 'u-operator';
