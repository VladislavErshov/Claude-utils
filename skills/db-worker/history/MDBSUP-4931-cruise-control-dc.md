# MDBSUP-4931 — проставить cruiseControlDc кластеру ecom-dynamic

Кластер: `afbd8f7c-578c-4f32-939e-6ff5ddf7457d` (ecom-dynamic, Kafka).
Проблема: cruise-хост есть (`1.cruise.ecom-dynamic-adtech-kafka.hc.one-infra.ru`,
host_state dc=hc), но UI показывал «Cruise-control DC: Выберите значение...» —
в актуальной версии `kafkaParams.cruiseControl` был `null`.

## Диагностика

- UI читает версию с максимальным `create_ts` — `findCurrentClusterVersionsByClusterId`
  (ClusterDao.ts:288, orderBy `cv.create_ts desc`).
- Все 7 версий кластера scheduled; у свежих (238623 от 2026-08-27, 233872 от 2026-08-25)
  `cruiseControl: null` явно, у старых ключа нет вовсе.
- Операций `create_additional_service` у кластера нет — круиз появился вне
  стандартного флоу, эталонных autoRebalance/throttle значений в БД не существует.
  Проставили минимальный объект `{"cruiseControlDc": "hc"}`.

## Выполненный UPDATE (прод, port-forward 53480, docker cp + psql -f)

```sql
BEGIN;
UPDATE db_cluster_version
SET cluster_params = jsonb_set(cluster_params, '{kafkaParams,cruiseControl}', '{"cruiseControlDc": "hc"}'::jsonb)
WHERE id IN (238623, 233872);
COMMIT;
-- UPDATE 2, верифицировано SELECT-ом: обе строки {"cruiseControlDc": "hc"}
```

Старые версии (221466 и раньше) не трогали — по прецеденту MDBSUP-4877.

## Нюанс

После правки DC становится immutable (DbParamsValidator.ts:182 проверяет
oldVersion...cruiseControlDc). Если на хосте реально включён auto-rebalance,
при следующем modify-флоу сгенерится PMS-конфиг с дефолтами (false) —
при необходимости дозаполнить autoRebalanceEnabled/replicationThrottleMb
из реального cruisecontrol.properties.
