# T2 Идемпотентность повторного запуска + фикс mdb-data base-url (2026-08-24)

**Результат: PASS**

## Фикс инфраструктуры (найден между T1 и T2)

`application-local.yaml:61` mdb-processing: internal-API mdb-data указывал на **wiremock** (`http://localhost:8088`)
вместо реального mdb-data. Поэтому в T1 activity `saveUpscaledKafkaControllersInfo` уходил в заглушку,
а `host_state` не пополнялся. Фикс: `mdb-data.base-url: http://localhost:8081` + рестарт mdb-processing.
⚠️ Это же, вероятно, касается и других internal-API (clickhouse/postgresql/newsql/redis) в local-прогонах —
проверять, что заглушки покрывают нужные вызовы, либо направлять в реальный mdb-data.

## Запуск T2

Тот же запрос: `POST /api/v2/mdb/kafka/clusters/9fc47c1b…/hosts/controllers?dc=dc` → 202.
Новая операция/workflow: `f4fa571e-a466-4e8d-8761-5551baf7ad91` (mdb-data строит controllersPerDc
из host_state — но host_state ещё НЕ содержал 2.controller (см. фикс) → снова `{dc:2, hc:1, kc:1}`).

## Результаты

- Все 3 child `upscaleKafkaControllerInDc` завершились мгновенно (dc: только discovery-activity
  `submitQueueIfNeeded`/`isServiceExists`/`getServiceInfo` → rescale не выполнялся — сервис уже 2).
- Parent COMPLETED: upsertPms идемпотентны, reload идемпотентен, save отработал.
- **host_state пополнился** `2.controller…dc` (теперь фикс base-url работает) — 4 контроллера.
- **PMS quorum не изменился**: те же 4 voters, без дублей (union-merge идемпотентен).
- operations: вторая операция add_hosts, done — по одной на каждый API-вызов, дублей нет.

## Вывод

Повторный запуск при достигнутой цели — безобиден: скип деплоя, идемпотентные PMS/reload/save.
Для чистого T2 (повтор с тем же operationId → тот же workflowId) нужно ретраить операцию,
а не слать новый запрос — покрыто T3-серияей через terminate+restart.

Примечание: из-за бага с wiremock в T1 `host_state` опоздал на один цикл — после T2 состояние
консистентно (PMS = host_state = one-cloud = 4 контроллера).

## Доп. попытка: downscale dc (для отката к цели T3) — FAILED по инфра-причине

`DELETE …/hosts/controllers?dc=dc` → workflow `23750047-70c7-4737-924d-57cfd9e0b90e` FAILED
на `migrateLeader` (DownscaleKafkaControllerInClusterWorkflowImpl.java:154): KafkaAdminClient
не подключается к брокерам `:9092` с локальной машины (SASL_SSL/SASL_PLAINTEXT, нет сетевого
доступа). Не баг кода — локальное ограничение. Обход: `mcc tp-port-forward` на 9092 брокеров
(не проверено), либо откатывать цель выбором другого ДЦ для T3.
⚠️ Итог: dc остался с 2 контроллерами, операция в mdb-data — failed. Для откатов между
сценариями downscale локально требует туннеля к 9092.
