# Подготовка кластеров к B-сценариям downscale брокеров (2026-09-07)

Цель: привести test-modify3 / test-modify4 / test-downgrade7 к продовому состоянию и добавить
4-го брокера в новый ДЦ `ic` — подготовка к тестам `DownscaleKafkaBrokerInClusterWorkflow`
(MDBDEV-2900, ветка `ershov/MDBDEV-2900-downscale-kafka-brokers`). Сами B-тесты — отдельная сессия.

## 1. Re-seed трёх кластеров из прода (туннель localhost:53480)

- Кластеры: test-modify3 `9fc47c1b`, test-modify4 `3fb46c41`, test-downgrade7 `23f108ac`.
- Снапшот одним запросом (db_cluster, versions LIMIT 40, host_state, one_cloud_meta,
  operations LIMIT 60, projects, namespaces, hardware_presets, settings) →
  `/tmp/seed-2900/three-clusters.json`, seed-SQL → `/tmp/seed-2900/seed.sql`
  (delete по трём cluster_id + INSERT ON CONFLICT DO NOTHING; `fake_id` у one_cloud_meta дропнут).
- **Новая грабля: FK `users_cluster_id_foreign`** — локальные users блокируют DELETE db_cluster.
  Решение: выгрузить users кластеров из прода тоже (16 записей, `/tmp/seed-2900/users.json`)
  и delete+insert их в сид. Итог: modify3 = 29 версий / 7 хостов / 3 users / 28 ops;
  modify4 = 9 / 7 / 2 / 13; downgrade7 = 2 / 7 / 11 / 19.
- NPE-фикс draft-версий применён (пустые `kafkaParams.*Config.config`, UPDATE 29 строк).
- Последние операции всех кластеров после сидa — `done` (409-блока нет).

## 2. ABC-стаб квот :3000 (обязателен для upscale через mdb-data)

mdb-data local ходит за квотами в `http://localhost:3000/v1/quotas/one-cloud/product/7514`
(`AbcClientImpl`). Без стаба — 500 «Connection refused» до Temporal-запуска. Стаб:
`python3 /tmp/seed-2900/abc-stub.py` (nohup). Контракт ответа:
- список записей по ДЦ (dc/hc/kc/pc/rc/ic/uc) — `dc: null` даёт NPE `DcProductQuota.dc() is null`;
- resourceType ровно те, что запрашивает валидатор: **VCPU, RAM, NVME, SSD, HDD, LAN_IN, LAN_OUT**
  (CPU не нужен) с большими `quota` и `demand: 0`;
- для `/datacenter/{dc}/product/...` — одиночная запись с этим dc.
Грабли по шагам: connection refused → NPE → «Quota is insufficient 400» (нет NVME/LAN_*) → 202.

## 3. Upscale 4-го брокера в ic: контракт brokersPerDc — полная карта

Первая попытка `{"brokersPerDc":{"ic":1}}` → 202 → workflow FAILED
`NO_SOURCE_BROKER_SERVICE`: `UpscaleKafkaBrokerWorkflowImpl.resolveSourceDc`
(UpscaleKafkaBrokerWorkflowImpl.java:52-57, метод :121-130) ищет broker-сервис-источник
манифеста ТОЛЬКО среди целевых ДЦ запроса; в новом ДЦ сервиса нет.

Прод-скан (30 прогонов 01–07.09, `WorkflowType="upscaleKafkaBroker"`): COMPLETED всегда с
ПОЛНОЙ картой по всем ДЦ кластера — `{pc:1,kc:1,hc:1,ec:1}` (dzen-common, ec новый),
`{nc:2,zc:2,ic:2}`, `{nc:1,ic:1,dc:1,pc:1}`, `{rc:1,pc:7,kc:2,hc:6,dc:1,ec:7}`; FAILED — тоже
полные карты (PARTIAL_UPSCALE_FAILURE уже после source-фазы). Ни одного прогона с
одиночным новым ДЦ → контракт: существующие ДЦ = текущее число (no-op child), новый ДЦ =
цель 1; `resolveSourceDc` находит source среди существующих.

Рабочие вызовы (после закрытия failed-операций `UPDATE operations SET status='done'`):
- test-modify3: `{"brokersPerDc":{"pc":1,"kc":1,"hc":1,"ic":1}}`
- test-modify4: `{"brokersPerDc":{"rc":1,"pc":1,"kc":1,"ic":1}}`
- test-downgrade7: `{"brokersPerDc":{"pc":1,"kc":1,"hc":1,"ic":1}}`
Все → 202 → parent `upscaleKafkaBroker` COMPLETED (16:49) + `reconcileKafkaCluster` COMPLETED.

## 4. Итоговое состояние

| Кластер | Брокеры | Операция | Workflow |
|---|---|---|---|
| test-modify3 | pc:1, kc:1, ic:1, hc:1 (4) | done | upscaleKafkaBroker COMPLETED |
| test-modify4 | rc:1, pc:1, kc:1, ic:1 (4) | done | COMPLETED |
| test-downgrade7 | pc:1, kc:1, ic:1, hc:1 (4) | done | COMPLETED |

Файлы: `/tmp/seed-2900/{three-clusters.json,users.json,seed.sql,abc-stub.py}`.
Стаб ABC жив (PID в nohup, порт 3000) — при перезапуске ноутбука поднять заново.

## 5. Накат данных на новые ic-брокеры: reassign round-robin (20:18)

Цель — чтобы у ic-брокеров были партиции перед B-сценариями (иначе drain мгновенный).

- **Инвентаризация** (через mcc + `/opt/kafka/bin/kafka-topics.sh --bootstrap-server <FQDN>:9092`):
  - ⚠️ bootstrap только по FQDN, не `localhost` — иначе SSL `No subject alternative DNS name matching localhost`.
  - test-modify3: ids `21001(ic),22001,23001,26001`; юзерский топик `test` (3 part, RF=3).
  - test-modify4: ids `21001(ic),22001,23001,25001`; юзерских топиков НЕТ → создан `test`
    (3 part, RF=3) + пара сообщений через console-producer.
  - test-downgrade7: ids `20001,21001(ic),22001,23001`; топики `anton_test1, test, test2,
    test3, test333, test_anton222` (19 партиций суммарно).
- **Vault-секреты уже в локальном vault** (заливать не надо): mount `zkv/`, путь
  `zkv/mdb/mdbdev/kafka/<fullQueue>/super` — формат `VaultUtil.constructPathToSuper`
  (`zkv/mdb/<project>/<dbType>/<fullQueue>/super`), пароли совпадают с продом.
  ⚠️ История 2026-08-16 устарела: mount называется `zkv/`, не `mdb/`.
- **Reassign** (processing API напрямую — операция не трогает host_state, рассинхрона нет):
  `POST localhost:8080/api/v1/mdb/processing/kafka/clusters/{id}/partitions/reassign`
  ```json
  {"operationId":"<uuid>","queueInfo":{"queueName":"<q>.mdbdev.db.production.mdb.prod",
   "productId":"7514","queueShortName":"<q>","pmsHost":"<q>.clouds","namespace":"INFRA"},
   "connectionParams":{"kafkaBrokerHosts":"<4 FQDN>:9092 через запятую",
   "vaultPasswordPath":"zkv/mdb/mdbdev/kafka/<q>.mdbdev.db.production.mdb.prod/super"},
   "topics":["test",...],"targetBrokerIds":[<все 4 id>]}
  ```
  Все три → 202 → workflow COMPLETED (в логе: 3 / 3 / 19 партиций на reassign).
- **Верификация**: `--describe --topic test` — Replicas содержат 21001 (ic) в большинстве
  партиций (round-robin: 3 реплики из 4 брокеров), ISR полный, лидеры размазаны.
  downgrade7: по 2 партиции с 21001 в каждом из test/test2/test3.

Кластеры готовы к B1–B14. Перед прогонами: дождаться тишины по ISR, снять baseline.

## Что дальше (B-сценарии, отдельная сессия)

1. Baseline-снимки: host_state, PMS (`kafka.layout`, quorum), живость кластера.
2. Прогнать B1 → B4 → B5/B6 → B9/B10 → B2 → B3 → B8 → B7/B11 → B12 → B13
   (см. план в SKILL.md). Откат между сценариями — upscale, не руками.
3. После B1/B2/B3 проверять, что reassign-фаза увела партиции с удаляемых брокеров
   (ISR полный, `describeTopics` без удалённых id) до rescale/withdraw.


