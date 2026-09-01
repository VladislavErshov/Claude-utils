# MDBDEV-3245 — ReconcileKafkaClusterWorkflow в modifyKafka (2026-08-28)

Проверка нового child-флоу `ReconcileKafkaClusterWorkflow` (актуализация `kafka.isWanCluster`
на кластерном ключе `<queue>.clouds` до деплоя хостов) на операции modifyKafka.

## Инфраструктура

- mdb-processing:8080 и mdb-data:8081 перезапущены (старые процессы были до коммитов
  `99cd3973`/`827df235`).
- **Грабля**: mdb-data не компилировался — его код ждёт `isWan` в DTO из `processing-api:3.52.0`,
  а в `~/.m2` лежала старая банка. Лечение:
  ```bash
  CI_COMMIT_TAG=v3.52.0 ./gradlew :api:publishToMavenLocal   # в mdb-processing
  ```
  (env `CI_COMMIT_TAG` форсит версию публикации, см. api/build.gradle:32).
- Проверка регистрации: `grep reconcile /tmp/mdb-processing.log` →
  `Registering ... ReconcileKafkaClusterWorkflowImpl on worker 'kafka-activities-worker'`.

## Кластеры (project 160 / mdbdev, dev — PMS-записи легальны)

| cluster_id | name | примечание |
|---|---|---|
| 23f108ac-1907-434e-a67b-dda01df316f4 | test-downgrade7 | сценарий 1 |
| 69204f9d-723c-4ad7-848c-efcd1b2389bd | test-downgrade6 | сценарий 2 |

Baseline-патч обязателен (иначе NPE в KafkaClusterDiffDetector):
```sql
UPDATE db_cluster_version
SET cluster_params = jsonb_set(
      jsonb_set(cluster_params, '{kafkaParams,brokerConfig}', '{"config": {}}'),
      '{kafkaParams,controller,controllerConfig}', '{"config": {}}')
WHERE cluster_id IN (...) AND status = 'scheduled';
```

## Сценарий 1 — идемпотентный upsert (isWan=false, тоггл autoRebalance)

Request: `/tmp/modify-s1.json` (params.isWan=false как в baseline; валидатор требует
`cruiseControl.replicationThrottleMb` при `autoRebalanceEnabled=true`, подошло 5).

Результат:
- 202; parent `modifyKafkaCluster` = `ad5b3a0a-e0bf-4902-ac24-65ad7dd9d1fe`.
- Child `reconcileKafkaCluster` (`<parent>_reconcile-cluster`) COMPLETED 14:59:12.343 —
  **раньше** старта modify-controller (14:59:12.869): барьер «reconcile до хостовых шагов» работает.
- Input child: `{namespace:"infra", pmsHost:"test-downgrade7-mdbdev-kafka.clouds", isWan:false}`.
- Activity `upsertIsWanCluster` (pms-activities-queue), вход: `infra`, `<queue>.clouds`, `false`.
- PMS до/после: `kafka.isWanCluster=false` — идемпотентно.
- Родитель дальше штатно прошёл updateConfigKafkaController → resizeKafkaController (per-instance).

## Сценарий 2 — флип isWan false→true

Request: `/tmp/modify-s2.json` (params.isWan=true, остальное зеркально baseline).

Результат:
- 202; parent `7675bbe3-c0a5-4672-9f6b-4e1eb798a47c`.
- Reconcile-first: completed 15:10:19.604 < modify children 15:10:20.08.
- PMS `test-downgrade6-mdbdev-kafka.clouds`: `kafka.isWanCluster` false → **true**.
- Per-DC ключи `test-downgrade6-mdbdev-kafka.{hc,kc}`: NOT_SET — не затронуты (пишем только в кластерный ключ).

## Сверка с Backstage (создание кластера)

Backstage при создании: `GenerateKafkaPmsSettingsTaskProcessor.generateIsWanCluster`
(plugins/mdb-backend/src/task/processor/kafka/GenerateKafkaPmsSettingsTaskProcessor.ts:123)
→ `PmsClient.createOrUpdateVariable` (plugins/common/src/onecloud/pms/PmsClient.ts:54).

| Аспект | Backstage creation | ReconcileKafkaClusterWorkflow | OK |
|---|---|---|---|
| PMS-ключ | `${metaParams.queue}.clouds` (one_cloud_meta) | `<queue>.clouds` (pmsHostName из modify) | ✅ |
| property | `kafka.isWanCluster` | `kafka.isWanCluster` (IS_WAN_CLUSTER) | ✅ |
| значение | `String(isWan)` → "true"/"false" | `String.valueOf(isWan)` | ✅ |
| application | `appName ?? "mdb"` | `null` → DEFAULT `"mdb"` (PmsServiceImpl) | ✅ |
| namespace | namespace кластера (infra) | `Namespace` из запроса (infra) | ✅ |
| API | `conf/update.do` upsert | `pmsApi.createOrUpdateVariable` (тот же upsert) | ✅ |
| семантика | пишется один раз при создании (isWan: boolean, non-null) | идемпотентный upsert перед каждым scale/modify/cruise; `isWan=null` → шаг пропускается | ✅ by design |

Вывод: reconcile пишет ровно ту же переменную (тот же ключ/property/application/формат значения),
что и создание — расхождений нет. Различие только в семантике запуска: creation — один раз,
reconcile — идемпотентное «залечивание» перед деплоями; null-флаг корректно пропускает шаг.

## Происхождение параметров modify-реквеста в mdb-data

`KafkaClusterModificationServiceImpl.buildProcessingDto` + `KafkaClusterModificationMapper.toModifyKafkaClusterDto`:

| Поле workflow input | Источник в mdb-data |
|---|---|
| `namespace` | таблица `namespaces` (по cluster_id) |
| `queue`, `fullQueue` | `one_cloud_meta` (`params_type='db-service'`) |
| `pmsHostName` | `queue + ".clouds"` — калькуляция из one_cloud_meta |
| `isWan` | **modify-запрос** `params.isWan` (diff-детектор не сравнивает, валидатор не проверяет) |
| broker DCs | `host_state` (getKafkaBrokers) |
| broker resources | `hardware_presets` (по request.hardwarePresetId) + params |
| controller resources/params/heap | params запроса |
| orders | `KafkaClusterDiffDetector` vs baseline `db_cluster_version` (heap ↑ → RESIZE_THEN_UPDATE) |
| `workflowTtl` | dto.workflowTtl → processing дефолт 4h (`ModifyKafkaClusterMapper`) |

⚠️ **Два источника isWan — это by design**: modify берёт `isWan` из запроса (draft `cluster_version`
обновляется, `one_cloud_meta` — нет), scale-флоу (`KafkaHostsServiceImpl:224`) и Backstage
creation берут из `one_cloud_meta.params.isWan`. `one_cloud_meta` обновляется **только при
создании кластера** — это creation-time константа (queue/fullQueue/isWan фиксируются раз и навсегда),
`db_cluster_version.cluster_params` — текущее состояние. На S2: meta=false (как при создании),
version=true (из modify), PMS=true — ожидаемое поведение.

## Наблюдения

- Оба родителя после reconcile долго бегут в per-instance resize/update-config (реальный
  one-cloud: `cloud_getInfoForInstances`, рестарты) — это штатный хвост modify, не связано с reconcile.
- PMS-снапшоты до/после — `kafka-config-inspector/bin/pms-read.sh <queue>.clouds kafka.isWanCluster`.
