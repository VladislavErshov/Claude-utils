# MDBDEV-1910: Testing modifyKafkaCluster через Backstage → mdb-data → Temporal

Полный e2e-тест: Backstage backend получает POST на создание новой версии кластера (`update_instances`), запускает task chain, который через `ModifyKafkaClusterTaskProcessor` дёргает `mdb-data`, а тот стартует `modifyKafkaCluster` workflow в Temporal с дочерними update/resize.

## Кластер

- **cluster_id**: `9e0336c7-50da-4487-8746-d332357180d3`
- **name**: `test-update-resize1`
- **project**: `mdbdev` (id=160), namespace `infra` (id=2)
- Реальные broker/controller хосты в one-cloud (`*.dc.one-infra.ru`, `*.kc...`, `*.zc...`)

## Изменения в коде (MDBDEV-1910)

Backstage перешёл от множества операторных тасок (StartKafkaBrokerResize, StartKafkaControllerResize, StartKafkaBrokerConfigUpdate, StartCruiseControlUpdateConfig) к одной задаче `MODIFY_KAFKA_CLUSTER`, которая через HTTP отдаёт всю работу в `mdb-data` (а тот уже делегирует mdb-processing'у через KafkaClusterProcessingApi).

| Файл | Изменение |
|------|-----------|
| `plugins/common/src/http/HttpRestClient.ts` | + `doPatchRequest` |
| `plugins/common/src/mdb/data/client/BaseMdbDataClient.ts` | + `doPatch` |
| `plugins/common/src/mdb/data/client/MdbDataClient.ts` | `modifyKafkaCluster(clusterId, request)` → `PATCH /api/v2/mdb/kafka/clusters/{id}/modify` |
| `plugins/common/src/mdb/data/manager/MdbDataManager.ts` | `modifyKafkaCluster(clusterId, request)` |
| `plugins/mdb-backend/src/task/processor/kafka/ModifyKafkaClusterTaskProcessor.ts` | body = `{params: version.clusterParams, hardwarePresetId: version.hardwarePreset.id, hosts: version.hosts, attempts: version.attempts}` |
| `plugins/mdb-backend/src/task/generator/kafka/modify/UpdateKafkaInstancesTaskGenerator.ts` | Убран override `generate` — базовый `UpdateInstancesTaskGenerator` уже корректно обрабатывает пустой массив операторов |
| `plugins/mdb-backend/src/MdbDependenciesBuilder.ts` | `mdbDataService` поднят выше блока `if (allowMdb)` и пробрасывается в `buildVersionTaskProcessors` |

Контракт mdb-data (`api/.../kafka/dto/cluster/ModifyKafkaClusterRequest.java`):
```
record ModifyKafkaClusterRequest(
  @NotNull ModifyKafkaClusterParams params,
  @NotNull Integer hardwarePresetId,
  @Nullable List<ClusterHostDto> hosts,
  @NotNull Integer attempts
)
```
`ModifyKafkaClusterParams` — подмножество `clusterParams` (acl/name/isWan/lan/disk/.../kafkaParams); Jackson игнорирует лишние поля, так что слать весь `version.clusterParams` безопасно.

## Запуск инфраструктуры и сервисов

Все три команды обязательны:

1. `/setup-local-backstage` — postgres :6432, redis :6379, sentinel :26379, clickhouse + `pg_boss` БД + Backstage backend :7007 (`yarn mdb-dev`)
2. `/setup-local-mdb-data` — postgres :6434 + mdb-data приложение :8081
3. `/setup-local-temporal` — temporal :7233 + vault + kafka + wiremock + mdb-processing :8080, переключение `app-config.mdb.local.yaml → mdb-processing.host: 'http://127.0.0.1:8080'`

Конфиг `app-config.mdb.local.yaml` после `/setup-local-temporal`:
```yaml
mdb:
  mdb-processing: { host: 'http://127.0.0.1:8080' }   # IPv4, иначе Node.js на macOS ECONNREFUSED
  mdb-data:       { host: 'http://localhost:8081' }
```

## Seed (применить в **обе** БД)

После старта Backstage (чтобы Flyway создал схемы), но до повторного использования — потому что Redis-кеш проектов и PMS-конфиг строятся при boot. После seed **необходим рестарт Backstage backend**.

```sql
-- namespaces (id=2 infra)
INSERT INTO namespaces (id, name, config, active, params)
VALUES (2, 'infra', '{"domain": "one-infra"}'::jsonb, true, '{"projects": ["all"]}'::jsonb)
ON CONFLICT (id) DO UPDATE SET name='infra', config=EXCLUDED.config, active=true;

-- projects (id=160 mdbdev)
INSERT INTO projects (id, name, product_id, idm_synced, queue_synced)
VALUES (160, 'mdbdev', 7514, true, true)
ON CONFLICT (id) DO UPDATE SET name='mdbdev', product_id=7514, idm_synced=true, queue_synced=true;

-- hardware_presets (database_preset.mongodbPreset ОБЯЗАТЕЛЕН даже для kafka — DbParamsValidator упадёт)
INSERT INTO hardware_presets (id, name, type, vcores_count, ram_gb, database_preset, is_active)
VALUES (100, 'baseline', 'standard', 1, 4,
  '{"mongodbPreset":{"defaults":{"intervalCommitMs":10,"wtEngineCacheSizeGb":16,"maxIncomingConnections":200},"maxValues":{"wtEngineCacheSizeGb":20,"maxIncomingConnections":400},"minValues":{"intervalCommitMs":1,"wtEngineCacheSizeGb":1,"maxIncomingConnections":10}}}'::jsonb,
  true)
ON CONFLICT (id) DO UPDATE SET database_preset=EXCLUDED.database_preset, is_active=true;

INSERT INTO hardware_presets (id, name, type, vcores_count, ram_gb, database_preset, is_active)
VALUES (169, 'm.pico', 'memory_optimized', 2, 2,
  '{"mongodbPreset":{"defaults":{"intervalCommitMs":10,"wtEngineCacheSizeGb":16,"maxIncomingConnections":200},"maxValues":{"wtEngineCacheSizeGb":20,"maxIncomingConnections":400},"minValues":{"intervalCommitMs":1,"wtEngineCacheSizeGb":1,"maxIncomingConnections":10}}}'::jsonb,
  true)
ON CONFLICT (id) DO UPDATE SET database_preset=EXCLUDED.database_preset, is_active=true;

-- db_cluster
INSERT INTO db_cluster (id, type, create_ts, update_ts, create_by, name, project_id, deleted, sharded, namespace_id, environment, criticality)
VALUES ('9e0336c7-50da-4487-8746-d332357180d3','kafka'::db_type,now(),now(),'vl.ershov','test-update-resize1',160,false,false,2,'production','D')
ON CONFLICT (id) DO UPDATE SET type='kafka', name='test-update-resize1', project_id=160, namespace_id=2;

-- one_cloud_meta (3 записи: db-service, kafka-controller-service, cruise-control-service; queue=test-update-resize1-mdbdev-kafka)
INSERT INTO one_cloud_meta (cluster_id, params, params_type) VALUES
  ('9e0336c7-50da-4487-8746-d332357180d3','{"isWan":false,"queue":"test-update-resize1-mdbdev-kafka","domain":"one-infra","fullQueue":"test-update-resize1-mdbdev-kafka.mdbdev.db.production.mdb.prod","rootQueue":"prod","serviceName":"broker"}'::jsonb,'db-service'),
  ('9e0336c7-50da-4487-8746-d332357180d3','{"isWan":false,"queue":"test-update-resize1-mdbdev-kafka","domain":"one-infra","fullQueue":"test-update-resize1-mdbdev-kafka.mdbdev.db.production.mdb.prod","rootQueue":"prod","serviceName":"controller"}'::jsonb,'kafka-controller-service'),
  ('9e0336c7-50da-4487-8746-d332357180d3','{"isWan":false,"queue":"test-update-resize1-mdbdev-kafka","domain":"one-infra","fullQueue":"test-update-resize1-mdbdev-kafka.mdbdev.db.production.mdb.prod","rootQueue":"prod","serviceName":"cruise"}'::jsonb,'cruise-control-service')
ON CONFLICT (cluster_id, params_type) DO UPDATE SET params=EXCLUDED.params;

-- host_state (3 broker × dc/kc/zc + 1 cruise + 3 controller × dc/kc/zc)
INSERT INTO host_state (id, cluster_id, host, update_ts, params, shard_id) VALUES
  (91455, '9e0336c7-50da-4487-8746-d332357180d3', '1.broker.test-update-resize1-mdbdev-kafka.dc.one-infra.ru', now(), '{"dc": "dc"}'::jsonb, NULL),
  (91456, '9e0336c7-50da-4487-8746-d332357180d3', '1.broker.test-update-resize1-mdbdev-kafka.kc.one-infra.ru', now(), '{"dc": "kc"}'::jsonb, NULL),
  (91457, '9e0336c7-50da-4487-8746-d332357180d3', '1.broker.test-update-resize1-mdbdev-kafka.zc.one-infra.ru', now(), '{"dc": "zc"}'::jsonb, NULL),
  (91458, '9e0336c7-50da-4487-8746-d332357180d3', '1.cruise.test-update-resize1-mdbdev-kafka.dc.one-infra.ru', now(), '{"dc": "dc"}'::jsonb, NULL),
  (91459, '9e0336c7-50da-4487-8746-d332357180d3', '1.controller.test-update-resize1-mdbdev-kafka.dc.one-infra.ru', now(), '{"dc": "dc"}'::jsonb, NULL),
  (91460, '9e0336c7-50da-4487-8746-d332357180d3', '1.controller.test-update-resize1-mdbdev-kafka.kc.one-infra.ru', now(), '{"dc": "kc"}'::jsonb, NULL),
  (91461, '9e0336c7-50da-4487-8746-d332357180d3', '1.controller.test-update-resize1-mdbdev-kafka.zc.one-infra.ru', now(), '{"dc": "zc"}'::jsonb, NULL)
ON CONFLICT (id) DO UPDATE SET cluster_id=EXCLUDED.cluster_id, host=EXCLUDED.host, params=EXCLUDED.params;

-- db_cluster_version baseline (status='done' — НЕ удалять при сбросе, иначе следующий POST падёт «нет базовой версии»)
-- brokerConfig/controllerConfig = {"config":{}} обязательно — KafkaClusterDiffDetector NPE-сафе
INSERT INTO db_cluster_version (id, cluster_id, cluster_params, create_ts, create_by, status, type, update_ts, attempts, hosts_params, hardware_preset_id, db_version)
VALUES (
  135287, '9e0336c7-50da-4487-8746-d332357180d3',
  '{"acl":{},"name":"test-update-resize1","isWan":false,"lanIn":10,"diskGb":8,"lanOut":20,"diskType":"nvme","projectId":160,"rootQueue":"prod","kafkaParams":{"controller":{"controllerDcs":["dc","kc","zc"],"controllerLanIn":15,"controllerMemGb":4,"controllerDiskGb":10,"controllerLanOut":50,"controllerVcores":4,"controllerDiskType":"nvme","controllerJvmHeapSizeMb":"2048","controllerConfig":{"config":{}}},"brokerConfig":{"config":{}},"cruiseControl":{"cruiseControlDc":"dc","cruiseUserPassword":""},"jvmHeapSizeMb":1024},"needLanIpv6":true,"needWanIpv4":false,"needWanIpv6":false}'::jsonb,
  now(), 'vl.ershov', 'done'::version_status, 'new'::version_type, now(), 3,
  '{"units":[{"dc":"dc"},{"dc":"kc"},{"dc":"zc"}]}'::jsonb,
  100,
  '{"id":9,"dockers":[{"dockerTag":"2.3.3","dockerName":"ubuntu20-kafka-3.8.0","dockerType":"service"},{"dockerTag":"1.0.7","dockerName":"ubuntu20-mdb-cruisecontrol","dockerType":"cruise-control"}],"sharded":false,"versionName":"3.8"}'::jsonb
)
ON CONFLICT (id) DO UPDATE SET cluster_params=EXCLUDED.cluster_params, status='done', type='new', hardware_preset_id=100;

-- Service auth для curl (access_type='w', НЕ 'write')
INSERT INTO services_auth (name, project_id, access_type)
VALUES ('local-tester', 160, 'w')
ON CONFLICT (name, project_id) DO UPDATE SET access_type='w';

-- Сброс sequences
SELECT setval('hardware_presets_id_seq', (SELECT MAX(id) FROM hardware_presets));
SELECT setval('host_state_id_seq', (SELECT MAX(id) FROM host_state));
SELECT setval('db_cluster_version_id_seq', (SELECT MAX(id) FROM db_cluster_version));
SELECT setval('namespaces_id_seq', (SELECT MAX(id) FROM namespaces));
SELECT setval('projects_id_seq', (SELECT MAX(id) FROM projects));
```

Применить:
```bash
docker cp /tmp/seed.sql postgres:/tmp/seed.sql && \
  docker exec postgres psql -U dev -d backstage_plugin_mdb -f /tmp/seed.sql
docker cp /tmp/seed.sql pg_backstage_plugin_mdb:/tmp/seed.sql && \
  docker exec pg_backstage_plugin_mdb psql -U dev -d backstage_plugin_mdb -f /tmp/seed.sql

# Рестарт Backstage backend для перечтения namespaces в Redis и PMS-конфига
pkill -f "mdb-app start\|mdb-backend start\|backstage-cli.*mdb"
yarn mdb-dev > /tmp/backstage.log 2>&1 &
```

## Триггер modify

```bash
TOKEN=$(node -e "
  const jwt=require('/Users/vl.ershov/Documents/Git/backstage/node_modules/jsonwebtoken');
  console.log(jwt.sign(
    {serviceName:'local-tester', projectId:160, accessType:'w'},
    '2210c0a2-fb9b-461f-9f21-a25acebb2559',
    {expiresIn:'365d'}
  ));")

curl -sS -X POST "http://localhost:7007/api/mdb/cluster/9e0336c7-50da-4487-8746-d332357180d3/version/" \
  -H "Authorization: ${TOKEN}" -H "Content-Type: application/json" \
  -d @/tmp/request.json -w "\nHTTP %{http_code}\n"
```

`/tmp/request.json` — полный шаблон в `SKILL.md` (раздел «Триггер modify»). Ключевое:
- `params` содержит **полный** набор полей (acl, lanIn/Out, diskGb, **diskType=nvme** — совпадает с baseline, иначе 422), `kafkaParams.controller` с `controllerDcs` (без него mdb-data NPE), `kafkaParams.brokerConfig`, `kafkaParams.cruiseControl`, `kafkaParams.jvmHeapSizeMb`
- Изменения относительно baseline (три диффа): `brokerConfig.config`, `controller.controllerConfig.config`, ресурсы (`lanIn 10→15`, `lanOut 20→25`, `hardwarePresetId 100→169`)
- `hardwarePresetId: 169`, `hosts: [{dc:"dc"},{dc:"kc"},{dc:"zc"}]`, `type: "update_instances"`, `attempts: 3`, `isNeedShards: false`

Ответ — HTTP 200 + `{status:"ok", createdClusterVersion:{id:N,…,status:"scheduled"}}`.

## Обход PMS-таски

Первая задача в chain — `update_kafka_pms_settings`. PMS-конфиг для namespace=infra указывает на prod (`stg.one-conf-web.devdc.odkl.ru`), который возвращает 400. Без обхода `modify_kafka_cluster` (зависящая от PMS) не запустится.

```sql
-- Найти PMS task
SELECT t.id FROM tasks t JOIN operations o ON o.id=t.operation_id
WHERE o.cluster_id='9e0336c7-50da-4487-8746-d332357180d3'
  AND o.status<>'canceled' AND t.type='update_kafka_pms_settings';

-- Помечаем done
UPDATE tasks SET status='done',
    result='{"status":"done","result":{"data":"Generated 5 settings"}}'::jsonb,
    finished_ts=now(), updated_at=now()
WHERE id=<id>;
```

Через ~10с (cadence воркера PT10S) `modify_kafka_cluster` подхватится, ModifyKafkaClusterTaskProcessor сделает PATCH в mdb-data, тот через mdb-processing стартует workflow.

## Проверка результата

```bash
# Tasks → должны быть все 'done'
docker exec postgres psql -U dev -d backstage_plugin_mdb -c "
  SELECT t.id, t.type, t.status, substring(t.result::text,1,80)
  FROM tasks t JOIN operations o ON o.id=t.operation_id
  WHERE o.cluster_id='9e0336c7-50da-4487-8746-d332357180d3' AND o.status<>'canceled'
  ORDER BY t.id;"
```
Ожидается:
```
update_kafka_pms_settings | done | Generated 5 settings
modify_kafka_cluster      | done | clusterId 9e0336c7…
finish_task               | done | true
```

```bash
# Temporal workflows (искать по началу workflowId = operationId из таски modify_kafka_cluster)
curl -s "http://localhost:8233/api/v1/namespaces/default/workflows?pageSize=30" | \
  jq -r '.executions[] | "\(.startTime[:19]) \(.status) \(.type.name) \(.execution.workflowId)"'
```

Ожидаемая цепочка (UPDATE_THEN_RESIZE order):
1. `modifyKafkaCluster` — RUNNING/COMPLETED (root)
2. `updateControllerConfig` — если controllerConfigDiff
3. `updateBrokerConfig` — если brokerConfigDiff, внутри `reloadKafkaDcConfig` для каждого DC (пауза 60с между DC)
4. `kafkaResize` → `kafkaResizeBroker` → `kafkaResizeBrokerInstance` для каждого broker — если brokerResourcesDiff

**Проверено и работает**: на текущем тесте увидели все workflow вплоть до `reloadKafkaDcConfig_{dc,kc,zc}` COMPLETED, далее идёт `kafkaResize…` цепочка (long-running через cloud API).

## Повторный запуск

```sql
UPDATE operations SET status='canceled'
WHERE cluster_id='9e0336c7-50da-4487-8746-d332357180d3'
  AND status IN ('in_progress','failed','scheduled','need_retry');

-- baseline id=135287 в status='done' НЕ трогаем
DELETE FROM db_cluster_version
WHERE cluster_id='9e0336c7-50da-4487-8746-d332357180d3'
  AND status IN ('draft','scheduled','failed','need_retry');
```

## Подводные камни (которые сожрали время)

1. **AccessType.WRITE = "w"**, не `'write'` (`plugins/common/src/authorization/dto.ts:28`). Service auth возвращал 403 пока в БД стояло `'write'`.
2. **`hardware_presets.database_preset`** — без `mongodbPreset` падает с `Cannot read properties of null (reading 'mongodbPreset')` 500 в DbParamsValidator даже для Kafka. Нужно **для ВСЕХ** пресетов кластера (включая baseline 100, не только целевой 169).
3. **Redis-кеш проектов** инициализируется при boot Backstage. Сидил namespace/project → нужен рестарт backend, иначе `Unknown namespaceId 2` или `getProjectByName` молча падает.
4. **PMS task** идёт в реальный prod-PMS, возвращает 400 — обходим UPDATE'ом в БД.
5. **`yarn dev` vs `yarn mdb-dev`** — первое падает без `app-config.mdb.yaml`. mdb-dev = `concurrently "yarn mdb-start" "yarn mdb-start-backend --inspect"` с обоими конфигами.
6. **IPv6** — `mdb-processing.host: 'http://localhost:8080'` не работает на macOS, нужен `127.0.0.1`.
7. **redis_sentinel conflict** — порт 26379 берёт первый запущенный (backstage stubs). mdb-data sentinel остаётся в Created, но это не блокирует приложение.
8. **`kafkaParams.controller` обязателен** — без `controllerDcs` mdb-data падает с NPE в `KafkaClusterModificationValidator.java:57`. Корневые `brokerConfig`/`controllerConfig` Jackson игнорирует — оборачивать в `kafkaParams.{controller.controllerConfig, brokerConfig}`.
9. **`diskType` неизменяемый** — `validateDbParamsForUpdateRequest` отвергает изменение типа диска: 422 `Нельзя изменить тип диска при изменении кластера`. В request.json всегда передавать значение baseline.
10. **`need_retry` в сбросе** — Backstage между попытками держит operation в `need_retry`. Без этого статуса в фильтре сброс не зацепит зависшую операцию.
11. **Baseline version (status='done')** — не удалять при сбросе, иначе следующий POST упадёт на «нет базовой версии».
12. **Backstage logs молчат на успех** — `ModifyKafkaClusterTaskProcessor` логирует только при ошибке. Прогресс смотреть через tasks в БД + `/tmp/mdb-data.log`/`/tmp/mdb-processing.log`, не через grep по `/tmp/backstage.log`.
13. **Workflow RUNNING ≠ провал** — `updateControllerConfig`/`kafkaResize` дёргают реальный one-cloud (`*.one-infra.ru`), локально недоступный. OCI runtime errors внутри workflow ожидаемы. Backstage-часть теста зелёная, как только `modify_kafka_cluster` task в `done` и main workflow стартанул.
14. **`pkill`** не всегда убивает Backstage node-процесс — после `pkill -f` проверять `lsof -nP -iTCP:7007 -sTCP:LISTEN -t` и добивать `kill -9 <PID>`.
