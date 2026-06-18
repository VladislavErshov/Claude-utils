# MDBDEV-2098: API для вызова UpdateKafkaClusterWorkflow

## Суть задачи

Создать API-ручку (workflow-цепочку) для обновления конфигурации брокеров Kafka через Processing-сервис. Ручка вызывается при `type: "update_instances"` для Kafka-кластера, когда изменён `brokerConfig` (но не hardware).

### Код

- **Processor**: `plugins/mdb-backend/src/task/processor/kafka/workflow/StartKafkaResizeWorkflowTaskProcessor.ts`
- **DTO**: `UpdateKafkaBrokerConfigDto` (в `plugins/common/src/mdb/processing/dto.ts`)
- **Processing client**: `plugins/common/src/mdb/processing/kafka/KafkaProcessingClient.ts`
  - POST `api/v1/mdb/processing/kafka/clusters/${clusterId}/config`

### Условие попадания в `processBrokerConfigUpdate`

1. Проект должен быть в `kafkaResizeProcessingEnabledProjects` (таблица `settings` в БД)
2. `hasBrokerHardwareChanges() === false` (hardware не изменён)
3. `hasBrokerConfigChanges() === true` (в новой версии добавлен/изменён `kafkaParams.brokerConfig.config`)

Если проект НЕ в списке — используется старый путь через `UpdateKafkaInstancesTaskGenerator` (операторные задачи CloudOps).

### Формируемый JSON (UpdateKafkaBrokerConfigDto)

```json
{
  "operationId": "99dcc188-ef9f-4e6e-877a-5fdb54305c08",
  "namespace": "infra",
  "pmsHostName": "test-cruise-cpu2-mdbdev-kafka.clouds",
  "brokers": [
    "1.broker.test-cruise-cpu2-mdbdev-kafka.dc.one-infra.ru",
    "1.broker.test-cruise-cpu2-mdbdev-kafka.ic.one-infra.ru",
    "1.broker.test-cruise-cpu2-mdbdev-kafka.uc.one-infra.ru"
  ],
  "parameters": {
    "compression.type": "gzip",
    "log.retention.hours": "72"
  },
  "forceUpdate": false,
  "workflowTtl": "PT1H"
}
```

- `operationId` — из `operation.id`, **обязателен** (Processing использует как workflowId и search attribute)
- `namespace` — из `namespace.name` (Processing десериализует в enum `Namespace`)
- `pmsHostName` = `{cloudParams.queue}.clouds` (из `one_cloud_meta.params->queue`)
- `brokers` = `kafkaHosts.brokerHosts.map(h => h.fqdn)` (FQDN брокеров из `host_state`)
- `parameters` = `kafkaParams.brokerConfig.config` (ключ-значение, всё String)

## Ошибки при локальном тестировании

### 1. Проект не в kafkaResizeProcessingEnabledProjects

**Симптом**: Создаётся стандартная цепочка задач (operator tasks через CloudOps), а не workflow-цепочка с `start_resize_kafka_workflow`.

**Фикс**: Добавить проект в таблицу `settings`:
```sql
INSERT INTO settings (type, value)
VALUES ('kafkaResizeProcessingEnabledProjects', 'mdbdev')
ON CONFLICT DO NOTHING;
```

### 2. Processing URL не сконфигурирован

**Симптом**: `Only absolute URLs are supported` (status 500). В `app-config.mdb.local.yaml` поле `mdb-processing.host = 'host'` (не валидный URL).

**Фикс**: Использовать скилл `/local-temporal` для подключения к локальному Processing. Либо обойти вручную — JSON формируется корректно и логируется.

### 3. PMS недоступен

**Симптом**: `update_kafka_pms_settings` падает с HTTP 400 от PMS.

**Фикс**: Обойти вручную:
```sql
UPDATE tasks SET status = 'done',
    result = '{"status":"done","result":{}}'::jsonb,
    finished_ts = now(), updated_at = now()
WHERE id = <task_id>;
```

### 4. CloudOps "Not found partition"

**Симптом**: `start_kafka_update_broker_config_operator` падает с `Not found partition` (404).

**Фикс**: Эта задача из старого пути (без workflow). При включённом проекте в `kafkaResizeProcessingEnabledProjects` эта задача не создаётся.

### 5. Temporal search attributes не созданы

**Симптом**: Processing возвращает 503 `The task processing system is temporarily unavailable`. В логах: `INVALID_ARGUMENT: Namespace default has no mapping defined for search attribute OperationId`.

**Фикс**: Запустить `localrun.sh` или создать вручную:
```bash
docker run --rm --network localrun_local-dev-network temporalio/admin-tools:latest \
    temporal operator search-attribute create \
    --address temporal:7233 \
    --name OperationId --type Keyword \
    --name ClusterId --type Keyword
```

## Цепочка задач (workflow-путь)

```
update_kafka_pms_settings            → (упадёт, обойти вручную)
start_resize_kafka_workflow           → вызов processBrokerConfigUpdate → Processing API → Temporal
start_kafka_cruise_update_config_operator → (зависит от предыдущей)
get_kafka_cruise_update_config_result     → (зависит от предыдущей)
```

## Полный цикл локального тестирования

### 1. Запуск инфраструктуры

```bash
cd /Users/vl.ershov/Documents/Git/mdb-processing/localrun && docker-compose up -d && ./localrun.sh
cd /Users/vl.ershov/Documents/Git/backstage/stubs && docker-compose up -d
docker exec postgres psql -U dev -d postgres -c "CREATE DATABASE pg_boss"
```

### 2. Запуск Processing

```bash
BOOT_RUN_JVM_ARGS="--add-opens java.base/java.lang=ALL-UNNAMED" \
/Users/vl.ershov/Documents/Git/mdb-processing/gradlew \
    -p /Users/vl.ershov/Documents/Git/mdb-processing \
    bootRun --args='--spring.profiles.active=local'
```

### 3. Переключение Backstage на локальный Processing

В `app-config.mdb.local.yaml` заменить `mdb-processing.host: 'host'` на `mdb-processing.host: 'http://<host-ip>:8080'`.

**Важно**: использовать `<host-ip>`, не `localhost` (IPv6-проблема — Node.js резолвит localhost в `::1`, а Processing слушает только IPv4).

### 4. Запуск Backstage

```bash
yarn workspace mdb-backend start --config ../../app-config.mdb.yaml --config ../../app-config.mdb.local.yaml
```

### 5. Заполнение БД (seed)

```sql
-- Namespace
INSERT INTO namespaces (id, name, config, active, params)
VALUES (2, 'infra', '{"domain": "one-infra"}', true, '{"projects": []}')
ON CONFLICT (id) DO UPDATE SET name='infra', config=EXCLUDED.config;

-- Project
INSERT INTO projects (id, name, product_id, idm_synced, queue_synced)
VALUES (160, 'mdbdev', 4819, true, true)
ON CONFLICT (id) DO UPDATE SET name='mdbdev';

-- Cluster
INSERT INTO db_cluster (id, type, create_ts, update_ts, create_by, name, project_id, deleted, sharded, namespace_id, environment, criticality)
VALUES (
  '33048de4-cdb5-4581-9d54-3d05777bfb60', 'kafka',
  '2026-05-06 20:39:15.335', '2026-05-06 20:39:15.335',
  'vl.ershov', 'test-cruise-cpu2',
  160, false, false, 2, 'production', 'D'
)
ON CONFLICT (id) DO UPDATE SET type='kafka', name='test-cruise-cpu2', project_id=160, namespace_id=2;

-- One Cloud Meta (broker, controller, cruise-control)
INSERT INTO one_cloud_meta (cluster_id, params, params_type) VALUES
  ('33048de4-cdb5-4581-9d54-3d05777bfb60',
   '{"isWan": false, "queue": "test-cruise-cpu2-mdbdev-kafka", "domain": "one-infra", "fullQueue": "test-cruise-cpu2-mdbdev-kafka.mdbdev.db.production.mdb.prod", "rootQueue": "prod", "serviceName": "broker"}',
   'db-service'),
  ('33048de4-cdb5-4581-9d54-3d05777bfb60',
   '{"isWan": false, "queue": "test-cruise-cpu2-mdbdev-kafka", "domain": "one-infra", "fullQueue": "test-cruise-cpu2-mdbdev-kafka.mdbdev.db.production.mdb.prod", "rootQueue": "prod", "serviceName": "controller"}',
   'kafka-controller-service'),
  ('33048de4-cdb5-4581-9d54-3d05777bfb60',
   '{"isWan": false, "queue": "test-cruise-cpu2-mdbdev-kafka", "domain": "one-infra", "fullQueue": "test-cruise-cpu2-mdbdev-kafka.mdbdev.db.production.mdb.prod", "rootQueue": "prod", "serviceName": "cruise-control-service"}',
   'cruise-control-service')
ON CONFLICT DO NOTHING;

-- Host State (3 broker + 3 controller + 1 cruise)
INSERT INTO host_state (id, cluster_id, host, update_ts, params, shard_id) VALUES
  (86815, '33048de4-cdb5-4581-9d54-3d05777bfb60', '1.broker.test-cruise-cpu2-mdbdev-kafka.dc.one-infra.ru', '2026-05-06 20:39:18.107', '{"dc": "dc"}', NULL),
  (86816, '33048de4-cdb5-4581-9d54-3d05777bfb60', '1.broker.test-cruise-cpu2-mdbdev-kafka.uc.one-infra.ru', '2026-05-06 20:39:18.107', '{"dc": "uc"}', NULL),
  (86817, '33048de4-cdb5-4581-9d54-3d05777bfb60', '1.broker.test-cruise-cpu2-mdbdev-kafka.ic.one-infra.ru', '2026-05-06 20:39:18.107', '{"dc": "ic"}', NULL),
  (86818, '33048de4-cdb5-4581-9d54-3d05777bfb60', '1.cruise.test-cruise-cpu2-mdbdev-kafka.dc.one-infra.ru', '2026-05-06 20:39:18.676', '{"dc": "dc"}', NULL),
  (86819, '33048de4-cdb5-4581-9d54-3d05777bfb60', '1.controller.test-cruise-cpu2-mdbdev-kafka.dc.one-infra.ru', '2026-05-06 20:39:19.509', '{"dc": "dc"}', NULL),
  (86820, '33048de4-cdb5-4581-9d54-3d05777bfb60', '1.controller.test-cruise-cpu2-mdbdev-kafka.uc.one-infra.ru', '2026-05-06 20:39:19.509', '{"dc": "uc"}', NULL),
  (86821, '33048de4-cdb5-4581-9d54-3d05777bfb60', '1.controller.test-cruise-cpu2-mdbdev-kafka.ic.one-infra.ru', '2026-05-06 20:39:19.509', '{"dc": "ic"}', NULL)
ON CONFLICT (id) DO UPDATE SET cluster_id=EXCLUDED.cluster_id, host=EXCLUDED.host, params=EXCLUDED.params;

-- Base version (without brokerConfig — new version must add it)
INSERT INTO db_cluster_version (id, cluster_id, cluster_params, create_ts, create_by, status, type, update_ts, attempts, hosts_params, hardware_preset_id, db_version)
VALUES (90770, '33048de4-cdb5-4581-9d54-3d05777bfb60',
'{"acl": {}, "name": "test-cruise-cpu2", "isWan": false, "lanIn": 10, "diskGb": 8, "lanOut": 20, "diskType": "nvme", "projectId": 160, "rootQueue": "prod", "kafkaParams": {"controller": {"controllerDcs": ["dc", "uc", "ic"], "controllerLanIn": 15, "controllerMemGb": 4, "controllerDiskGb": 10, "controllerLanOut": 50, "controllerVcores": 4, "controllerDiskType": "nvme", "controllerJvmHeapSizeMb": "1024"}, "cruiseControl": {"cruiseControlDc": "dc", "cruiseUserPassword": ""}, "jvmHeapSizeMb": 1024}, "needLanIpv6": true, "needWanIpv4": false, "needWanIpv6": false}',
'2026-05-06 20:39:15.344', 'vl.ershov', 'scheduled', 'new', '2026-05-06 20:39:15.344', 3,
'{"units": [{"dc": "dc"}, {"dc": "uc"}, {"dc": "ic"}]}',
30,
'{"id": 9, "dockers": [{"dockerTag": "1.0.7", "dockerName": "ubuntu20-mdb-cruisecontrol", "dockerType": "cruise-control"}, {"dockerTag": "2.3.1", "dockerName": "ubuntu20-kafka-3.8.0", "dockerType": "service"}], "sharded": false, "versionName": "3.8"}')
ON CONFLICT (id) DO UPDATE SET cluster_params=EXCLUDED.cluster_params, status=EXCLUDED.status;

-- Feature flag
INSERT INTO settings (type, value)
VALUES ('kafkaResizeProcessingEnabledProjects', 'mdbdev')
ON CONFLICT DO NOTHING;

-- Reset sequences
SELECT setval('db_cluster_version_id_seq', (SELECT COALESCE(MAX(id), 1) FROM db_cluster_version));
SELECT setval('host_state_id_seq', (SELECT COALESCE(MAX(id), 1) FROM host_state));
SELECT setval('settings_id_seq', (SELECT COALESCE(MAX(id), 1) FROM settings));
```

### 6. Отправка curl

```bash
curl -s -X POST http://localhost:7007/api/mdb/cluster/33048de4-cdb5-4581-9d54-3d05777bfb60/version/ \
  -H "Content-Type: application/json" \
  -d '{
  "params": {
    "projectId": 160,
    "name": "test-cruise-cpu2",
    "lanOut": 20, "lanIn": 10, "diskGb": 8, "diskType": "nvme",
    "rootQueue": "prod", "isWan": false,
    "needLanIpv6": true, "needWanIpv4": false, "needWanIpv6": false,
    "kafkaParams": {
      "controller": {
        "controllerDcs": ["dc","uc","ic"],
        "controllerLanIn": 15, "controllerMemGb": 4, "controllerDiskGb": 10,
        "controllerLanOut": 50, "controllerVcores": 4, "controllerDiskType": "nvme",
        "controllerJvmHeapSizeMb": "1024"
      },
      "cruiseControl": { "cruiseControlDc": "dc", "cruiseUserPassword": "" },
      "jvmHeapSizeMb": 1024,
      "brokerConfig": { "config": { "compression.type": "gzip", "log.retention.hours": "72" } }
    },
    "acl": {}
  },
  "hardwarePresetId": 30,
  "dbVersionId": 9,
  "isNeedShards": false,
  "hosts": [{"dc":"dc"},{"dc":"uc"},{"dc":"ic"}],
  "type": "update_instances",
  "attempts": 3
}'
```

### 7. Обход PMS и проверка

```sql
-- Проверить задачи
SELECT id, type, status FROM tasks WHERE operation_id = '<operation_id>' ORDER BY id;

-- Обойти PMS
UPDATE tasks SET status = 'done',
    result = '{"status":"done","result":{"data":"Generated 5 settings"}}'::jsonb,
    finished_ts = now(), updated_at = now()
WHERE id = <pms_task_id>;

-- Проверить результат workflow
SELECT id, type, status, substring(result::text, 1, 300) FROM tasks WHERE operation_id = '<operation_id>' ORDER BY id;
```

### 8. Проверка в Temporal

```bash
# По operationId
curl -s 'http://localhost:8233/api/v1/namespaces/default/workflows?query=OperationId+%3D+%27<operation_id>%27'
```

Или открыть http://localhost:8233 — найти workflow `updateBrokerConfig` по operationId.

### 9. Очистка для повторного теста

```sql
DELETE FROM tasks WHERE operation_id IN (SELECT id FROM operations WHERE cluster_id = '33048de4-cdb5-4581-9d54-3d05777bfb60');
DELETE FROM operations WHERE cluster_id = '33048de4-cdb5-4581-9d54-3d05777bfb60';
DELETE FROM db_cluster_version WHERE cluster_id = '33048de4-cdb5-4581-9d54-3d05777bfb60' AND id > 90770;
```

После очистки можно отправить curl повторно.

### 10. Откат конфига

Вернуть `mdb-processing.host: 'host'` в `app-config.mdb.local.yaml`.
