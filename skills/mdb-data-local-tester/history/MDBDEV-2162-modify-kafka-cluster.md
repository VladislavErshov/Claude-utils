# MDBDEV-2162: Testing modifyKafkaCluster — update + resize

Протестирована интеграция mdb-data + mdb-processing + temporal для `modifyKafkaCluster` с реальным кластером в one-cloud. Workflow проходит update broker config + resize broker resources.

## Кластер

- **cluster_id**: `9e0336c7-50da-4487-8746-d332357180d3`
- **name**: `test-update-resize1`
- **type**: `kafka`
- **project**: `mdbdev` (id=160)
- **namespace**: `infra` (id=2)

Реальные broker/controller хосты существуют в one-cloud (`master.dc.odkl.ru` / `master.kc.odkl.ru` / `master.zc.odkl.ru`).

## Запуск инфраструктуры

1. `/setup-local-mdb-data` — postgres + redis + mdb-data.
2. mdb-data запускать **с `--server.port=8081`** (8080 занят под processing):
   ```bash
   ./gradlew bootRun --args='--spring.profiles.active=local --server.port=8081' > /tmp/mdb-data.log 2>&1 &
   ```
3. `/setup-local-temporal` — temporal + vault + kafka + wiremock в `mdb-processing/localrun/`.
4. mdb-processing:
   ```bash
   cd /Users/vl.ershov/Documents/Git/mdb-processing
   BOOT_RUN_JVM_ARGS="--add-opens java.base/java.lang=ALL-UNNAMED" \
     ./gradlew bootRun --args='--spring.profiles.active=local' > /tmp/mdb-processing.log 2>&1 &
   ```

## Seed БД

Файл: `seed_real_cluster.sql` (применять через `docker cp` + `psql -f`).

### namespaces (id=2)
```sql
INSERT INTO namespaces (id, name, config, active, params)
VALUES (2, 'infra', '{"domain": "one-infra"}'::jsonb, true, '{"projects": ["all"]}'::jsonb)
ON CONFLICT (id) DO UPDATE SET name='infra', config=EXCLUDED.config, active=true;
```

### projects (id=160)
```sql
INSERT INTO projects (id, name, product_id, idm_synced, queue_synced)
VALUES (160, 'mdbdev', 7514, true, true)
ON CONFLICT (id) DO UPDATE SET name='mdbdev', product_id=7514, idm_synced=true, queue_synced=true;
```

### hardware_presets
```sql
-- baseline preset (текущий для кластера)
INSERT INTO hardware_presets (id, name, type, vcores_count, ram_gb, is_active)
VALUES (100, 'baseline', 'standard', 1, 4, true)
ON CONFLICT (id) DO UPDATE SET name='baseline', type='standard', vcores_count=1, ram_gb=4, is_active=true;

-- preset 169 (целевой, m.pico) — обновлён по SQL пользователя
UPDATE hardware_presets SET name = 'm.pico', type = 'memory_optimized', vcores_count = 2, ram_gb = 2,
  database_preset = '{"mongodbPreset": {"defaults": {"intervalCommitMs": 10, "wtEngineCacheSizeGb": 16, "maxIncomingConnections": 200}, "maxValues": {"wtEngineCacheSizeGb": 20, "maxIncomingConnections": 400}, "minValues": {"intervalCommitMs": 1, "wtEngineCacheSizeGb": 1, "maxIncomingConnections": 10}}}'::jsonb,
  is_active = true WHERE id = 169;
```

### db_cluster
```sql
INSERT INTO db_cluster (id, type, create_ts, update_ts, create_by, name, project_id, deleted, sharded, namespace_id, environment, criticality)
VALUES (
  '9e0336c7-50da-4487-8746-d332357180d3', 'kafka'::db_type,
  '2026-06-04 17:59:26.424000+03', '2026-06-04 17:59:26.424000+03',
  'vl.ershov', 'test-update-resize1',
  160, false, false, 2, 'production', 'D'
)
ON CONFLICT (id) DO UPDATE SET type='kafka', name='test-update-resize1', project_id=160, namespace_id=2, environment='production', criticality='D';
```

### one_cloud_meta (3 записи: broker, controller, cruise)
```sql
INSERT INTO one_cloud_meta (cluster_id, params, params_type)
VALUES
  ('9e0336c7-50da-4487-8746-d332357180d3',
   '{"isWan": false, "queue": "test-update-resize1-mdbdev-kafka", "domain": "one-infra", "fullQueue": "test-update-resize1-mdbdev-kafka.mdbdev.db.production.mdb.prod", "rootQueue": "prod", "serviceName": "broker"}'::jsonb,
   'db-service'),
  ('9e0336c7-50da-4487-8746-d332357180d3',
   '{"isWan": false, "queue": "test-update-resize1-mdbdev-kafka", "domain": "one-infra", "fullQueue": "test-update-resize1-mdbdev-kafka.mdbdev.db.production.mdb.prod", "rootQueue": "prod", "serviceName": "controller"}'::jsonb,
   'kafka-controller-service'),
  ('9e0336c7-50da-4487-8746-d332357180d3',
   '{"isWan": false, "queue": "test-update-resize1-mdbdev-kafka", "domain": "one-infra", "fullQueue": "test-update-resize1-mdbdev-kafka.mdbdev.db.production.mdb.prod", "rootQueue": "prod", "serviceName": "cruise"}'::jsonb,
   'cruise-control-service')
ON CONFLICT (cluster_id, params_type) DO UPDATE SET params=EXCLUDED.params;
```

### host_state (7 хостов)
**Формат hostname**: `{n}.{role}.{clustername}.{dc}.one-infra.ru`, где role ∈ {broker, controller, cruise}.

```sql
INSERT INTO host_state (id, cluster_id, host, update_ts, params, shard_id)
VALUES
  (91455, '9e0336c7-50da-4487-8746-d332357180d3', '1.broker.test-update-resize1-mdbdev-kafka.dc.one-infra.ru', '2026-06-04 17:59:29.096000+03', '{"dc": "dc"}'::jsonb, NULL),
  (91456, '9e0336c7-50da-4487-8746-d332357180d3', '1.broker.test-update-resize1-mdbdev-kafka.kc.one-infra.ru', '2026-06-04 17:59:29.096000+03', '{"dc": "kc"}'::jsonb, NULL),
  (91457, '9e0336c7-50da-4487-8746-d332357180d3', '1.broker.test-update-resize1-mdbdev-kafka.zc.one-infra.ru', '2026-06-04 17:59:29.096000+03', '{"dc": "zc"}'::jsonb, NULL),
  (91458, '9e0336c7-50da-4487-8746-d332357180d3', '1.cruise.test-update-resize1-mdbdev-kafka.dc.one-infra.ru', '2026-06-04 17:59:30.525000+03', '{"dc": "dc"}'::jsonb, NULL),
  (91459, '9e0336c7-50da-4487-8746-d332357180d3', '1.controller.test-update-resize1-mdbdev-kafka.dc.one-infra.ru', '2026-06-04 17:59:31.520000+03', '{"dc": "dc"}'::jsonb, NULL),
  (91460, '9e0336c7-50da-4487-8746-d332357180d3', '1.controller.test-update-resize1-mdbdev-kafka.kc.one-infra.ru', '2026-06-04 17:59:31.520000+03', '{"dc": "kc"}'::jsonb, NULL),
  (91461, '9e0336c7-50da-4487-8746-d332357180d3', '1.controller.test-update-resize1-mdbdev-kafka.zc.one-infra.ru', '2026-06-04 17:59:31.520000+03', '{"dc": "zc"}'::jsonb, NULL)
ON CONFLICT (id) DO UPDATE SET cluster_id=EXCLUDED.cluster_id, host=EXCLUDED.host, params=EXCLUDED.params;
```

### db_cluster_version (baseline — текущее состояние кластера)

**Важно**: `cluster_params.kafkaParams.brokerConfig` и `cluster_params.kafkaParams.controller.controllerConfig` должны быть (можно пустыми `{"config": {}}`), иначе `KafkaClusterDiffDetector` падает с NPE.

```sql
INSERT INTO db_cluster_version (id, cluster_id, cluster_params, create_ts, create_by, status, type, update_ts, attempts, hosts_params, hardware_preset_id, db_version)
VALUES (
  135287,
  '9e0336c7-50da-4487-8746-d332357180d3',
  '{"acl": {}, "name": "test-update-resize1", "isWan": false, "lanIn": 10, "diskGb": 8, "lanOut": 20, "diskType": "nvme", "projectId": 160, "rootQueue": "prod", "kafkaParams": {"controller": {"controllerDcs": ["dc", "kc", "zc"], "controllerLanIn": 15, "controllerMemGb": 4, "controllerDiskGb": 10, "controllerLanOut": 50, "controllerVcores": 4, "controllerDiskType": "nvme", "controllerJvmHeapSizeMb": "2048", "controllerConfig": {"config": {}}}, "brokerConfig": {"config": {}}, "cruiseControl": {"cruiseControlDc": "dc", "cruiseUserPassword": ""}, "jvmHeapSizeMb": 1024}, "needLanIpv6": true, "needWanIpv4": false, "needWanIpv6": false}'::jsonb,
  '2026-06-04 17:59:26.432000+03', 'vl.ershov', 'done'::version_status, 'new'::version_type, '2026-06-04 17:59:26.432000+03', 3,
  '{"units": [{"dc": "dc"}, {"dc": "kc"}, {"dc": "zc"}]}'::jsonb,
  100,
  '{"id": 9, "dockers": [{"dockerTag": "2.3.3", "dockerName": "ubuntu20-kafka-3.8.0", "dockerType": "service"}, {"dockerTag": "1.0.7", "dockerName": "ubuntu20-mdb-cruisecontrol", "dockerType": "cruise-control"}], "sharded": false, "versionName": "3.8"}'::jsonb
)
ON CONFLICT (id) DO UPDATE SET cluster_params=EXCLUDED.cluster_params, status='done', type='new', hardware_preset_id=100;
```

### Сброс sequences
```sql
SELECT setval('hardware_presets_id_seq', (SELECT MAX(id) FROM hardware_presets));
SELECT setval('host_state_id_seq', (SELECT MAX(id) FROM host_state));
SELECT setval('db_cluster_version_id_seq', (SELECT MAX(id) FROM db_cluster_version));
SELECT setval('namespaces_id_seq', (SELECT MAX(id) FROM namespaces));
SELECT setval('projects_id_seq', (SELECT MAX(id) FROM projects));
```

## Modify request

Файл: `/tmp/modify_request_final.json`. Триггерит все 3 diff'а:
- `brokerConfigDiff=true` — current `{"config": {}}` → new `{"compression.type": "uncompressed", "auto.create.topics.enable": "true"}`
- `brokerResourcesDiff=true` — preset 100 → 169, lanIn 10 → 15, lanOut 20 → 25
- `controllerConfigDiff=true` — current `{"config": {}}` → new `{"config": {"log.retention.hours": "168"}}`

```json
{
  "params": {
    "acl": {},
    "name": "test-update-resize1",
    "isWan": false,
    "lanIn": 15,
    "diskGb": 8,
    "lanOut": 25,
    "diskType": "nvme",
    "projectId": 160,
    "rootQueue": "prod",
    "kafkaParams": {
      "controller": {
        "controllerDcs": ["dc", "kc", "zc"],
        "controllerLanIn": 15,
        "controllerMemGb": 4,
        "controllerDiskGb": 10,
        "controllerLanOut": 50,
        "controllerVcores": 4,
        "controllerDiskType": "nvme",
        "controllerJvmHeapSizeMb": "2048",
        "controllerConfig": {
          "config": {"log.retention.hours": "168"}
        }
      },
      "cruiseControl": {
        "cruiseControlDc": "dc",
        "cruiseUserPassword": ""
      },
      "brokerConfig": {
        "config": {
          "auto.create.topics.enable": "true",
          "compression.type": "uncompressed"
        }
      },
      "jvmHeapSizeMb": 1024
    },
    "needLanIpv6": true,
    "needWanIpv4": false,
    "needWanIpv6": false
  },
  "hardwarePresetId": 169,
  "hosts": [
    {"dc": "dc"},
    {"dc": "kc"},
    {"dc": "zc"}
  ],
  "attempts": 3
}
```

**Endpoint**: `PATCH http://localhost:8081/api/v2/mdb/kafka/clusters/9e0336c7-50da-4487-8746-d332357180d3/modify`

**Ответ**: `202 Accepted` — operation создана, workflow стартовал.

## Проверка в temporal

```bash
# Список workflow
curl -s "http://localhost:8233/api/v1/namespaces/default/workflows?pageSize=20" | \
  jq -r '.executions[] | "\(.startTime) \(.status) \(.type.name) \(.execution.workflowId)"'

# Декодировать input modifyKafkaCluster
WORKFLOW_ID="..."
curl -s "http://localhost:8233/api/v1/namespaces/default/workflows/${WORKFLOW_ID}/history" | \
  jq -r '.history.events[0].workflowExecutionStartedEventAttributes.input.payloads[0].data' | \
  base64 -d | jq
```

### Ожидаемая структура workflow

`modifyKafkaCluster` (order=UPDATE_THEN_RESIZE) запускает последовательно:
1. `updateControllerConfig` (если `updateControllerConfigData` != null)
2. `updateBrokerConfig` (если `updateBrokerConfigData` != null) — внутри запускает `reloadKafkaDcConfig` для каждого DC, между DC пауза 60с
3. `resize` → `kafkaResize` → `kafkaResizeBroker` → `kafkaResizeBrokerInstance` для каждого broker

## Повторный запуск

```sql
DELETE FROM db_cluster_version WHERE cluster_id = '9e0336c7-50da-4487-8746-d332357180d3' AND status = 'draft';
UPDATE operations SET status = 'canceled' WHERE cluster_id = '9e0336c7-50da-4487-8746-d332357180d3' AND status IN ('in_progress', 'failed');
```

## Результат

При корректном seed с реальными хостами one-cloud:
- `modifyKafkaCluster` workflow стартует и запускает дочерние workflows.
- `updateBrokerConfig` → `reloadKafkaDcConfig` (по DC) — каждый DC завершается успешно, если broker хосты существуют в one-cloud и `instanceInfo` возвращает `state=RUNNING`.
- `resize` → `kafkaResize` → `kafkaResizeBroker` → `kafkaResizeBrokerInstance` — resize идёт через cloud API, может быть long-running.