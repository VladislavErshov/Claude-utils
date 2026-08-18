# MDBDEV-2162 (2026-06-30): unified modify endpoint (resize/config → modify)

Прогон теста из `MDBDEV-2162-modify-kafka-cluster-rerun-2026-06-20.md` после рефакторинга: убраны вызовы `/resize` и `/config` в processing-сервисе, для kafka `MODIFY_CLUSTER` всегда идёт единый `PATCH /api/v2/mdb/kafka/clusters/{id}/modify`.

## Кластер

- **cluster_id**: `9e0336c7-50da-4487-8746-d332357180d3` (`test-update-resize1`, project=mdbdev id=160, namespace=infra id=2)
- **baseline version**: 135287 (`status=done, type=new`)
- **новая version**: 135291 (`update_instances, status=scheduled → done`)

## Что изменилось в коде

1. `ProcessingEnabledProjectChecker` — для `KAFKA + MODIFY_CLUSTER` всегда возвращает `true` (ранний `return`), без обращения к settings. Флаг `KAFKA_RESIZE_ENABLED_PROJECTS` удалён из `SettingsType` и маппинга.
2. `StartKafkaResizeWorkflowTaskProcessor` — переписан: убрано ветвление `processResize`/`processBrokerConfigUpdate`, убраны хелперы `collectBrokerParameters`, `buildControllerResources`, `buildBrokerResources`. Теперь всегда собирает DTO `{params, hardwarePresetId, hosts, attempts}` и вызывает `mdbDataManager.modifyKafkaCluster(...)` → `PATCH /api/v2/mdb/kafka/clusters/{id}/modify`.
3. `KafkaProcessingClient`/`KafkaProcessingService` — удалены методы `resize` и `updateBrokerConfig`. Endpoint'ы `/api/v1/mdb/processing/kafka/clusters/{id}/resize` и `/config` больше не вызываются из Backstage.
4. Удалены DTO `ResizeKafkaClusterDto`, `ResizeKafkaServiceResourcesDto`, `UpdateKafkaBrokerConfigDto`.
5. `MdbDependenciesBuilder.buildVersionTaskProcessors` — убраны параметры `kafkaHostsManager`/`kafkaProcessingService` (стали unused). `new StartKafkaResizeWorkflowTaskProcessor(clusterManager, mdbDataService, logger)`. `kafkaHostsManager`/`kafkaProcessingService` остались в `buildDatabaseTaskProcessors` для `StartKafkaUpsertTopicWorkflowProcessor`.

## Запуск инфраструктуры

Все сервисы уже были запущены (up 18 hours):
- `stubs` (postgres:6432, redis:6379, sentinel:26379, clickhouse) + Backstage :7007
- `mdb-data` :8081 (pg_backstage_plugin_mdb:6434)
- `mdb-processing` :8080 + temporal:7233 + temporal-ui:8233 + vault:8200 + kafka:29092 + wiremock:8088

**Обязательный перезапуск Backstage** — он был запущен до изменений в коде:
```bash
PID=$(lsof -nP -iTCP:7007 -sTCP:LISTEN -t); kill $PID; sleep 3
yarn mdb-dev > /tmp/backstage.log 2>&1 &
# ~70с до «Listening on :7007»
```

## Сидирование

Сиды уже лежали в обеих БД от прошлых прогонов. Дополнительно применялось только:
```sql
-- Очистка старой version 135290 (scheduled от прошлого прогона)
DELETE FROM db_cluster_version
WHERE cluster_id='9e0336c7-50da-4487-8746-d332357180d3'
  AND status IN ('draft','scheduled','failed','need_retry');
-- в обеих БД (postgres:6432 + pg_backstage_plugin_mdb:6434)
```

`services_auth` для `local-tester` (project_id=160, access_type='w') уже присутствовал — JWT работает.

## /tmp/request.json

Тот же шаблон, что в rerun 2026-06-20, с поправкой `lanIn: 20` (валидатор требует минимум 20 Мб/с для брокеров):

```json
{
  "params": {
    "kafkaParams": {
      "createTopicWhenKafkaStart": true,
      "topicRetentionMs": 604800000,
      "replicationFactor": 3,
      "defaultPartitionCount": 100,
      "controller": {
        "controllerDcs": ["dc", "kc", "zc"],
        "controllerLanIn": 15, "controllerLanOut": 50,
        "controllerVcores": 4, "controllerMemGb": 4,
        "controllerDiskGb": 10, "controllerDiskType": "nvme",
        "controllerJvmHeapSizeMb": "2048",
        "controllerConfig": {"config": {"log.flush.interval.messages": 10000}}
      },
      "brokerConfig": {"config": {"num.network.threads": 16, "socket.send.buffer.bytes": 204800}},
      "cruiseControl": {"cruiseControlDc": "dc", "cruiseUserPassword": ""},
      "jvmHeapSizeMb": 1024
    },
    "kafkaVersion": "3.7.0",
    "compression": {"type": "producer"},
    "acl": {"topics": []},
    "lanIn": 20, "lanOut": 20,
    "diskType": "nvme", "diskGb": 8
  },
  "hardwarePresetId": 169,
  "hosts": [{"dc":"dc"},{"dc":"kc"},{"dc":"zc"}],
  "type": "update_instances",
  "attempts": 3,
  "isNeedShards": false
}
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

Первая попытка → HTTP 400 `{"error":{"field":"params.lanIn","message":"Для брокеров минимальное значение lan_in должно быть не менее 20 Мб/с"}}`. После правки `lanIn: 10 → 20` → HTTP 200, `createdClusterVersion.id=135291, status=scheduled`.

## Обход PMS

PMS-конфиг для namespace=infra на prod-PMS → 400. PMS-таска падает в `need_retry`, `start_resize_kafka_workflow` не стартует.

```sql
UPDATE tasks SET status='done',
    result='{"status":"done","result":{"data":"Generated 5 settings"}}'::jsonb,
    finished_ts=now(), updated_at=now()
WHERE type='update_kafka_pms_settings'
  AND operation_id IN (
    SELECT id FROM operations
    WHERE cluster_id='9e0336c7-50da-4487-8746-d332357180d3' AND status<>'canceled'
  )
  AND status<>'done'
RETURNING id, status;
-- task id=10 → done
```

Через ~10с (cadence PT10S) `start_resize_kafka_workflow` подхватывается и завершается `done`.

## Результат

Backstage tasks (operation `6681fc89-abf3-4913-a7be-a297d84f2dca`):
```
 10 | update_kafka_pms_settings                 | done       | Generated 5 settings
 11 | start_resize_kafka_workflow               | done       | {"data":{"fullQueue":"test-update-resize1-mdbdev-kafka.mdbdev.db.production.mdb.prod","workflowDomainName":"kafka"}}
 12 | start_kafka_cruise_update_config_operator | scheduled  |  (обращается к реальному cruise control — недоступен локально)
 13 | get_kafka_cruise_update_config_result     | scheduled  |
```

mdb-data логи:
```
KafkaClusterController.modifyKafkaCluster — STARTED
KafkaClusterFacade - Received request to modify Kafka cluster: clusterId=9e0336c7-50da-4487-8746-d332357180d3
OperationServiceImpl - Created operation d6b3d32f-968d-433a-b26a-e481634ff4ca of type MODIFY_CLUSTER
KafkaClusterModificationServiceImpl - Successfully initiated Kafka cluster modification
KafkaClusterController.modifyKafkaCluster — COMPLETED
```

Temporal workflows (operationId `d6b3d32f-968d-433a-b26a-e481634ff4ca`):
```
07:12:23 RUNNING modifyKafkaCluster       d6b3d32f-968d-433a-b26a-e481634ff4ca
07:12:23 RUNNING modifyController         d6b3d32f-968d-433a-b26a-e481634ff4ca_modify-controller
07:12:23 RUNNING updateControllerConfig   d6b3d32f-968d-433a-b26a-e481634ff4ca_modify-controller_update-controller-config
```

Цепочка работает: Backstage → mdb-data (PATCH /api/v2/mdb/kafka/clusters/{id}/modify) → mdb-processing → Temporal `modifyKafkaCluster`. Workflow long-running — `updateControllerConfig` пингует реальный cloud-хост (`*.one-infra.ru`), недоступный локально → OCI runtime errors, workflow висит. Это ожидаемо.

## Ключевое отличие от прошлых прогонов

Раньше (rerun 2026-06-20) workflow-путь использовал `ModifyKafkaClusterTaskProcessor` (task type `modify_kafka_cluster`) — это был **старый операторный путь** через `TaskChainGenerator`, который тоже вызывал `modifyKafkaCluster`. Workflow-путь через `StartKafkaResizeWorkflowTaskProcessor` (task type `start_resize_kafka_workflow`) тогда не использовался для modify — только если проект был в `kafkaResizeProcessingEnabledProjects`.

Теперь:
- `KAFKA + MODIFY_CLUSTER` всегда идёт через **workflow-путь** (`START_RESIZE_KAFKA_WORKFLOW` task).
- Внутри `StartKafkaResizeWorkflowTaskProcessor` вызывается `mdbDataManager.modifyKafkaCluster` (тот же endpoint, что в старом `ModifyKafkaClusterTaskProcessor`).
- Старый путь через `TaskChainGenerator` → `UpdateKafkaInstancesTaskGenerator` → `MODIFY_KAFKA_CLUSTER` task — недостижим (ранний `return true` в `ProcessingEnabledProjectChecker`). Мёртвый код, можно вычистить отдельно.

Старый `ModifyKafkaClusterTaskProcessor` и task type `MODIFY_KAFKA_CLUSTER` остались зарегистрированными, но для kafka `MODIFY_CLUSTER` не вызываются.

## Подводные камни (новые в этом прогоне)

1. **`lanIn` минимум 20 для брокеров** — валидатор `ValidateKafkaBrokerLanIn` (или аналог) отвергает `lanIn < 20` с 400 `Для брокеров минимальное значение lan_in должно быть не менее 20 Мб/с`. В шаблоне из rerun 2026-06-20 было `lanIn: 10` — не проходит. Ставить `lanIn: 20`.
2. **Backstage нужно перезапустить после изменения кода** в `plugins/mdb-backend` и `plugins/common`. `yarn mdb-dev` подхватывает TS-изменнения только при reload.
3. **Cruise control tasks блокируют finish_task** — если в request.json есть `cruiseControl.cruiseControlDc`, создаются tasks 12/13 (`start_kafka_cruise_update_config_operator` / `get_kafka_cruise_update_config_result`). Они идут после `start_resize_kafka_workflow` и не завершаются локально (нет cruise control). `finish_task` не создаётся. Для чистого теста modify можно убрать `cruiseControl` из request.json.

## Повторный запуск

```sql
UPDATE operations SET status='canceled'
WHERE cluster_id='9e0336c7-50da-4487-8746-d332357180d3'
  AND status IN ('in_progress','failed','scheduled','need_retry');

DELETE FROM db_cluster_version
WHERE cluster_id='9e0336c7-50da-4487-8746-d332357180d3'
  AND status IN ('draft','scheduled','failed','need_retry');
-- baseline version 135287 (status=done) НЕ трогать
-- применять в обеих БД (postgres:6432 + pg_backstage_plugin_mdb:6434)
```
