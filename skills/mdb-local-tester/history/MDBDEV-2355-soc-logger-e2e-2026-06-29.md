# MDBDEV-2355: SOC logger + JVM heap + tosAgent — E2E через Backstage → mdb-data → processing (2026-06-29)

End-to-end тест новой логики `ModifyKafkaClusterParams.socLogger` через все три компонента.
Предыдущая история (`MDBDEV-2355-soc-logger-2026-06-29.md`) покрывала только прямой PATCH к mdb-data;
здесь — полный путь от Backstage `POST /version/` до temporal workflow input.

## Путь

Backstage `POST /api/mdb/cluster/{id}/version/` → `CreateOperationTasksJob` →
`TaskChainGenerator` (проект НЕ в `kafkaResizeProcessingEnabledProjects` setting) →
`UpdateKafkaInstancesTaskGenerator` → таска `MODIFY_KAFKA_CLUSTER` →
`ModifyKafkaClusterTaskProcessor` → mdb-data `PATCH /api/v2/mdb/kafka/clusters/{id}/modify` →
processing → temporal workflow `modifyKafkaCluster`.

Ключевая настройка: в `settings` **НЕ** должно быть `kafkaResizeProcessingEnabledProjects`
(или значение не должно включать `mdbdev` и `all`) — иначе `isProjectEnabledForProcessing=true`
и Backstage пойдёт по старому пути `START_RESIZE_KAFKA_WORKFLOW` (resize-эндпоинты processing),
где `socLogger` не поддерживается.

## Кластер

- **cluster_id**: `9e0336c7-50da-4487-8746-d332357180d3` (name `test-update-resize1`, type `kafka`)
- **project_id**: 160 (`mdbdev`), **namespace_id**: 2 (`infra`)
- Baseline `db_cluster_version` (status=done, id=135287):
  - `db_version.dockers[0].dockerTag`: `2.3.3` (для tosAgent-негатива; повышается до `2.4.0` для позитива)
  - hardware_preset_id=100 (vcores=1, ram_gb=4, **обязательно с `database_preset.mongodbPreset`**)
  - `kafkaParams.brokerConfig.config={}`, `kafkaParams.controller.controllerConfig.config={}` (иначе NPE в `KafkaClusterDiffDetector`)
  - `kafkaParams.jvmHeapSizeMb=1024` (broker), `controllerJvmHeapSizeMb="2048"` (controller)
  - нет `socLogger`, нет `tosAgent`
- **services_auth**: `(name='local-tester', project_id=160, access_type='w')` для JWT

## Запрос

Файл: `/tmp/request_soc_e2e.json`. Body для `POST /api/mdb/cluster/{id}/version/`:

```json
{
  "params": {
    "acl": {}, "name": "test-update-resize1", "isWan": false,
    "lanIn": 10, "diskGb": 8, "lanOut": 20, "diskType": "nvme",
    "projectId": 160, "rootQueue": "prod",
    "kafkaParams": {
      "controller": {
        "controllerDcs": ["dc","kc","zc"], "controllerLanIn": 15, "controllerMemGb": 4,
        "controllerDiskGb": 10, "controllerLanOut": 50, "controllerVcores": 4,
        "controllerDiskType": "nvme", "controllerJvmHeapSizeMb": 3072,
        "controllerConfig": {"config": {"log.retention.hours": "168"}}
      },
      "cruiseControl": {"cruiseControlDc": "dc", "cruiseUserPassword": "", "jvmHeapSizeMb": 5120},
      "brokerConfig": {"config": {"compression.type": "uncompressed"}},
      "jvmHeapSizeMb": 2048,
      "tosAgent": false
    },
    "needLanIpv6": true, "needWanIpv4": false, "needWanIpv6": false,
    "socLogger": {
      "enabled": true,
      "endpoint": [
        "1.broker.kafka-queries-soc-mdb-kafka.uc.one-infra.ru:9092",
        "1.broker.kafka-queries-soc-mdb-kafka.pc.one-infra.ru:9092",
        "1.broker.kafka-queries-soc-mdb-kafka.kc.one-infra.ru:9092"
      ],
      "passwordVaultPath": "/zkv/dbs/logs-broker/kafka:soc-logs-password",
      "topic": "soc-audit-log",
      "user": "soc-logs-robot"
    }
  },
  "hardwarePresetId": 100, "isNeedShards": false,
  "hosts": [{"dc":"dc"},{"dc":"kc"},{"dc":"zc"}],
  "type": "update_instances", "attempts": 3
}
```

JWT: `node -e "jwt.sign({serviceName:'local-tester',projectId:160,accessType:'w'},'2210c0a2-fb9b-461f-9f21-a25acebb2559',{expiresIn:'365d'})"`.

## Сценарии

### 1. Позитивный: socLogger + jvm heap changes + tosAgent=false (docker 2.3.3)

- Backstage: **200**, `db_cluster_version` id=135288 (status=scheduled), в `clusterParams` приехал `socLogger`
- `modify_kafka_cluster` task = **done** (после обхода PMS)
- Temporal workflowId `5a9440d9-a061-4b2a-86d8-55828447f76b`, input:
  ```json
  {
    "socLoggerData": {
      "enabled": true,
      "endpoint": ["...uc...", "...pc...", "...kc..."],
      "passwordVaultPath": "/zkv/dbs/logs-broker/kafka:soc-logs-password",
      "topic": "soc-audit-log", "user": "soc-logs-robot"
    },
    "updateBrokerConfigData": {
      "heapSizeMB": 2048, "tosAgentEnabled": false,
      "parameters": {"compression.type": "uncompressed"}, ...
    },
    "updateControllerConfigData": {
      "heapSizeMB": 3072, "parameters": {"log.retention.hours": "168"}, ...
    },
    "brokerOrder": "RESIZE_THEN_UPDATE",
    "controllerOrder": "RESIZE_THEN_UPDATE",
    "cruiseOrder": "UPDATE_THEN_RESIZE"
  }
  ```

### 2. Негативный: socLogger.endpoint=[] (docker 2.3.3, tosAgent=false)

- Backstage: **200** (Backstage НЕ валидирует содержимое `socLogger.endpoint`)
- `modify_kafka_cluster` task = **failed** с ошибкой mdb-data:
  ```json
  {"errors": [{"field": "params.socLogger.endpoint",
               "message": "endpoint должен содержать хотя бы один адрес"}],
   "error_message": "Validation failed"}
  ```
- Подтверждает: валидация `socLogger` живёт в mdb-data (`KafkaClusterModificationValidator.validateSocLogger`), ошибка пробрасывается в Backstage как failed task.

### 3. Негативный: tosAgent=true + docker 2.3.3

- Backstage: **422** (Backstage сам валидирует tosAgent против docker-версии ДО отправки в mdb-data):
  ```json
  {"success": false, "field": "params.kafkaParams.tosAgent",
   "message": "Агент сетевой оптимизации репликации доступен только на образе брокера версии 2.4.0 и выше. Выполните минорное обновление."}
  ```
- То есть tosAgent валидируется **дважды**: Backstage (422) и mdb-data (400, если бы дошло).

### 4. Позитивный: tosAgent=true + docker bumped to 2.4.0

Перед тестом: `UPDATE db_cluster_version SET db_version = jsonb_set(db_version, '{dockers,0,dockerTag}', '"2.4.0"') WHERE id=135287;` в обеих БД.

- Backstage: **200**, `modify_kafka_cluster` task = **done**
- Temporal workflowId `7adf60fb-dacb-4cc8-9a1a-d7858f131d69`, input:
  ```json
  {
    "updateBrokerConfigData": {
      "heapSizeMB": 2048, "tosAgentEnabled": true, ...
    },
    "socLoggerData": {...},  // одновременно с tosAgent
    "updateControllerConfigData": {"heapSizeMB": 3072, ...},
    "brokerOrder": "RESIZE_THEN_UPDATE",
    "controllerOrder": "RESIZE_THEN_UPDATE",
    "cruiseOrder": "UPDATE_THEN_RESIZE"
  }
  ```

## Подтверждённые propagation-цепочки

| Поле в request DTO             | Поле в temporal workflow input              | Статус       |
|--------------------------------|---------------------------------------------|--------------|
| `socLogger.*`                  | `socLoggerData.*`                           | ✅ propagated |
| `kafkaParams.jvmHeapSizeMb`    | `updateBrokerConfigData.heapSizeMB`         | ✅ propagated |
| `kafkaParams.controller.controllerJvmHeapSizeMb` | `updateControllerConfigData.heapSizeMB` | ✅ propagated |
| `kafkaParams.cruiseControl.jvmHeapSizeMb` | — (нигде)                           | ❌ dropped (открытая находка) |
| `kafkaParams.tosAgent`         | `updateBrokerConfigData.tosAgentEnabled`    | ✅ propagated |
| broker heap 1024→2048          | `brokerOrder=RESIZE_THEN_UPDATE`            | ✅ correct    |
| controller heap 2048→3072      | `controllerOrder=RESIZE_THEN_UPDATE`        | ✅ correct    |
| cruise heap (current отсутствует) | `cruiseOrder=UPDATE_THEN_RESIZE` (const)  | ✅ correct    |

## Что проверено в Backstage-коде

`plugins/mdb-backend/src/dto/responseDto.ts`:
- добавлен тип `SocLoggerDto = { enabled: boolean; endpoint: string[]; passwordVaultPath: string; topic: string; user: string }`
- в `ClusterParams` добавлено `socLogger?: SocLoggerDto`

Этого достаточно: `cluster_params` грузится из JSON-колонки как `ClusterParams`, а `ModifyKafkaClusterTaskProcessor`
шлёт `version.clusterParams` целиком как `params` в `ModifyKafkaClusterRequest`. Поле уезжает на верхний уровень
`params` — ровно там, где его ждёт `ModifyKafkaClusterParams.socLogger` в mdb-data.

## Подводные камни

1. **`kafkaResizeProcessingEnabledProjects` setting**: если в `settings` есть запись со значением `all` или
   содержащим имя проекта — `isProjectEnabledForProcessing=true` и Backstage пойдёт по старому пути
   `START_RESIZE_KAFKA_WORKFLOW` → `StartKafkaResizeWorkflowTaskProcessor`, где `socLogger` НЕ поддерживается
   (`ResizeKafkaClusterDto` его не имеет). Для теста новой логики setting НЕ должен содержать проект.
2. **`hardware_presets.database_preset.mongodbPreset`**: обязательно для всех пресетов кластера (даже Kafka),
   иначе `POST /version/` падает с 500 `Cannot read properties of null (reading 'mongodbPreset')` в
   `PresetsMapper.mapHardwarePresetEntityToModel`. В существующем `seed_kafka_cluster.sql` preset 100
   был без `database_preset` — пришлось добавлять вручную.
3. **PMS task (`update_kafka_pms_settings`)**: идёт первой в цепочке, падает на реальном prod-PMS
   (`stg.one-conf-web.devdc.odkl.ru` возвращает 404/400). Обходить SQL:
   ```sql
   UPDATE tasks SET status='done',
       result='{"status":"done","result":{"data":"Generated 5 settings"}}'::jsonb,
       finished_ts=now(), updated_at=now()
   WHERE type='update_kafka_pms_settings' AND status IN ('in_progress','failed','need_retry','scheduled');
   ```
4. **Backstage-валидатор cruise heap**: требует `> 4Гб` (строго больше 4096). В истории mdb-data
   использовалось 4095 (проходило mdb-data, но НЕ Backstage). Использовать 5120.
5. **Backstage-валидатор tosAgent**: проверяет docker-образ ДО отправки в mdb-data. Если baseline
   docker < 2.4.0 — 422 сразу из Backstage, до mdb-data не доходит.
6. **Опечатка в workflowId**: mdb-data логирует operation id как `5a9440d9-...-55828447f76b` (5582, не 5528).
   Легко пропустить при копировании.
7. **Temporal CLI workflow show**: требует `--namespace default` (иначе "workflow not found").
   UI API на :8233 использует другой формат путей, проще использовать `temporal workflow show` из
   `temporalio/admin-tools` Docker-образа.

## Cleanup между сценариями

```sql
-- Backstage DB (postgres:6432)
DELETE FROM tasks WHERE operation_id IN (SELECT id FROM operations WHERE cluster_id='9e0336c7-50da-4487-8746-d332357180d3');
DELETE FROM operations WHERE cluster_id='9e0336c7-50da-4487-8746-d332357180d3';
DELETE FROM db_cluster_version WHERE cluster_id='9e0336c7-50da-4487-8746-d332357180d3' AND status IN ('scheduled','draft','failed','need_retry');

-- mdb-data DB (pg_backstage:6434)
DELETE FROM operations WHERE cluster_id='9e0336c7-50da-4487-8746-d332357180d3';
DELETE FROM db_cluster_version WHERE cluster_id='9e0336c7-50da-4487-8746-d332357180d3' AND status IN ('scheduled','draft','failed','need_retry');
```

После tosAgent-позитива восстановить docker tag обратно на 2.3.3 (или оставить 2.4.0 — следующая
итерация теста должна учитывать).

## Запуск инфраструктуры

1. `/setup-local-backstage` → postgres:6432, redis:6379, sentinel:26379, clickhouse, Backstage :7007
2. `/setup-local-mdb-data` → pg_backstage:6434, mdb-data :8081
3. `/setup-local-temporal` → temporal:7233, vault, kafka, wiremock, mdb-processing :8080
4. Seed `/tmp/seed_soc_e2e.sql` в обе БД + добавить `mongodbPreset` в preset 100
5. **Рестарт Backstage** (Redis-кеш проектов строится при boot)

## Файлы

- Seed: `/tmp/seed_soc_e2e.sql`
- Request (positive): `/tmp/request_soc_e2e.json`
- Request (negative socLogger): `/tmp/request_soc_neg_endpoint.json`
- Request (tosAgent): `/tmp/request_tos_neg.json`
- Workflow input dumps: `/tmp/wf_show.json`, `/tmp/wf_tos.json`
