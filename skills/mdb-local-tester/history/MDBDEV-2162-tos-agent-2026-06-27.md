# MDBDEV-2162: tosAgent — валидация docker-образа при модификации Kafka (2026-06-27)

Новое поле `tosAgent: @Nullable Boolean` в `ModifyKafkaParams`. Включение tosAgent требует
docker-образ версии ≥ 2.4.0. Валидация в `KafkaClusterModificationValidator.validateTosAgent`.

## Кластер

- **cluster_id**: `9e0336c7-50da-4487-8746-d332357180d3` (name `test-update-resize1`, type `kafka`)
- Baseline `db_cluster_version` (status=done, id=135287):
  - `db_version.dockers[0]`: `{dockerTag: "2.3.3", dockerName: "ubuntu20-kafka-3.8.0", dockerType: "service"}`
  - hardware_preset_id=100 (ramGb=8)

## Валидатор

`KafkaClusterModificationValidator.validateTosAgent`:
- `tosAgent == null || false` → ok (валидация пропускается)
- `tosAgent == true` && `currentDockerVersion < 2.4.0` (или null) → error
  `params.kafkaParams.tosAgent`: "Включение tosAgent требует docker-образ версии DockerTagVersion[major=2, minor=4, patch=0] или выше"

`TOS_AGENT_MIN_DOCKER_VERSION = DockerTagVersion.of(2, 4, 0)`.

Source of `currentDockerVersion`: `KafkaClusterFacade.resolveCurrentDockerVersion(currentVersion)`:
- берёт `db_version.dockers`, фильтрует по `dockerType == "service"`, берёт первый;
- парсит `dockerTag` через `DockerTagVersion.parse`.
- Если dockers пусто → null (валидатор тоже падает с ошибкой при tosAgent=true).

## Запрос

Файл: `/tmp/modify_request_tos_agent.json`.

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
      "cruiseControl": {"cruiseControlDc": "dc", "cruiseUserPassword": "", "jvmHeapSizeMb": 4095},
      "brokerConfig": {"config": {"compression.type": "uncompressed"}},
      "jvmHeapSizeMb": 2048,
      "tosAgent": true
    },
    "needLanIpv6": true, "needWanIpv4": false, "needWanIpv6": false
  },
  "hardwarePresetId": 100,
  "hosts": [{"dc":"dc"},{"dc":"kc"},{"dc":"zc"}],
  "attempts": 3
}
```

Endpoint: `PATCH http://localhost:8081/api/v2/mdb/kafka/clusters/{cluster_id}/modify`

## Сценарии

### 1. Негативный: docker 2.3.3 + tosAgent=true → 400

Baseline docker tag = `2.3.3` (< 2.4.0).

```bash
curl -s -X PATCH .../modify -H "Content-Type: application/json" -d @/tmp/modify_request_tos_agent.json
```

**Response 400**:
```json
{
  "error_message": "Validation failed",
  "errors": [{
    "field": "params.kafkaParams.tosAgent",
    "message": "Включение tosAgent требует docker-образ версии DockerTagVersion[major=2, minor=4, patch=0] или выше"
  }]
}
```

### 2. Позитивный: docker 2.4.0 + tosAgent=true → 202

Перед тестом повышаем docker-tag в БД:
```sql
UPDATE db_cluster_version
SET db_version = jsonb_set(db_version, '{dockers,0,dockerTag}', '"2.4.0"')
WHERE id=135287;
```

Тот же запрос → **Response 202 Accepted**. Operation `9b421e8d-...` запущена,
temporal workflow `modifyController → resize-controller → resize-controller_dc_1`
выполняется (та же цепочка что и обычный modify — tosAgent не меняет workflow, это
только валидация на стороне mdb-data).

## Подводные камни

1. **Draft версия**: после 202 в БД создаётся новый `db_cluster_version` со status=draft
   и пустым `db_version`. `getLatestClusterVersion` возвращает именно draft (ORDER BY
   update_ts DESC). Но валидатор вызывается ДО создания draft, так что видит
   предыдущую `done` версию с реальным `db_version`. После неудачной попытки
   надо удалить draft: `DELETE FROM db_cluster_version WHERE status='draft'`.
2. **Propagация в processing**: `tosAgent` идёт из request DTO через
   `KafkaClusterModificationMapper.toDto()` (поле `tosAgentEnabled` в `ModifyKafkaClusterDto`)
   → `ModifyKafkaClusterMapper.toUpdateBrokerConfig()` на стороне mdb-processing
   → поле `tosAgentEnabled` в `UpdateBrokerConfigInputData` (temporal workflow input,
   внутри `updateBrokerConfigData`). Подтверждено в workflow input:
   ```json
   "updateBrokerConfigData": {
     ...
     "heapSizeMB": 2048,
     "tosAgentEnabled": true,
     ...
   }
   ```
   Поле обрабатывается только если есть `brokerConfigDiff` (иначе `toUpdateBrokerConfig`
   возвращает null). На сторону контроллера/круиза не пробрасывается.

## Запуск инфраструктуры

- mdb-data: `./gradlew bootRun --args='--spring.profiles.active=local --server.port=8081'` (8081)
- mdb-processing: 8080
- temporal UI: http://localhost:8233
- pg: `docker exec pg_backstage_plugin_mdb psql -U dev -d backstage_plugin_mdb -c "..."`

## Cleanup

```sql
UPDATE operations SET status='canceled'
WHERE cluster_id='9e0336c7-50da-4487-8746-d332357180d3' AND status='in_progress';
DELETE FROM db_cluster_version
WHERE cluster_id='9e0336c7-50da-4487-8746-d332357180d3' AND status='draft';
-- восстановить docker tag обратно на 2.3.3
UPDATE db_cluster_version
SET db_version = jsonb_set(db_version, '{dockers,0,dockerTag}', '"2.3.3"')
WHERE id=135287;
```
