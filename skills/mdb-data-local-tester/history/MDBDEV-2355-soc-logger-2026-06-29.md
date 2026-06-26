# MDBDEV-2355: SOC logger — валидация и propagation в workflow (2026-06-29)

Новое поле `socLogger: @Nullable SocLoggerDto` в `ModifyKafkaClusterParams`. Валидация в
`KafkaClusterModificationValidator.validateSocLogger`. Проброс в workflow через
`KafkaClusterModificationMapper.toModifyKafkaClusterDto` → `ModifyKafkaClusterDto.socLogger` →
processing-side `ModifyKafkaClusterMapper.toInputData` → `ModifyKafkaClusterInputData.socLoggerData`.

## Кластер

- **cluster_id**: `9e0336c7-50da-4487-8746-d332357180d3` (name `test-update-resize1`, type `kafka`)
- Baseline `db_cluster_version` (status=done, id=135287):
  - `db_version.dockers[0]`: `{dockerTag: "2.3.3", dockerName: "ubuntu20-kafka-3.8.0", dockerType: "service"}`
  - hardware_preset_id=100 (ramGb=8)
  - В `cluster_params` добавлены пустые `kafkaParams.brokerConfig.config={}` и
    `kafkaParams.controller.controllerConfig.config={}` (иначе `KafkaClusterDiffDetector` падает с NPE).

## Валидатор

`KafkaClusterModificationValidator.validateSocLogger`:
- `socLogger == null` → ok
- `socLogger.enabled && currentDockerVersion < 2.3.3` → error `params.socLogger.enabled`:
  "Включение SOC logger требует docker-образ версии DockerTagVersion[major=2, minor=3, patch=3] или выше"
- `endpoint` пустой → error `params.socLogger.endpoint`: "endpoint должен содержать хотя бы один адрес"
- `endpoint` содержит blank-элемент → error `params.socLogger.endpoint`: "endpoint не должен содержать пустых значений"
- `passwordVaultPath` blank → error `params.socLogger.passwordVaultPath`
- `topic` blank → error `params.socLogger.topic`
- `user` blank → error `params.socLogger.user`

`SOC_LOGGER_MIN_DOCKER_VERSION = DockerTagVersion.of(2, 3, 3)` (ниже чем TOS_AGENT_MIN=2.4.0).

## Реальные значения SOC logger

```
passwordVaultPath: /zkv/dbs/logs-broker/kafka:soc-logs-password
endpoint: [
  "1.broker.kafka-queries-soc-mdb-kafka.uc.one-infra.ru:9092",
  "1.broker.kafka-queries-soc-mdb-kafka.pc.one-infra.ru:9092",
  "1.broker.kafka-queries-soc-mdb-kafka.kc.one-infra.ru:9092"
]
topic: soc-audit-log
user: soc-logs-robot
```

## Запрос (positive)

Файл: `/tmp/modify_request_soc_real.json`.

Endpoint: `PATCH http://localhost:8081/api/v2/mdb/kafka/clusters/9e0336c7-50da-4487-8746-d332357180d3/modify`

## Сценарии

### 1. Негативный: пустой endpoint → 400

`endpoint: []` → 400, error `params.socLogger.endpoint`: "endpoint должен содержать хотя бы один адрес".

### 2. Негативный: docker 2.3.2 + enabled → 400

Baseline docker tag понижен до `2.3.2`. Тот же запрос → 400, error `params.socLogger.enabled`:
"Включение SOC logger требует docker-образ версии DockerTagVersion[major=2, minor=3, patch=3] или выше".

### 3. Позитивный: docker 2.3.3 + valid socLogger → 202

Baseline docker tag = `2.3.3` (= SOC_LOGGER_MIN_DOCKER_VERSION). Запрос с реальными значениями → 202.
Operation запущена, temporal workflow `modifyKafkaCluster` выполняется.

Workflow input (декодированный):
```json
{
  "socLoggerData": {
    "enabled": true,
    "endpoint": [
      "1.broker.kafka-queries-soc-mdb-kafka.uc.one-infra.ru:9092",
      "1.broker.kafka-queries-soc-mdb-kafka.pc.one-infra.ru:9092",
      "1.broker.kafka-queries-soc-mdb-kafka.kc.one-infra.ru:9092"
    ],
    "passwordVaultPath": "/zkv/dbs/logs-broker/kafka:soc-logs-password",
    "topic": "soc-audit-log",
    "user": "soc-logs-robot"
  },
  "controllerOrder": "RESIZE_THEN_UPDATE",
  "brokerOrder": "RESIZE_THEN_UPDATE",
  "cruiseOrder": "UPDATE_THEN_RESIZE"
}
```

## Подводные камни

1. **BrokerConfig/ControllerConfig в baseline**: `KafkaClusterDiffDetector` вызывает
   `currentParams.brokerConfig().config()` и `currentParams.controller().controllerConfig().config()`
   без null-проверок. Если baseline `cluster_params` не содержит этих полей → NPE 500.
   Решение: добавить пустые `{"config": {}}` в baseline перед тестом.
2. **Draft версия**: после 202 в БД создаётся новый `db_cluster_version` со status=draft.
   После теста надо удалить draft: `DELETE FROM db_cluster_version WHERE status='draft'`.
3. **Docker tag для baseline**: для негативного SOC-сценария понизить docker tag в baseline:
   `UPDATE db_cluster_version SET db_version = jsonb_set(db_version, '{dockers,0,dockerTag}', '"2.3.2"') WHERE id=135287;`
4. **propagation в processing**: `socLogger` идёт из request DTO через `KafkaClusterModificationMapper.toDto()`
   → `ModifyKafkaClusterMapper.toInputData()` на стороне mdb-processing → поле `socLoggerData` в
   `ModifyKafkaClusterInputData` (temporal workflow input). Processing вызывает
   `KafkaPmsActivityImpl.upsertSocLoggerConfig(namespace, pmsHostName, socLogger)` для записи в PMS.

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
-- восстановить docker tag на 2.3.3 если меняли
UPDATE db_cluster_version
SET db_version = jsonb_set(db_version, '{dockers,0,dockerTag}', '"2.3.3"')
WHERE id=135287;
```
