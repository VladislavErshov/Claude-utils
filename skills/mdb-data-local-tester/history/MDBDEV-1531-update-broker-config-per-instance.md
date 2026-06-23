# MDBDEV-1531: Use one workflow for one instance while update broker configs

Прогон `modifyKafkaCluster` после рефакторинга `updateBrokerConfig` в mdb-processing. Главное изменение: вместо одного workflow `reloadKafkaDcConfig` на DC теперь запускается по одному workflow `reloadKafkaBrokerInstance` на каждый broker-инстанс.

## Кластер

Тот же, что в `MDBDEV-2162-modify-kafka-cluster.md`:
- **cluster_id**: `9e0336c7-50da-4487-8746-d332357180d3`
- **name**: `test-update-resize1`, type `kafka`

## Запуск инфраструктуры

```bash
# pg + redis
docker compose -f /Users/vl.ershov/Documents/Git/mdb-data/docker-compose.yml up -d
# temporal + vault + kafka + wiremock
cd /Users/vl.ershov/Documents/Git/mdb-processing/localrun && docker compose up -d

# миграции V2–V4 (fresh pg)
for v in V2__shedlock V3__add_operation_status_tracking_columns V4__operations_alter_error_message_to_text; do
  docker cp /Users/vl.ershov/Documents/Git/mdb-data/src/main/resources/db/migration/${v}.sql pg_backstage_plugin_mdb:/tmp/${v}.sql
  docker exec pg_backstage_plugin_mdb psql -U dev -d backstage_plugin_mdb -f /tmp/${v}.sql
done

# seed (см. MDBDEV-2162-modify-kafka-cluster.md) + preset 169 INSERT

# java
./gradlew bootRun --args='--spring.profiles.active=local --server.port=8081' > /tmp/mdb-data.log 2>&1 &
cd /Users/vl.ershov/Documents/Git/mdb-processing && \
  BOOT_RUN_JVM_ARGS="--add-opens java.base/java.lang=ALL-UNNAMED" \
  ./gradlew bootRun --args='--spring.profiles.active=local' > /tmp/mdb-processing.log 2>&1 &
```

## Modify request — только broker config diff

Файл: `/tmp/modify_request_broker_only.json`.

Отличия от baseline (`db_cluster_version` id=135287):
- `brokerConfig.config`: `{}` → `{"auto.create.topics.enable": "true", "compression.type": "uncompressed"}` — DIFF
- `controllerConfig.config`: `{}` — SAME (нет controller diff)
- `lanIn: 10`, `lanOut: 20`, `hardwarePresetId: 100` — SAME (нет resize diff)

```json
{
  "params": {
    "acl": {},
    "name": "test-update-resize1",
    "isWan": false,
    "lanIn": 10,
    "diskGb": 8,
    "lanOut": 20,
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
        "controllerConfig": {"config": {}}
      },
      "cruiseControl": {"cruiseControlDc": "dc", "cruiseUserPassword": ""},
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
  "hardwarePresetId": 100,
  "hosts": [{"dc": "dc"}, {"dc": "kc"}, {"dc": "zc"}],
  "attempts": 3
}
```

Endpoint: `PATCH http://localhost:8081/api/v2/mdb/kafka/clusters/9e0336c7-50da-4487-8746-d332357180d3/modify`
Ответ: `202 Accepted`.

## Результат в temporal

WorkflowId родителя: `a6162d42-230c-4626-832b-292bc1f6785b`

Все workflow COMPLETED:
```
COMPLETED modifyKafkaCluster              a6162d42-...-...-...-...
COMPLETED updateBrokerConfig              a6162d42-..._update-broker-config
COMPLETED reloadKafkaBrokerInstance       a6162d42-..._update-broker-config_dc_1
COMPLETED reloadKafkaBrokerInstance       a6162d42-..._update-broker-config_kc_1
COMPLETED reloadKafkaBrokerInstance       a6162d42-..._update-broker-config_zc_1
```

### Input modifyKafkaCluster (декодирован)

```json
{
  "updateControllerConfigData": null,
  "updateBrokerConfigData": {
    "clusterId": "9e0336c7-50da-4487-8746-d332357180d3",
    "namespace": "infra",
    "pmsHostName": "test-update-resize1-mdbdev-kafka.clouds",
    "brokers": [
      "1.broker.test-update-resize1-mdbdev-kafka.dc.one-infra.ru",
      "1.broker.test-update-resize1-mdbdev-kafka.kc.one-infra.ru",
      "1.broker.test-update-resize1-mdbdev-kafka.zc.one-infra.ru"
    ],
    "parameters": {
      "auto.create.topics.enable": "true",
      "compression.type": "uncompressed"
    },
    "forceUpdate": false,
    "workflowTtl": 10800
  },
  "resizeData": null,
  "order": "UPDATE_THEN_RESIZE",
  "workflowTtl": 14400
}
```

`updateControllerConfigData` и `resizeData` — `null`, то есть diff-детектор отработал корректно: только broker config.

## Что проверяем (MDBDEV-1531)

Раньше (см. `MDBDEV-2162-modify-kafka-cluster.md`) `updateBrokerConfig` запускал **один** дочерний workflow `reloadKafkaDcConfig` на каждый DC, внутри которого последовательно перезагружались broker'ы этого DC, с паузой 60с между DC.

Теперь `updateBrokerConfig` запускает **один** workflow `reloadKafkaBrokerInstance` на каждый broker-инстанс. WorkflowId имеют суффикс `_<dc>_<n>` (`_dc_1`, `_kc_1`, `_zc_1`), что подтверждает переключение на per-instance модель.

В этом прогоне все три per-instance workflow завершились COMPLETED — то есть новая логика работает: один workflow на один инстанс, без падений, без зависимости от wiremock-покрытия DC (что было проблемой в прогоне 2026-06-22 для controller config).

## Команды для проверки

```bash
# Список workflow этого прогона
curl -s "http://localhost:8233/api/v1/namespaces/default/workflows?pageSize=30" | \
  jq -r '.executions[] | select(.execution.workflowId | startswith("a6162d42")) | "\(.status) \(.type.name) \(.execution.workflowId)"'

# Декодировать input родителя
WORKFLOW_ID="a6162d42-230c-4626-832b-292bc1f6785b"
curl -s "http://localhost:8233/api/v1/namespaces/default/workflows/${WORKFLOW_ID}/history" | \
  jq -r '.history.events[0].workflowExecutionStartedEventAttributes.input.payloads[0].data' | \
  base64 -d | jq
```
