# MDBDEV-2162: Повторный прогон 2026-06-22 — новые изменения в temporal

Прогон `modifyKafkaCluster` после правок в темпорале. Инфраструктура поднималась с нуля, обнаружены проблемы с миграциями и seed-данными. Workflow стартует корректно, но спотыкается на wiremock для kc/zc.

## Кластер

Тот же, что в `MDBDEV-2162-modify-kafka-cluster.md`:
- **cluster_id**: `9e0336c7-50da-4487-8746-d332357180d3`
- **name**: `test-update-resize1`, type `kafka`

## Запуск инфраструктуры

mdb-data и mdb-processing уже работали (java-процессы на 8081 и 8080), но docker-инфраструктура была полностью поднята заново:

```bash
docker compose -f /Users/vl.ershov/Documents/Git/mdb-data/docker-compose.yml up -d
cd /Users/vl.ershov/Documents/Git/mdb-processing/localrun && docker compose up -d
```

Контейнеры: `pg_backstage_plugin_mdb`, `redis_sentinel`, `mdb-processing-temporal`, `mdb-processing-temporal-postgres`, `mdb-processing-vault`, `mdb-processing-wiremock`, `kafka-broker`, `kafka-controller`, `temporal-ui`, `localrun-kafdrop-1`.

## Проблема 1: Fresh pg теряет flyway-миграции

`docker-compose.yml` mdb-data не имеет volume для данных pg. После пересоздания контейнера pg поднимается только с `init_schema.sql`, который НЕ включает миграции V2–V7 (`shedlock`, `in_processing`, etc.).

mdb-data при этом уже запущен, и его flyway отработал на старом (стёртом) pg. На новый pg миграции не применятся автоматически — flyway не запускается повторно.

**Симптом**: `PATCH /modify` → HTTP 500:
```
column "in_processing" of relation "operations" does not exist
relation "shedlock" does not exist
```

**Фикс**: вручную применить V2–V4 на свежий pg:
```bash
for v in V2__shedlock V3__add_operation_status_tracking_columns V4__operations_alter_error_message_to_text; do
  docker cp /Users/vl.ershov/Documents/Git/mdb-data/src/main/resources/db/migration/${v}.sql \
    pg_backstage_plugin_mdb:/tmp/${v}.sql
  docker exec pg_backstage_plugin_mdb psql -U dev -d backstage_plugin_mdb -f /tmp/${v}.sql
done
```

Альтернатива: перезапустить mdb-data после старта pg — flyway применит миграции сам.

## Проблема 2: Preset 169 нужно INSERT, не UPDATE

В исходной истории (`MDBDEV-2162-modify-kafka-cluster.md`) только `UPDATE hardware_presets ... WHERE id=169`, но на свежем БД этого preset нет — `UPDATE 0`.

Добавить `INSERT ... ON CONFLICT DO UPDATE`:
```sql
INSERT INTO hardware_presets (id, name, type, vcores_count, ram_gb, is_active, database_preset)
VALUES (169, 'm.pico', 'memory_optimized', 2, 2, true,
  '{"mongodbPreset": {"defaults": {"intervalCommitMs": 10, "wtEngineCacheSizeGb": 16, "maxIncomingConnections": 200}, "maxValues": {"wtEngineCacheSizeGb": 20, "maxIncomingConnections": 400}, "minValues": {"intervalCommitMs": 1, "wtEngineCacheSizeGb": 1, "maxIncomingConnections": 10}}}'::jsonb)
ON CONFLICT (id) DO UPDATE SET name='m.pico', type='memory_optimized', vcores_count=2, ram_gb=2, is_active=true;
```

## Seed SQL

Полностью как в `MDBDEV-2162-modify-kafka-cluster.md`, но с фиксом preset 169 (см. выше). Файл: `/tmp/seed_2162.sql`.

Применять через `docker cp` + `psql -f` (не heredoc — тихо не применяет UPDATE).

## Modify request

Тот же, что в `MDBDEV-2162-modify-kafka-cluster.md`. Файл: `/tmp/modify_request_final.json`.

Endpoint: `PATCH http://localhost:8081/api/v2/mdb/kafka/clusters/9e0336c7-50da-4487-8746-d332357180d3/modify`

Ответ: `202 Accepted` (после фикса миграций).

## Результат в temporal

Workflow `modifyKafkaCluster` стартовал (workflowId `b855e3aa-7fb0-418d-bdff-0d793c96a532`), статус RUNNING. Дочерний `updateControllerConfig` тоже стартовал.

### Input workflow (декодирован)

```json
{
  "updateControllerConfigData": {
    "clusterId": "9e0336c7-50da-4487-8746-d332357180d3",
    "namespace": "infra",
    "pmsHostName": "test-update-resize1-mdbdev-kafka.clouds",
    "controllers": ["...dc.one-infra.ru", "...kc.one-infra.ru", "...zc.one-infra.ru"],
    "parameters": {"log.retention.hours": "168"},
    "forceUpdate": false,
    "workflowTtl": 10800
  },
  "updateBrokerConfigData": {
    "clusterId": "9e0336c7-50da-4487-8746-d332357180d3",
    "namespace": "infra",
    "brokers": ["...dc.one-infra.ru", "...kc.one-infra.ru", "...zc.one-infra.ru"],
    "parameters": {"auto.create.topics.enable": "true", "compression.type": "uncompressed"},
    "forceUpdate": false,
    "workflowTtl": 10800
  },
  "resizeData": {
    "namespace": "infra",
    "queue": "test-update-resize1-mdbdev-kafka",
    "brokerResources": {
      "dcs": ["dc", "kc", "zc"],
      "alloc": {"cores": "2", "mem": "2G", "lanIn": "15M", "lanOut": "25M"},
      "volumes": {"disks": [{"size": "8g", "type": "NVME", "name": "data", "durability": "persist"}]}
    },
    "workflowTtl": 14400
  },
  "order": "UPDATE_THEN_RESIZE",
  "workflowTtl": 14400
}
```

Все 3 diff'а присутствуют:
- `brokerConfigDiff` → `updateBrokerConfigData` с `compression.type=uncompressed`, `auto.create.topics.enable=true`
- `brokerResourcesDiff` → `resizeData.brokerResources.alloc`: cores 1→2, mem 4G→2G, lanIn 10M→15M, lanOut 20M→25M, preset 100→169
- `controllerConfigDiff` → `updateControllerConfigData` с `log.retention.hours=168`

## Проблема 3: updateControllerConfig падает на kc/zc

`updateControllerConfig` завершается FAILED: `Config reload failed for hosts: [kc, zc]`.

Wiremock в `mdb-processing/localrun/wiremock/` не отдаёт корректный ответ для reload config на контроллерах в kc/zc — только dc проходит. По логам mdb-processing виден цикл restart → wait → ping → restart для kc/zc, но в итоге workflow падает.

Родительский `modifyKafkaCluster` тоже падает по `CHILD_WORKFLOW_EXECUTION_FAILED`, до `updateBrokerConfig` и `resize` дело не доходит.

Это ограничение тестового wall, не баг кода. Чтобы пройти дальше — поправить stubs wiremock для kc/zc или мокировать `reloadKafkaDcConfig`/`ping` activity.

## Команды для проверки

```bash
# Список workflow
curl -s "http://localhost:8233/api/v1/namespaces/default/workflows?pageSize=15" | \
  jq -r '.executions[] | select(.startTime > "2026-06-22T15:00") | "\(.status) \(.type.name) \(.execution.workflowId)"'

# Декодировать input
WORKFLOW_ID="b855e3aa-7fb0-418d-bdff-0d793c96a532"
curl -s "http://localhost:8233/api/v1/namespaces/default/workflows/${WORKFLOW_ID}/history" | \
  jq -r '.history.events[0].workflowExecutionStartedEventAttributes.input.payloads[0].data' | \
  base64 -d | jq

# Ошибка упавшего workflow
WORKFLOW_ID="b855e3aa-7fb0-418d-bdff-0d793c96a532_update-controller-config"
curl -s "http://localhost:8233/api/v1/namespaces/default/workflows/${WORKFLOW_ID}/history" | \
  jq -r '.history.events[] | select(.eventType=="EVENT_TYPE_WORKFLOW_EXECUTION_FAILED") | .workflowExecutionFailedEventAttributes.failure.message'
```
