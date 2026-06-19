# MDBDEV-2162 (re-run 2026-06-20): modifyKafkaCluster e2e повтор

Повторный прогон теста из `MDBDEV-2162-modify-kafka-cluster.md` на актуальном main. Цель — убедиться, что цепочка Backstage → mdb-data → Temporal/Processing для `modify_kafka_cluster` всё ещё работает после изменений в коде.

## Кластер

- **cluster_id**: `9e0336c7-50da-4487-8746-d332357180d3` (`test-update-resize1`, project=mdbdev id=160, namespace=infra id=2)
- **baseline version**: 135287 (`status=done, type=new`)
- **новая version**: 135289 (`update_instances, status=scheduled → done`)

## Что отличается от прошлого прогона

1. **Сидов не было в обеих БД** — пришлось заново применить `/tmp/seed_kafka_cluster.sql` + `/tmp/seed_extras.sql` (mongodbPreset для preset 100, services_auth для JWT) в `postgres` (6432) и `pg_backstage_plugin_mdb` (6434).
2. **Структура cluster_params в DTO создания версии** — корневые `brokerConfig`/`controllerConfig` больше не работают: mdb-data падает с NPE `Cannot invoke "ModifyKafkaControllerParams.controllerDcs()" because "ModifyKafkaParams.controller()" is null`. Нужно вкладывать controller/brokerConfig/cruiseControl **внутрь `kafkaParams`** (см. `/tmp/request.json`).
3. **diskType обязательный** — валидатор 422 `Нельзя изменить тип диска при изменении кластера`, если `diskType` не передан или отличается от baseline. У baseline `nvme` → в request.json `"diskType":"nvme","diskGb":8`.

## Запуск инфраструктуры

1. `/setup-local-backstage` — `docker compose up -d` в `backstage/stubs` + `CREATE DATABASE pg_boss` + `yarn mdb-dev` (~70с до `Listening on :7007`)
2. `/setup-local-mdb-data` — `docker compose up -d` в `mdb-data` (redis_sentinel упадёт на 26379 — это OK, stubs-sentinel-1 уже там) + `./gradlew bootRun --args='--spring.profiles.active=local --server.port=8081'` (~5с до UP на :8081)
3. `/setup-local-temporal` — `./localrun.sh` в `mdb-processing/localrun` (temporal:7233, vault:8200, kafka:29092, wiremock:8088, temporal-ui:8233) + `gradlew bootRun --args='--spring.profiles.active=local'` для processing на :8080 (~75с до UP)

Конфиг `app-config.mdb.local.yaml` уже корректный: `mdb-processing.host: 'http://127.0.0.1:8080'`, `mdb-data.host: 'http://localhost:8081'`.

## Сидирование (обе БД одновременно)

```bash
for db in postgres pg_backstage_plugin_mdb; do
  docker cp /tmp/seed_kafka_cluster.sql $db:/tmp/seed.sql
  docker exec $db psql -U dev -d backstage_plugin_mdb -f /tmp/seed.sql
  docker cp /tmp/seed_extras.sql $db:/tmp/seed_extras.sql
  docker exec $db psql -U dev -d backstage_plugin_mdb -f /tmp/seed_extras.sql
done
```

`/tmp/seed_extras.sql`:
```sql
UPDATE hardware_presets
SET database_preset = '{"mongodbPreset":{"defaults":{...},"maxValues":{...},"minValues":{...}}}'::jsonb
WHERE id=100;

INSERT INTO services_auth (name, project_id, access_type)
VALUES ('local-tester', 160, 'w')
ON CONFLICT (name, project_id) DO UPDATE SET access_type='w';
```

После сида — **обязательный рестарт Backstage** (Redis-кеш проектов/PMS-конфиг строятся при boot):
```bash
pkill -f "mdb-app start\|mdb-backend start\|backstage-cli.*mdb"
yarn mdb-dev > /tmp/backstage.log 2>&1 &
```

## /tmp/request.json (рабочая структура)

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
    "lanIn": 10, "lanOut": 10,
    "diskType": "nvme", "diskGb": 8
  },
  "hardwarePresetId": 169,
  "hosts": [{"dc":"dc"},{"dc":"kc"},{"dc":"zc"}],
  "type": "update_instances",
  "attempts": 3,
  "isNeedShards": false
}
```

**Ключевое**: `controller` (с `controllerDcs`/`controllerConfig`), `brokerConfig`, `cruiseControl` — **внутри `kafkaParams`**, не в корне. Иначе `ModifyKafkaClusterModificationValidator.validateControllerDcs` (mdb-data) упадёт с NPE.

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

HTTP 200 → `{status:"ok", createdClusterVersion:{id:135289, status:"scheduled"}}`.

## Обход PMS

PMS-конфиг для namespace=infra указывает на prod (`stg.one-conf-web.devdc.odkl.ru`) → 400. PMS-таска падает в `need_retry`, `modify_kafka_cluster` не стартует.

```sql
-- task id=4 в этом прогоне (id=1 был в прошлом)
UPDATE tasks SET status='done',
    result='{"status":"done","result":{"data":"Generated 5 settings"}}'::jsonb,
    finished_ts=now(), updated_at=now()
WHERE type='update_kafka_pms_settings'
  AND operation_id IN (
    SELECT id FROM operations
    WHERE cluster_id='9e0336c7-50da-4487-8746-d332357180d3' AND status<>'canceled'
  );
```

Через ~10с (cadence PT10S) `modify_kafka_cluster` подхватывается, `ModifyKafkaClusterTaskProcessor` делает PATCH в mdb-data, тот через mdb-processing стартует workflow.

## Результат

Backstage tasks (operation 9cd19aac-b776-4269-886f-c5b8aa2463af):
```
 4 | update_kafka_pms_settings | done   | Generated 5 settings
 5 | modify_kafka_cluster      | done   | {"data":{"clusterId":"9e0336c7…"}}
 6 | finish_task               | done   | {"data":true}
```

Temporal workflows (operationId 9cd19aac-b776-4269-886f-c5b8aa2463af):
```
12:01:56 RUNNING modifyKafkaCluster       9cd19aac-b776-4269-886f-c5b8aa2463af
12:01:56 RUNNING updateControllerConfig   9cd19aac-b776-4269-886f-c5b8aa2463af_update-controller-config
```

`updateControllerConfig` активно работает — processing пингует `1.controller.test-update-resize1-mdbdev-kafka.dc.one-infra.ru` (реальный cloud-хост не отвечает, `OCI runtime error` — ожидаемо в локальном тесте). Workflow long-running, но это уже не часть `modify_kafka_cluster` task — его работа (стартовать workflow) выполнена.

Цепочка работает: Backstage → mdb-data (PATCH /api/v2/mdb/kafka/clusters/{id}/modify) → mdb-processing → Temporal.

## Подводные камни (новые в этом прогоне)

1. **NPE в mdb-data без `kafkaParams.controller`** — `KafkaClusterModificationValidator.validateControllerDcs` (mdb-data/src/main/java/.../KafkaClusterModificationValidator.java:57) deref `kafkaParams.controller().controllerDcs()`. Controller **обязательно** внутри `kafkaParams`, не в корне clusterParams. Поля `controllerConfig`/`brokerConfig` в корне DTO игнорируются Jackson'ом при десериализации в `ModifyKafkaClusterParams`.
2. **`diskType` обязателен в request.json** — если не передать, 422 `Нельзя изменить тип диска при изменении кластера`. Берётся из baseline (`nvme`).
3. **Сидов не было** — после `docker compose down` в прошлой сессии БД остались, но данные пропали (вquoted `docker ps` показывал 0 containers перед стартом). Проверять count до теста, не предполагать наличие.
4. **PMS task id** меняется между прогонами (1, 4, …) — искать по `type='update_kafka_pms_settings'` + `cluster_id`, не хардкодить id.

## Повторный запуск

```sql
UPDATE operations SET status='canceled'
WHERE cluster_id='9e0336c7-50da-4487-8746-d332357180d3'
  AND status IN ('in_progress','failed','scheduled','need_retry');

DELETE FROM db_cluster_version
WHERE cluster_id='9e0336c7-50da-4487-8746-d332357180d3'
  AND status IN ('draft','scheduled','failed','need_retry');
-- baseline version 135287 (status=done) НЕ трогать
```
