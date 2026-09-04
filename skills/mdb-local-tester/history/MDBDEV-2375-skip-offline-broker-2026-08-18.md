# MDBDEV-2375: skip offline Kafka hosts in config-update reload

Прогон `UpdateConfigKafkaBrokerWorkflow` на реальном dev-кластере `test-modify3` после фикса
`KafkaHostReloadHelper.filterOutOfflineHosts` — воркфлоу не должен падать, если один брокер
лежит в облаке (state не в `{STARTING, RUNNING}`).

## Что проверяем

Фикс в `KafkaHostReloadHelper.executeReloadCycle` (общий для broker/controller/cruise) перед
циклом reload вызывает приватный `filterOutOfflineHosts`, который отсеивает хосты с
`state` не в `EnumSet.of(STARTING, RUNNING)` в `skippedHosts` и логирует warning. PMS-конфиг
уже апсёртнут — лежащий брокер применит его при следующем старте. Без фильтра цикл виснет в
ожидании хоста, который `KafkaIteratePolicy.takeNext` никогда не выберет, и воркфлоу падает
по таймауту.

Аналог `ClickHouseInstanceUtils.filterOutOfflineInstances` — restartable-состояния те же
(`{STARTING, RUNNING}`).

## Кластер

- **cluster_id**: `9fc47c1b-011d-4aaa-b411-de5345a0204e`
- **name**: `test-modify3`, type `kafka`, project `mdbdev` (160), namespace `infra` (2)
- **queue**: `test-modify3-mdbdev-kafka`
- **brokers**: 3 (dc, hc, kc) — в проде `1.broker.test-modify3-mdbdev-kafka.dc.one-infra.ru`
  лежит (state != RUNNING), hc и kc — RUNNING+PREFAIL.
- **hardware_preset_id**: 100 (m.pico)

## Запуск инфраструктуры

```bash
# уже поднято:
#   pg_backstage_plugin_mdb (6434), redis_sentinel (26379)
#   temporal-ui (8233), mdb-processing-temporal (7233), mdb-processing-wiremock (8088)
#   mdb-data на 8081, mdb-processing на 8080

# после изменения кода в KafkaHostReloadHelper — перезапустить mdb-processing:
lsof -ti:8080 | xargs -r kill -9
cd /Users/vl.ershov/Documents/Git/mdb-processing && \
  BOOT_RUN_JVM_ARGS="--add-opens java.base/java.lang=ALL-UNNAMED" \
  ./gradlew bootRun --args='--spring.profiles.active=local' > /tmp/mdb-processing.log 2>&1 &
```

## db-seed

SQL для удалённой БД (выполнить через `mcc ssh` + `psql` на проде):

```sql
SELECT jsonb_build_object(
  'db_cluster', (SELECT json_agg(t) FROM (SELECT * FROM db_cluster WHERE id='9fc47c1b-011d-4aaa-b411-de5345a0204e') t),
  'db_cluster_version', (SELECT json_agg(t ORDER BY create_ts DESC) FROM (SELECT * FROM db_cluster_version WHERE cluster_id='9fc47c1b-011d-4aaa-b411-de5345a0204e' ORDER BY create_ts DESC LIMIT 3) t),
  'host_state', (SELECT json_agg(t) FROM (SELECT * FROM host_state WHERE cluster_id='9fc47c1b-011d-4aaa-b411-de5345a0204e') t),
  'one_cloud_meta', (SELECT jsonb_agg(to_jsonb(t) ORDER BY t.params_type) FROM (SELECT * FROM one_cloud_meta WHERE cluster_id='9fc47c1b-011d-4aaa-b411-de5345a0204e') t),
  'projects', (SELECT json_agg(t) FROM (SELECT p.* FROM projects p JOIN db_cluster c ON c.project_id=p.id WHERE c.id='9fc47c1b-011d-4aaa-b411-de5345a0204e') t),
  'namespaces', (SELECT json_agg(t) FROM (SELECT n.* FROM namespaces n JOIN db_cluster c ON c.namespace_id=n.id WHERE c.id='9fc47c1b-011d-4aaa-b411-de5345a0204e') t),
  'hardware_presets', (SELECT json_agg(t) FROM (SELECT hp.* FROM hardware_presets hp WHERE hp.id IN (SELECT DISTINCT hardware_preset_id FROM db_cluster_version WHERE cluster_id='9fc47c1b-011d-4aaa-b411-de5345a0204e')) t)
);
```

Применение через Python-генератор (см. `/tmp/seed/gen.py`): читает JSON, строит DELETE + INSERT
для `operations`, `db_cluster_version`, `host_state`, `one_cloud_meta` + UPSERT для `db_cluster`.
Затем:

```bash
docker cp /tmp/seed/seed_9fc47c1b.sql pg_backstage_plugin_mdb:/tmp/seed.sql
docker exec pg_backstage_plugin_mdb psql -U dev -d backstage_plugin_mdb -f /tmp/seed.sql
```

## Подготовка baseline

В проде `db_cluster_version.cluster_params.kafkaParams.brokerConfig = null` и
`controller.controllerConfig = null` — `KafkaClusterDiffDetector` падает с NPE. Перед modify
патчим baseline (id=216838) — ставим пустые `{}`:

```sql
UPDATE db_cluster_version SET cluster_params = jsonb_set(jsonb_set(
  cluster_params,
  '{kafkaParams,brokerConfig}', '{"config":{}}'::jsonb, true),
  '{kafkaParams,controller,controllerConfig}', '{"config":{}}'::jsonb, true
) WHERE id = 216838;
```

## Modify request — только brokerConfig diff

Файл: `/tmp/seed/modify_request_broker_only.json`. Отличия от baseline:
- `brokerConfig.config`: `{}` → `{"auto.create.topics.enable": "true", "compression.type": "uncompressed"}` — DIFF
- `controllerConfig.config`: `{}` — SAME
- `lanIn/lanOut: 10/10`, `hardwarePresetId: 100`, `jvmHeapSizeMb: 1024` — SAME

```json
{
  "params": {
    "acl": {},
    "name": "test-modify3",
    "isWan": false,
    "lanIn": 10,
    "diskGb": 8,
    "lanOut": 10,
    "diskType": "nvme",
    "projectId": 160,
    "rootQueue": "prod",
    "kafkaParams": {
      "controller": {
        "controllerDcs": ["dc","hc","kc"],
        "controllerLanIn": 10,
        "controllerMemGb": 2,
        "controllerConfig": {"config": {}},
        "controllerDiskGb": 10,
        "controllerLanOut": 10,
        "controllerVcores": 2,
        "controllerDiskType": "nvme",
        "controllerJvmHeapSizeMb": 1025
      },
      "cruiseControl": {
        "cruiseControlDc": "ic",
        "cruiseUserPassword": "",
        "autoRebalanceEnabled": true,
        "autoRebalanceOnBrokerFailEnabled": true,
        "replicationThrottleMb": 5
      },
      "brokerConfig": {
        "config": {
          "auto.create.topics.enable": "true",
          "compression.type": "uncompressed"
        }
      },
      "jvmHeapSizeMb": 1024,
      "tosAgent": true,
      "socLogger": {"enabled": false}
    },
    "needLanIpv6": true,
    "needWanIpv4": false,
    "needWanIpv6": false
  },
  "hardwarePresetId": 100,
  "isNeedShards": false,
  "hosts": [{"dc":"dc"},{"dc":"hc"},{"dc":"kc"}],
  "type": "update_instances",
  "attempts": 3
}
```

Endpoint: `PATCH http://localhost:8081/api/v2/mdb/kafka/clusters/9fc47c1b-011d-4aaa-b411-de5345a0204e/modify`
Ответ: `202 Accepted`.

## Результат в temporal

WorkflowId родителя: `eeb69c8b-6524-4d58-bc51-5e4960fe4bf1`

```
COMPLETED modifyKafkaCluster              eeb69c8b-...
COMPLETED modifyKafkaController           eeb69c8b-..._modify-controller
COMPLETED modifyKafkaBroker               eeb69c8b-..._modify-broker
COMPLETED updateConfigKafkaBroker         eeb69c8b-..._modify-broker_update-broker-config
```

## Логи mdb-processing (фикс в действии)

```
INFO  UpdateConfigKafkaBrokerWorkflowImpl - Starting Kafka config update for cluster 9fc47c1b-... in DCs [dc, hc, kc]
INFO  KafkaPmsActivityImpl - Successfully applied 2 and removed 11 Kafka broker config parameters into PMS property kafka.broker.properties for host test-modify3-mdbdev-kafka.clouds. Preserved 41 blacklisted params.
WARN  KafkaHostReloadHelper - broker reload: skipping 1 non-running hosts (config will apply on their next start): [1.broker.test-modify3-mdbdev-kafka.dc.one-infra.ru]
INFO  KafkaHostReloadHelper - Starting broker reload of 2 hosts (1 skipped) with max concurrency 1
```

- `skipping 1 non-running hosts` — `filterOutOfflineHosts` отработал, dc-брокер пропущен.
- `Starting broker reload of 2 hosts` — цикл пошёл по restartable-хостам (hc, kc).

## Грабли: PREFAIL + forceUpdate=false

Первый прогон (без `forceUpdate`) завис: hc и kc в состоянии `RUNNING+PREFAIL`, а
`KafkaIteratePolicy.takeNext` при `forceUpdate=false` фильтрует PREFAIL через
`forceUpdate || info.availability() != PREFAIL`. В цикле `getHostsToReload` возвращал пустой
батч, и воркфлоу крутился `Pausing PT15S` до `workflowTtl`.

Декодированный `cloud_getInfoForInstances` result подтвердил:
```
"1.broker.test-modify3-mdbdev-kafka.kc.one-infra.ru": {"availability":"PREFAIL","state":"RUNNING",...}
"1.broker.test-modify3-mdbdev-kafka.hc.one-infra.ru": {"availability":"PREFAIL","state":"RUNNING",...}
```

Фикс: повторить modify-запрос с `forceUpdate=true` (параметр проходит в
`UpdateConfigKafkaBrokerRequest.forceUpdate` через `updateBrokerConfigData`). После этого
`takeNext` берёт PREFAIL-хосты, и workflow завершается COMPLETED.

Это **pre-existing behavior** `KafkaIteratePolicy`, не связано с таской MDBDEV-2375. Если
нужно, чтобы PREFAIL-хосты так же skip-ились (как offline) — отдельная задача.

## Команды для проверки

```bash
# Список workflow прогона
curl -s "http://localhost:8233/api/v1/namespaces/default/workflows?pageSize=20" | \
  jq -r '.executions[] | select(.execution.workflowId | startswith("eeb69c8b")) | "\(.status) \(.type.name) \(.execution.workflowId)"'

# Декодировать input updateConfigKafkaBroker
WID="eeb69c8b-6524-4d58-bc51-5e4960fe4bf1_modify-broker_update-broker-config"
curl -s "http://localhost:8233/api/v1/namespaces/default/workflows/${WID}/history" | \
  jq -r '.history.events[0].workflowExecutionStartedEventAttributes.input.payloads[0].data' | \
  base64 -d | jq

# Декодировать result cloud_getInfoForInstances (availability/state)
curl -s "http://localhost:8233/api/v1/namespaces/default/workflows/${WID}/history?pageSize=50" | \
  jq -r '.history.events[] | select(.eventType=="EVENT_TYPE_ACTIVITY_TASK_COMPLETED") | .activityTaskCompletedEventAttributes.result.payloads[0].data' | \
  tail -1 | base64 -d | jq '.[].availability, .[].state'
```

## Вывод

Фикс `filterOutOfflineHosts` работает на реальном прод-кластере: лежащий брокер (state !=
RUNNING/STARTING) пропускается, PMS-конфиг применится при его следующем старте, остальные
брокеры рестартуются. Для PREFAIL-хостов требуется `forceUpdate=true` — это отдельная
поведенческая особенность `KafkaIteratePolicy`.
