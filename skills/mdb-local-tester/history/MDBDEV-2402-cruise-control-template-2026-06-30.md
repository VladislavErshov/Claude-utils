# MDBDEV-2402: Cruise Control template rendering (no Jinja) — 2026-06-30

Полный отказ от Jinja-патчинга `kafka.cruisecontrol.properties`. Теперь конфиг
генерируется из шаблона `kafka-cruise-control-config.template` простой подстановкой
`${KEY}` → value. Рендерер удалён.

## Что изменилось

### mdb-processing

1. **`src/main/resources/kafka-cruise-control-config.template`** — шаблон переписан,
   удалены все `{% if %}`/`{% else %}`/`{% endif %}` и `$VAR` (без скобок). Остались
   только `${KEY}` плейсхолдеры:
   - `${BOOTSTRAP_SERVERS}` — список брокеров `:9092` через запятую
   - `${SECURITY_PROTOCOL}` — `SASL_SSL` или `SASL_PLAINTEXT`
   - `${AUTO_REBALANCE_ENABLED}` — `true`/`false`
   - `${SELF_HEALING_BROKER_FAILURE_ENABLED}` — `true`/`false`
   - `${REPLICATION_THROTTLE}` — байты или `Long.MAX_VALUE` (если `replicationThrottleMb == null`)

2. **`CruiseControlConfigRenderer.java`** — удалён (рендерер не нужен).

3. **`UpdateKafkaCruiseConfigInputData`** — добавлено поле `List<String> brokers`
   (не nullable). Если `brokers` пустой — `UpdateKafkaCruiseConfigInputData` = null.

4. **`ModifyKafkaClusterMapper.toUpdateCruiseConfigData`** (processing) —
   возвращает null если `cruiseControl == null || brokers == null || brokers.isEmpty()`.

5. **`KafkaPmsActivity.upsertCruiseControlConfig`** — сигнатура изменена:
   ```java
   void upsertCruiseControlConfig(Namespace, String pmsHostName,
       @Nullable CruiseControlDto cruiseControl, List<String> brokers);
   ```

6. **`KafkaPmsActivityImpl.upsertCruiseControlConfig`** — переписан:
   - загружает шаблон через `getClassLoader().getResourceAsStream(...)`
   - читает `kafka.ssl.enabled` из PMS для `SECURITY_PROTOCOL`
   - подставляет значения через `String.replace("${KEY}", value)`
   - `replicationThrottleMb == null` → `${REPLICATION_THROTTLE} = String.valueOf(Long.MAX_VALUE)`
     (= `9223372036854775807` байт, фактически unthrottled, строка остаётся в конфиге)
   - `autoRebalanceOnBrokerFailEnabled == null` → `false`
   - удалены `replaceCruiseProperty`/`removeCruiseProperty` и regex-паттерны

7. **`UpdateKafkaCruiseConfigWorkflowImpl`** — передаёт `data.brokers()` в активити.

8. **Wiremock**: добавлен `get-kafka-ssl-enabled.json` — возвращает `"true"` для
   `kafka.ssl.enabled` на хосте `test-update-resize1-mdbdev-kafka.clouds`.

### mdb-data

Без изменений — `brokerHostnames` уже собираются в
`KafkaClusterModificationServiceImpl.buildProcessingDto` и передаются в
`ModifyKafkaClusterDto.brokers` через маппер (строка 91).

## Кластер

- **cluster_id**: `9e0336c7-50da-4487-8746-d332357180d3` (name `test-update-resize1`)
- **dc**: `dc`
- **cruise-инстанс**: `1.cruise.test-update-resize1-mdbdev-kafka.dc.one-infra.ru` (реальный, доступен по ssh)

## Запросы

### Сценарий 1: throttle = 10 Mb

Файл: `/tmp/modify_cruise_only.json`. PATCH `/api/v2/mdb/kafka/clusters/{id}/modify`.

Cruise-only (brokerConfig.config пустой, `cruiseControlDiff` триггерится через
`KafkaClusterDiffDetector.hasCruiseControlDiff`).

```json
"cruiseControl": {
  "cruiseControlDc": "dc",
  "autoRebalanceEnabled": true,
  "autoRebalanceOnBrokerFailEnabled": true,
  "replicationThrottleMb": 10
}
```

### Сценарий 2: throttle = null

Файл: `/tmp/modify_cruise_throttle_null.json`. Тот же запрос, но:

```json
"cruiseControl": {
  "cruiseControlDc": "dc",
  "autoRebalanceEnabled": true,
  "autoRebalanceOnBrokerFailEnabled": true,
  "replicationThrottleMb": null
}
```

## Результат в temporal

### Сценарий 1 (throttle = 10 Mb)

operation id `cf12a7d4-bee7-470d-a8fa-cbc69503cff0`. Все workflow `COMPLETED`.

Workflow `updateCruiseConfig` input (декодирован):
```json
{
  "namespace": "infra",
  "pmsHostName": "test-update-resize1-mdbdev-kafka.clouds",
  "queue": "test-update-resize1-mdbdev-kafka",
  "clusterId": "9e0336c7-50da-4487-8746-d332357180d3",
  "cruiseControl": {
    "dc": "dc",
    "autoRebalanceEnabled": true,
    "autoRebalanceOnBrokerFailEnabled": true,
    "replicationThrottleMb": 10,
    "jvmHeapSizeMb": null
  },
  "brokers": [
    "1.broker.test-update-resize1-mdbdev-kafka.dc.one-infra.ru",
    "1.broker.test-update-resize1-mdbdev-kafka.kc.one-infra.ru",
    "1.broker.test-update-resize1-mdbdev-kafka.zc.one-infra.ru"
  ],
  "forceUpdate": false,
  "workflowTtl": 3600
}
```

### Сценарий 2 (throttle = null)

operation id `bbb08b52-ef0d-4187-9f7c-7ab5a9e93e69`. Все workflow `COMPLETED`:
- `modifyKafkaCluster` → COMPLETED
- `modifyCruise` → COMPLETED
- `updateCruiseConfig` → COMPLETED (включая reload реального cruise-инстанса)

Лог processing:
```
Applied Cruise Control config to PMS for host test-update-resize1-mdbdev-kafka.clouds:
  auto rebalance=true, auto rebalance on broker failed=true, replication throttle=null Mb
```

## Сгенерированный конфиг (PMS)

Ключевые строки, проверены через временный отладочный лог в `KafkaPmsActivityImpl`:

```
bootstrap.servers=1.broker.test-update-resize1-mdbdev-kafka.dc.one-infra.ru:9092,1.broker.test-update-resize1-mdbdev-kafka.kc.one-infra.ru:9092,1.broker.test-update-resize1-mdbdev-kafka.zc.one-infra.ru:9092
security.protocol=SASL_SSL
default.replication.throttle=9223372036854775807   # Long.MAX_VALUE для throttle=null
self.healing.enabled=true
self.healing.broker.failure.enabled=true
self.healing.goal.violation.enabled=true
```

Конфиг не содержит `{%` и нераскрытых `$KEY`.

## PMS в local-профиле

`application-local.yaml`: `pms.baseUrl: https://pms.cloud.vk.team` — реальный PMS.
Поэтому тело POST-запроса не видно в wiremock. Для проверки рендеринга временно
добавлял `log.info("Rendered cruise control template:\n{}", rendered)` в
`KafkaPmsActivityImpl.upsertCruiseControlConfig`, после проверки убрал.

Рендеринг также покрыт unit-тестом
`KafkaPmsActivityImplTest.upsertCruiseControlConfig_shouldPatchProperties`.

## Unit-тест

`KafkaPmsActivityImplTest.upsertCruiseControlConfig_shouldPatchProperties`:
мокает `kafka.ssl.enabled="true"`, передаёт 2 брокера, проверяет что результат:
- содержит `bootstrap.servers=1.broker.test.cloud.ru:9092,2.broker.test.cloud.ru:9092`
- содержит `security.protocol=SASL_SSL`
- содержит `self.healing.enabled=true`
- содержит `self.healing.broker.failure.enabled=true`
- содержит `self.healing.goal.violation.enabled=true`
- содержит `default.replication.throttle=268435456` (1024*1024*256)
- не содержит `{%` и нераскрытых `$KEY`

## Cleanup между прогонами

```sql
DELETE FROM operations WHERE cluster_id='9e0336c7-50da-4487-8746-d332357180d3';
DELETE FROM db_cluster_version WHERE cluster_id='9e0336c7-50da-4487-8746-d332357180d3'
  AND status IN ('draft','failed','need_retry','scheduled');
```

## Подводные камни

1. **`KafkaClusterDiffDetector.hasCruiseControlDiff`** — добавлен, иначе cruise-only
   PATCH вернёт 400 "No Kafka cluster changes found".
2. **`brokers` обязательно не пустой** — иначе `toUpdateCruiseConfigData` вернёт null
   и cruise update не запустится. В mdb-data `brokerHostnames` всегда собираются из БД.
3. **mdb-data жёстко привязан к порту 8081** из-за конфликта с processing на 8080.
4. **Exit 137 у mdb-data** — если запускать `./gradlew bootRun` в nohup и случайно
   убить связанный процесс, может умереть. Просто перезапустить.
5. **PMS-запрос не виден в wiremock** — local-профиль указывает на реальный
   `pms.cloud.vk.team`. Для проверки тела — временный `log.info` в активити.
6. **Reload cruise-инстанса требует реального хоста** — `KafkaHostWaiter` делает
   ssh-ping. В локальном тесте без реального cruise-инстанса workflow падает с
   `RELOAD_FAILED`. С реальным хостом (как в сценарии 2) — проходит до `COMPLETED`.
