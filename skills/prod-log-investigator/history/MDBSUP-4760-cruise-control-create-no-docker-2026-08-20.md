# MDBSUP-4760: Создание Cruise Control падает — No cruise-control docker found for dbVersion: 9

Симптом: операция создания Cruise Control (CREATE_ADDITIONAL_SERVICE) на Kafka-кластере падает с 500. В mdb-processing следов операции нет вообще.

## Кластер

- **cluster_id**: `80ab8038-f27e-43c5-86c0-8996a87b0f1b`
- **operation_id**: `33845091-0f7c-4ff5-b709-b23073e72bf0` (CREATE_ADDITIONAL_SERVICE, createdBy=denis.mikhaylov)
- **traceId**: `ab7feb02eea576a36b4b5aba0f77cf7f`

## Поиск ошибки

### Шаг 1: mdb-data логи

Кластер нашёлся grep'ом по cluster_id в `/mnt/logs/mdb-data.err.log` (хосты `1.mdb-data.mdb-data.{pc,uc,kc}`, в hc/kc mdb-data-хостов нет/timeout):

```
java.lang.IllegalStateException: No cruise-control docker found for dbVersion: 9
  at KafkaHostsServiceImpl.findCruiseDocker(KafkaHostsServiceImpl.java:392)
  at KafkaHostsServiceImpl.startCreateCruiseControl(KafkaHostsServiceImpl.java:361)
  at KafkaHostsController.createCruiseControl(KafkaHostsController.java:65)
```

Падает в САМОМ mdb-data до похода в processing (`processingClient.createCruiseControl` не вызывается) — поэтому в логах mdb-processing только sync-шум.

### Шаг 2: БД (backstage_plugin_mdb)

Живая таблица `db_version_dockers` для dbVersion 9 (kafka 3.8): docker `cruise-control` **ЕСТЬ**
(id=19, `ubuntu20-mdb-cruisecontrol-2.5.147:1.0.2`).

А вот `findCruiseDocker` читает НЕ живую таблицу, а JSON-снапшот из `db_cluster_version.db_version`
(`ClusterVersionMapper.toDbVersion(JsonNode)` → `treeToValue`). У кластера все 5 снапшотов
(от создания 2025-04-11 до draft 2026-08-17) содержат только `service` docker — кластер жил без CC.

### Шаг 3: кто пишет снапшот

- Modify-флоу копирует снапшот как есть: `KafkaClusterModificationServiceImpl:165` → `.dbVersion(currentVersion.getDbVersion())` — никогда не обновляется из живых таблиц.
- CC docker попадает в снапшот только из upgrade-workflow mdb-processing, и только `if (hasCruise(data))` (`UpgradeKafkaVersionWorkflowImpl:191`) — т.е. только если CC уже стоял.

**Парадокс бага:** у кластера без CC снапшот никогда не содержит CC docker → `findCruiseDocker`
гарантированно падает при создании CC. Живой источник (db_version_dockers) игнорируется.

## Фикс (mdb-data)

`KafkaHostsServiceImpl.findCruiseDocker`: снапшот остаётся первичным источником, добавлен
fallback в живую таблицу через `DbVersionDockerService.findFirstByDbVersionIdAndType(dbVersionId, DockerType.CRUISE_CONTROL)`
+ маппер `toDockerVersion(DbVersionDockerEntity)`. Инжект `DbVersionDockerService` (паттерн как в
`TemporalCreateClusterPreparer`). Хардкод `"cruise-control"` заменён на `DockerType.CRUISE_CONTROL.getValue()`.

Компиляция: `./gradlew compileJava` — OK.

## Уроки

1. **`db_cluster_version.db_version` (jsonb) — снапшот на момент операции, не источник правды.** Любой поиск «что доступно для версии» должен идти в живые таблицы (`db_versions` / `db_version_dockers`).
2. **Снапшот дополняется только при upgrade и только для уже установленных сервисов** — новый сервис (CC, exporter и т.п.) в снапшоте не появится сам.
3. **Нет следов операции в mdb-processing ≠ баг processing** — если до `processingClient.*` дело не дошло, ищи в mdb-data err.log до конца stacktrace (grep по cluster_id, `GlobalExceptionHandler - Unhandled exception: path=...`).
4. **Подключение к локальной БД плагина**: `127.0.0.1:53480`, user `backstage`, БД `backstage_plugin_mdb` (psql из `/opt/homebrew/opt/libpq/bin`).
