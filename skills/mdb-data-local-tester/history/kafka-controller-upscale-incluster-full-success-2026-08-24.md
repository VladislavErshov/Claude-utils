# Kafka controller upscale InCluster/InDc — полный успешный прогон — 2026-08-24

Финальная архитектура ветки MDBDEV-3180: parent `upscaleKafkaControllerInCluster` (pms → children → reload → save) + child `upscaleKafkaControllerInDc`. Кластер test-modify3 (`9fc47c1b-011d-4aaa-b411-de5345a0204e`), upscale контроллера в `ic`.

## Прогон 1 (упал на save): нашёл дыру в wiremock

Local-профиль mdb-processing: `external.api...mdb-data.base-url: http://localhost:8088` (wiremock).
Активность `saveUpscaledKafkaControllersInfo` идёт в mdb-data internal API
`POST /internal/api/v2/mdb/kafka/clusters/{id}/hosts/save/upscaled-controllers` — стаба не было → 404 `Request was not matched`.

**Фикс — стаб в wiremock** (переживает рестарт контейнера, т.к. файл в mappings):
```bash
cat > /tmp/save-upscaled-controllers.json <<'EOF'
{"request":{"method":"POST","urlPathPattern":"/internal/api/v2/mdb/kafka/clusters/[^/]+/hosts/save/upscaled-controllers"},"response":{"status":200}}
EOF
docker cp /tmp/save-upscaled-controllers.json mdb-processing-wiremock:/home/wiremock/mappings/mdbdata/save-upscaled-controllers.json
curl -X POST http://localhost:8088/__admin/mappings/reset
```

## Прогон 2: WORKFLOW_EXECUTION_STATUS_COMPLETED

После заливки стаба: `DELETE FROM operations WHERE cluster_id='...'` → повторный POST `?dc=ic` → 202.

Temporal input (новый контракт): `queueInfo {queueShortName: test-modify3-mdbdev-kafka, pmsHost, namespace}`, `controllersPerDc {"ic":1}`, `brokerDcs [dc,hc,kc]`, `connectionParams {kafkaBrokerHosts, vaultPasswordPath}`, `workflowTtl 10800s` (3ч).

Фазы parent (по activity): `cloud_getInfosForServices` → `upsertKafkaLayout` → `upsertControllerQuorum` → child → `updateConfigKafkaBroker` (child) → `updateConfigKafkaController` (child) → `saveUpscaledKafkaControllersInfo` (200 от стаба) → COMPLETED.

Child `upscaleKafkaControllerInDc`: COMPLETED, лог `Controller service in DC ic already has 1 replicas, nothing to submit` — идемпотентность: сервис создан прогоном 1, повторного submit нет.

## PMS после

- `kafka.controller.quorum` — 5 voter, дублей нет (union-merge сработал; uc/ic — остатки прошлых тестов)
- `kafka.layout` — `dc,hc,kc,ic,zc,uc`

## Замечания

- Хост в `host_state` локальной БД не появился (save ушёл в wiremock-стаб, не в живой mdb-data). Для проверки записи — направить mdb-data base-url на localhost:8081 или дергать живой internal API напрямую.
- `restartHosts` больше не делает leader discovery — порядок перезапуска контроллеров обеспечивает `updateConfigKafkaController` child.
