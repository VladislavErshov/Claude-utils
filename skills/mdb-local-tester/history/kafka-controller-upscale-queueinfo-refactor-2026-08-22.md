# Kafka controller upscale: рефакторинг на QueueInfo + controllersPerDc (parent/child) — 2026-08-22

Ветка mdb-processing `ershov/MDBDEV-3180-Kafka]-make-controller-upscale-idempotent-on-retry`.
Кластер `9fc47c1b-011d-4aaa-b411-de5345a0204e` (test-modify3, kafka, project 160 mdbdev, ns infra).

## Что проверялось

`POST /api/v2/mdb/kafka/clusters/{id}/hosts/controllers?dc=ic` после рефакторинга
`UpscaleKafkaController` на схему брокерного upscale: родитель + параллельные child по ДЦ,
input на `QueueInfo` + `Map<String, Integer> controllersPerDc`.

## Изменения

- processing api: `UpscaleKafkaControllerDto` → `operationId, controllersPerDc, queueInfo(QueueInfoDto), controllerDcs, brokerDcs, hardwarePreset, kafkaConnectionParams` (убраны namespace/cloud/queue/fullQueue/replicas).
- processing: `UpscaleKafkaControllerRequest` на `queueInfo`+`controllersPerDc`; новый child `UpscaleKafkaControllerDcWorkflow` (очередь, pending-quorum, идемпотентный деплой, delete pending); parent — discovery, kafka.layout, Async-children + PARTIAL_UPSCALE_FAILURE, quorum, leader, reloadBrokers, restarts, save.
- mdb-data: `KafkaHostsServiceImpl.upscaleKafkaController` строит `Map.of(dc, currentInDc+1)` + `buildQueueInfo(params, project, namespace)`; mapper toDto без cloud/queue/fullQueue/replicas.
- docs/kafka/upscale-controller.md переписан под новую архитектуру.

## Найденный баг (исправлен в этой сессии)

Первый прогон ветки упал с NPE: mdb-data не отправлял `replicas` (не было в mapper),
новый workflow делал unbox null → `Cannot invoke Integer.intValue()`. Фикс — переход
на `controllersPerDc`, вычисляемый в mdb-data как текущее число контроллеров в ДЦ + 1.

## Грабли: mdb-data собирается против processing-api из nexus

`build.gradle` mdb-data: `implementation 'one.cloud.mdb:processing-api:3.50.0'`.
Локальные изменения api mdb-processing НЕ подхватываются, пока не опубликовать в mavenLocal:

```bash
CI_COMMIT_TAG=v3.50.0 ./gradlew :api:publishToMavenLocal   # в mdb-processing
# затем ./gradlew clean compileJava в mdb-data (mavenLocal() уже первый в repositories)
```

Признак старого api: MapStruct warning `Unmapped target properties: "cloud, queue, fullQueue, replicas"`.

## Прогон (dc=ic, retry-сценарий — controller в ic уже создан первым прогоном)

Temporal input: `queueInfo {queueName, queueShortName, pmsHost, namespace=infra}`, `controllersPerDc {"ic":1}`, `controllerDcs [dc,hc,kc]`.

- parent: `cloud_getInfosForServices` → `cloud_getInfoForInstances` → `upsertKafkaLayout` → child → `upsertControllerQuorum` → `kafka_host_getLeaderId` (известный local-SSL блокер).
- child `upscaleKafkaControllerInDc` (..._ic): COMPLETED, лог `Controller service in DC ic already has 1 replicas, nothing to submit` — идемпотентность нового child подтверждена.
- PMS после: `kafka.controller.quorum` = 5 voter (dc/hc/kc/uc/ic), дублей нет; `kafka.layout` без изменений.

## 409 при повторном POST

`Already has active or failed operation` — лечится `DELETE FROM operations WHERE cluster_id='...'` (или дождаться завершения).

## Команды

```bash
curl -X POST "http://localhost:8081/api/v2/mdb/kafka/clusters/9fc47c1b-011d-4aaa-b411-de5345a0204e/hosts/controllers?dc=ic"
# child: curl temporal ...workflows?query=WorkflowType='upscaleKafkaControllerInDc'
```
