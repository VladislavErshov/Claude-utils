# MDBDEV-2882: CreateKafkaCruiseWorkflow — 2026-08-06

Обновление воркфлоу `CreateCruiseControlWorkflowImpl.createCruiseControl` после
правок: убран `restartCruiseInstance`, добавлен `ensureCruiseQueueExists`,
переставлены шаги. Протестировано на `cruiseControlDc=ic` — очередь в ic
создалась через `submitQueueIfNeeded`, workflow прошёл до конца.

## Что делает workflow (актуальный flow)

`CreateCruiseControlWorkflowImpl.createCruiseControl(CreateCruiseControlRequest)`:

1. **discoverBrokerHosts** — `KafkaHostReloadHelper.discoverKafkaHosts(cloud,
   namespace, queue, brokerDcs, "broker")` → FQDN брокеров для
   `bootstrap.servers` в cruise properties.
2. **PMS-настройки cruise-control** на хост `<queue>.clouds` (5 activity):
   - `upsertCruiseControlLogConfig` — log4j
   - `upsertCruiseControlConfig` — properties (с brokerHosts)
   - `upsertCruiseControlCapacity` — capacity.json (diskGb/lanInMb/lanOutMb)
   - `upsertCruiseControlJaas` — jaas
   - `upsertCruiseControlSysconfig` — sysconfig (jvmHeapSizeMb)
3. **Pyvault certs** на хост `<serviceName>.<queue>.clouds`:
   - `upsertCruiseControlPyvaultConf(namespace, certsHost, namespaceDomain, isWan)`
4. **Vault-секрет** пользователя `cruise`:
   - `KafkaCruiseVaultActivity.createCruiseUserSecret(namespace, projectName, fullQueue, password)`
   - путь: `zkv/mdb/<projectName>/kafka/<fullQueue>/cruise`
5. **ensureCruiseQueueExists** — создать cloud-очередь в `cruiseControlDc`,
   если её ещё нет:
   - `cloud.submitQueueIfNeeded(sourceDc=brokerDcs[0], targetDc=cruiseControlDc, namespace, fullQueue)`
   - при создании — `CloudWaiter.waitQueueRunning(cruiseControlDc, namespace, fullQueue, 10min)`
6. **submitCruiseService** — рендер и сабмит манифеста cruise-сервиса:
   - `KafkaCruiseManifestActivity.renderCruiseServiceManifest(CruiseManifest)`
   - `CloudActivity.submitServiceManifest(cruiseControlDc, namespace, fullQueue, replicas=1, manifest)`
   - force-submit=true → повторный сабмит безопасен
7. **enableBrokerCruiseMetrics** — включить `metric.reporters` в
   `kafka.broker.properties`:
   - `pmsActivity.addBrokerProperties(namespace, pmsHost, {"metric.reporters": "...CruiseControlMetricsReporter"})`
8. **reloadBrokersWithCruiseMetrics** — child-workflow
   `UpdateKafkaBrokerConfigWorkflow` (через `runIgnoringAlreadyStarted`).
9. **waitServiceRunning** — `CloudWaiter.waitServiceRunning(submitted, 10min)`.
   **Стоит в самом конце** — пока cruise стартует в cloud, мы успеваем сделать
   reload брокеров, не тратя время на отдельное ожидание.

**Удалено** (было в предыдущей версии): `restartCruiseInstance` + `discoverCruiseHost`
+ `KafkaHostActivity.restartCruiseInstanceSsh` + `KafkaHostWaiter.waitSshRestartedCruiseInstanceReady`
+ `CloudWaiter.waitHostsRunning`. Первый запуск cruise-инстанса выходит
готовым — отдельный SSH-рестарт не нужен.

## Кластер

- **cluster_id**: `96777c6a-52c5-40db-8e4c-2d7f824301f6` (name `test-43version-4`)
- **namespace**: `infra`
- **queue**: `test-43version-4-mdbdev-kafka`
- **fullQueue**: `test-43version-4-mdbdev-kafka.mdbdev.db.production.mdb.prod`
- **pmsHostName**: `test-43version-4-mdbdev-kafka.clouds`
- **projectName**: `mdbdev`
- **brokerDcs** (3 DC): `hc`, `kc`, `pc` — в любом из них очередь `fullQueue`
  существует
- **cruiseControlDc**: `ic` — очередь в ic **не существовала** на момент запуска,
  `ensureCruiseQueueExists` создал её копированием манифеста из `hc` (первый
  brokerDc)
- **cruise serviceName**: `cruise.test-43version-4-mdbdev-kafka`
- **cruiseUserPassword**: `mcYsZ6psAch4wZNJ`
- **docker**: `ubuntu20-mdb-cruisecontrol-2.5.147` / `1.0.2`
  (НЕ `kafka`/`2.4.0` как в предыдущей истории — это образ именно cruise-control)

## Запуск workflow

Запускается напрямую через temporal client (mdb-data modify-эндпоинт этот flow
пока не дёргает — нет кодогенерации API).

```bash
docker cp /tmp/cruise-request.json mdb-processing-temporal:/tmp/cruise-request.json
docker exec mdb-processing-temporal sh -c "tctl --address 172.19.0.5:7233 workflow run \
  --taskqueue kafka-activities-queue \
  --workflow_type createCruiseControl \
  --workflow_id cruise-create-ic-<timestamp> \
  --execution_timeout 3600 \
  --input_file /tmp/cruise-request.json"
```

`CreateCruiseControlRequest` (24 поля, см. record + тест
`CreateCruiseControlWorkflowImplTest.request()`):

```json
{
  "clusterId": "96777c6a-52c5-40db-8e4c-2d7f824301f6",
  "namespace": "INFRA",
  "queue": "test-43version-4-mdbdev-kafka",
  "fullQueue": "test-43version-4-mdbdev-kafka.mdbdev.db.production.mdb.prod",
  "rootQueue": "prod",
  "projectName": "mdbdev",
  "pmsHostName": "test-43version-4-mdbdev-kafka.clouds",
  "certsHostName": "cruise.test-43version-4-mdbdev-kafka.clouds",
  "serviceName": "cruise.test-43version-4-mdbdev-kafka",
  "cruiseControlDc": "ic",
  "namespaceDomain": "mdb",
  "isWan": false,
  "cruiseControl": {
    "dc": "ic",
    "autoRebalanceEnabled": true,
    "jvmHeapSizeMb": 2048
  },
  "brokerDcs": ["hc", "kc", "pc"],
  "brokerParameters": {"num.io.threads": "8"},
  "brokerDiskGb": 100,
  "brokerLanInMb": 100,
  "brokerLanOutMb": 200,
  "dockerName": "ubuntu20-mdb-cruisecontrol-2.5.147",
  "dockerTag": "1.0.2",
  "cruiseUserPassword": "mcYsZ6psAch4wZNJ",
  "workflowTtl": "PT1H"
}
```

**Важно про `namespace`**: в JSON передаём `"INFRA"` (uppercase), не `"infra"`.
Temporal-сериализатор `Namespace` expects uppercase enum value. Со строчным
`"infra"` workflow падает на первом activity с ошибкой десериализации.

**Важно про `workflowTtl`**: сериализуется как ISO-8601 строка `"PT1H"`, не
как число секунд. В предыдущей истории передавали `3600` — это была другая
версия codec'а.

**Важно про `brokerDcs`** (не `brokerHosts`!): после rebase на master поле
переименовано с `brokerHosts` (List<FQDN>) на `brokerDcs` (List<DC name>).
Workflow сам discover'ит FQDN через `cloud.getInfosForServices`.

## Результат в temporal

Все workflow `COMPLETED`. Порядок activity в `createCruiseControl`:

```
cloud:getInfosForServices(discoverBrokerHosts)
pms:upsertCruiseControlLogConfig
pms:upsertCruiseControlConfig
pms:upsertCruiseControlCapacity
pms:upsertCruiseControlJaas
pms:upsertCruiseControlSysconfig
pms:upsertCruiseControlPyvaultConf       # на certsHost
vault:createCruiseUserSecret
cloud:submitQueueIfNeeded                 # hc → ic, очередь создана
cloud:getQueueState (waitQueueRunning)
manifest:renderCruiseServiceManifest
cloud:submitServiceManifest               # в DC ic
pms:addBrokerProperties                   # metric.reporters=CruiseControlMetricsReporter
[child: updateBrokerConfig → 3 reloadKafkaBrokerInstance для hc/kc/pc]
cloud:getServiceInfo (waitServiceRunning) # в самом конце
```

**Ключевое отличие от предыдущего прогона**: шаг `cloud:submitQueueIfNeeded`
отработал — очередь `test-43version-4-mdbdev-kafka.mdbdev.db.production.mdb.prod`
скопирована из `hc` в `ic`, `waitQueueRunning` дождался state=RUNNING.
Раньше (до правок) на `cruiseControlDc=ic` workflow падал с
`404 Queue ... not found` на `submitServiceManifest`.

Также **нет** шагов `restartCruiseInstanceSsh` / `pingSshRestartedCruiseInstanceReady`
/ `getInfoForInstances` — они убраны из workflow целиком.

## Rebase на master (2026-08-06)

Сделан rebase на `origin/master` (HEAD `6fa191ea MDBDEV-2949 fix kafka status ping`).
Конфликты в 6 файлах:

1. **`KafkaPmsActivity.java`** — javadoc для `addBrokerProperties`. Master
   добавил базовый javadoc, branch — расширенный с описанием `{% if false %}`-
   блоков. Оставили branch version (более полная).

2. **`KafkaPmsActivityImpl.java`** — сигнатура `addBrokerProperties`. Master —
   однострочник, branch — многострочник с `final var`. Оставили branch version.

3. **`KafkaPropertiesUtil.java`** — master добавил `appendProperties` + 
   `enableTemplateBlocks` (из MDBDEV-2955), branch добавил их же из MDBDEV-2882.
   Полный дубликат. Оставили branch version (там `final var`, `private` modifier,
   javadoc). Проверить, что master version не сломал ничего — `KafkaPmsActivityImplTest`
   проходит.

4. **`application.yaml`** / **`application-test.yaml`** — master добавил
   `kafka-upgrade-version-workflow` (MDBDEV-2815), branch добавил
   `kafka-create-cruise-control-workflow`. Оставили оба.

5. **`ModifyKafkaClusterWorkflowImplTest.java`** — master добавил mock для
   `getControllerQuorum`, branch добавил mock'и для cruise-control методов.
   Оставили оба.

`./gradlew test --tests ...ModifyKafkaClusterWorkflowImplTest --tests
...CreateCruiseControlWorkflowImplTest checkstyleMain checkstyleTest` — green.

## Подводные камни

1. **`namespace` в JSON — uppercase `"INFRA"`**, не `"infra"`. Temporal
   payload codec для `Namespace` enum требует uppercase. Со строчным значением
   workflow падает на первом activity.

2. **`workflowTtl` — ISO-8601 строка `"PT1H"`**, не число `3600`. В предыдущей
   истории было число — это была другая версия codec'а.

3. **`brokerDcs` (не `brokerHosts`)** — список DC имён (`["hc","kc","pc"]`),
   не FQDN. После rebase на master.

4. **Docker image для cruise-control** — `ubuntu20-mdb-cruisecontrol-2.5.147`
   / `1.0.2`. Это **не** образ `kafka`/`2.4.0` (тот для broker). Если
   перепутать — cruise-сервис упадёт на старте.

5. **`cruiseControlDc` может быть любым DC** — даже тем, где очереди ещё нет.
   `ensureCruiseQueueExists` скопирует манифест очереди из `brokerDcs.getFirst()`
   (где очередь точно существует, т.к. там живут брокеры) и дождётся
   state=RUNNING. Раньше (до правок) на DC без очереди падали с 404.

6. **Повторный сабмит сервиса безопасен** — `submitServiceManifest` использует
   `force-submit=true` (последний аргумент `cloudService.submitService(..., true)`
   в `CloudActivityImpl.java:426`). Можно перезапускать workflow с тем же
   `workflow_id` (с `WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE`) — пересабмит
   существующего cruise-сервиса не падает, `waitServiceRunning` корректно
   отрабатывает на уже запущенном сервисе.

7. **`local` профиль пишет в РЕАЛЬНЫЙ PMS** (`pms.cloud.vk.team`) и ходит к
   РЕАЛЬНЫМ cloud masters. mTLS-сертификат из `~/.mccloud/` работает. Тестировать
   только на dev-кластерах (mdbdev, project 160). Никогда не запускать на проде.

8. **Vault `zkv` должен быть включён вручную**:
   ```bash
   docker exec mdb-processing-vault vault secrets list | grep zkv
   # если нет:
   docker exec mdb-processing-vault vault secrets enable -path=zkv kv-v2
   ```

9. **Restart processing после смены кода** — `bootRun` не hot-reload'ит
   изменения workflow. После правок:
   ```bash
   lsof -ti:8080 | xargs -r kill -9
   BOOT_RUN_JVM_ARGS="--add-opens java.base/java.lang=ALL-UNNAMED" \
     /Users/vl.ershov/Documents/Git/mdb-processing/gradlew -p /Users/vl.ershov/Documents/Git/mdb-processing \
     bootRun --args='--spring.profiles.active=local' > /tmp/mdb-processing.log 2>&1 &
   ```

## Ссылки

- Workflow: `src/main/java/one/cloud/mdb/processing/kafka/workflow/create/cruise/CreateCruiseControlWorkflowImpl.java`
- Request record: `src/main/java/one/cloud/mdb/processing/kafka/model/create/cruise/CreateCruiseControlRequest.java`
- Тест: `src/test/java/one/cloud/mdb/processing/kafka/workflow/create/cruise/CreateCruiseControlWorkflowImplTest.java`
- Дока: `docs/kafka/create-cruise-control.md`
- Vault activity: `KafkaCruiseVaultActivity` / `KafkaCruiseVaultActivityImpl`
- Manifest activity: `KafkaCruiseManifestActivity` / `KafkaCruiseManifestActivityImpl`
- Предыдущая история: `MDBDEV-2882-create-cruise-control-2026-08-05.md`
