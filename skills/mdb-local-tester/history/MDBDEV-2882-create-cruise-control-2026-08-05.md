# MDBDEV-2882: CreateKafkaCruiseWorkflow — 2026-08-05

Новый workflow `CreateKafkaCruiseWorkflowImpl.createCruiseControl` — порт
backstage-цепочки `generateTasksForCreateCruiseControl` в mdb-processing.
Запускается (пока) напрямую через temporal client, не через mdb-data
modify-эндпоинт.

## Что делает workflow

`CreateKafkaCruiseWorkflowImpl.createCruiseControl(CreateKafkaCruiseRequest)`:

1. **PMS-настройки cruise-control** на хост `<queue>.clouds` (5 activity):
   - `upsertCruiseControlLogConfig` — log4j
   - `upsertCruiseControlConfig` — properties (с brokerHosts)
   - `upsertCruiseControlCapacity` — capacity.json (diskGb/lanInMb/lanOutMb)
   - `upsertCruiseControlJaas` — jaas
   - `upsertCruiseControlSysconfig` — sysconfig (jvmHeapSizeMb)
2. **Pyvault certs** на хост `<serviceName>.<queue>.clouds`:
   - `upsertCruiseControlPyvaultConf(namespace, certsHost, namespaceDomain, isWan)`
3. **Vault-секрет** пользователя `cruise`:
   - `KafkaCruiseVaultActivity.createCruiseUserSecret(namespace, projectName, fullQueue, password)`
   - путь: `zkv/mdb/<projectName>/kafka/<fullQueue>/cruise`
4. **Broker metrics reporter** (включение `CruiseControlMetricsReporter` в
   `kafka.broker.properties`):
   - `pmsActivity.addBrokerProperties(namespace, pmsHost, {"metric.reporters": "...CruiseControlMetricsReporter"})`
5. **Reload брокеров** через child-workflow `UpdateKafkaBrokerConfigWorkflow`:
   - `UpdateBrokerConfigInputData` собирается с `queue` + `dcs` (master-формат),
     DCs извлекаются из FQDN broker-hosts через `HostUtil.parseCloudFromHost`
   - `ChildWorkflowUtils.runIgnoringAlreadyStarted` — идемпотентность
6. **Submit cruise service manifest** в `cruiseControlDc`:
   - `KafkaCruiseManifestActivity.renderCruiseServiceManifest(CruiseManifest)`
   - `CloudActivity.submitServiceManifest(dc, namespace, fullQueue, replicas=1, manifest)`
7. **Wait service running** — `CloudWaiter.waitServiceRunning(submitted, 10min)`
8. **Restart cruise instance**:
   - `discoverCruiseHost` через `cloud.getServiceInfo(dc, namespace, serviceName)` →
     `instances[0].hostname`
   - `kafkaActivity.restartCruiseInstanceSsh(host)` (SSH-exec на хост)
   - `KafkaHostWaiter.waitSshRestartedCruiseInstanceReady(dc, host, 10min)`
   - `CloudWaiter.waitHostsRunning([host], 10min)`

DI: 3 `ActivityOptions` бина (`pmsActivityOptions`, `cloudActivityOptions`,
`kafkaActivityOptions`). Vault/manifest activity используют `kafkaActivityOptions`
— `vaultActivityOptions`/`manifestActivityOptions` **не определены** в
`ActivityOptionsFactory`, поэтому конструктор workflow обязан принимать ровно 3
бина.

## Кластер

- **cluster_id**: `96777c6a-52c5-40db-8e4c-2d7f824301f6` (name `test-43version-4`)
- **namespace**: `infra`
- **queue**: `test-43version-4-mdbdev-kafka`
- **fullQueue**: `test-43version-4-mdbdev-kafka.mdbdev.db.production.mdb.prod`
- **pmsHostName**: `test-43version-4-mdbdev-kafka.clouds`
- **projectName**: `mdbdev`
- **brokerHosts** (3 DC): `hc`, `kc`, `pc`
- **cruiseControlDc**: `kc` (сначала пробовали `ic` — очередь не существует,
  см. "Подводные камни" #2)
- **cruise serviceName**: `cruise.test-43version-4-mdbdev-kafka`
- **cruiseUserPassword**: `mcYsZ6psAch4wZNJ`
- **docker**: `kafka` / `2.4.0` (для tos-agent/socLogger toggle-чеков, но в этом
  flow не валидируется — валидация в mdb-data `KafkaClusterModificationValidator`,
  а мы запускали workflow напрямую)

## Запуск workflow

Запускается напрямую через temporal client (mdb-data modify-эндпоинт этот flow
пока не дёргает — нет кодогенерации API). Скрипт:

```bash
docker exec mdb-processing-temporal sh -c 'tctl --address 172.21.0.5:7233 \
  workflow run \
  --taskqueue kafka-activities-queue \
  --workflow_type createCruiseControl \
  --workflow_id 15a69cd1-9cda-45f1-89eb-0b8fece103ca \
  --execution_timeout 3600 \
  --input <base64-encoded-payload>'
```

`CreateKafkaCruiseRequest` (record, конструктор — 24 поля, см. тест
`CreateKafkaCruiseWorkflowImplTest.request()`):

```json
{
  "clusterId": "96777c6a-52c5-40db-8e4c-2d7f824301f6",
  "namespace": "infra",
  "queue": "test-43version-4-mdbdev-kafka",
  "fullQueue": "test-43version-4-mdbdev-kafka.mdbdev.db.production.mdb.prod",
  "namespaceDomain": "mdb",
  "projectName": "mdbdev",
  "pmsHostName": "test-43version-4-mdbdev-kafka.clouds",
  "certsHostName": "cruise.test-43version-4-mdbdev-kafka.clouds",
  "serviceName": "cruise.test-43version-4-mdbdev-kafka",
  "cruiseControlDc": "kc",
  "rootQueue": "prod",
  "isWan": false,
  "cruiseControl": {
    "dc": "kc",
    "autoRebalanceEnabled": true,
    "jvmHeapSizeMb": 2048
  },
  "brokerHosts": [
    "1.broker.test-43version-4-mdbdev-kafka.hc.one-infra.ru",
    "1.broker.test-43version-4-mdbdev-kafka.kc.one-infra.ru",
    "1.broker.test-43version-4-mdbdev-kafka.pc.one-infra.ru"
  ],
  "brokerParameters": {"num.io.threads": "8"},
  "brokerDiskGb": 100,
  "brokerLanInMb": 100,
  "brokerLanOutMb": 200,
  "dockerName": "kafka",
  "dockerTag": "2.4.0",
  "cruiseUserPassword": "mcYsZ6psAch4wZNJ",
  "workflowTtl": 3600
}
```

## Результат в temporal

workflow_id `15a69cd1-9cda-45f1-89eb-0b8fece103ca`. Все workflow `COMPLETED`:

- `reloadKafkaBrokerInstance ..._hc_1` — COMPLETED
- `reloadKafkaBrokerInstance ..._kc_1` — COMPLETED
- `reloadKafkaBrokerInstance ..._pc_1` — COMPLETED
- `updateBrokerConfig` (child) — COMPLETED
- `createCruiseControl` (parent) — COMPLETED ✅

Activities в `createCruiseControl` (порядок по event id):

```
pms:upsertCruiseControlLogConfig
pms:upsertCruiseControlConfig
pms:upsertCruiseControlCapacity
pms:upsertCruiseControlJaas
pms:upsertCruiseControlSysconfig
pms:upsertCruiseControlPyvaultConf       # на certsHost
vault:createCruiseUserSecret
pms:addBrokerProperties                  # metric.reporters=CruiseControlMetricsReporter
[child: updateBrokerConfig → 3 reloadKafkaBrokerInstance]
manifest:renderCruiseServiceManifest
cloud:submitServiceManifest              # в DC kc
cloud:getServiceInfo (waitServiceRunning)
cloud:getServiceInfo (discoverCruiseHost)
kafka:restartCruiseInstanceSsh
kafka:pingSshRestartedCruiseInstanceReady
cloud:getInfoForInstances (waitHostsRunning)
```

Была одна retry-итерация на event 77 (activity failed → timer backoff →
reschedule → completed на event 88). Это нормальное retry-поведение temporal
для transient-ошибок (скорее всего cloud API timeout на `submitServiceManifest`
или `getServiceInfo`).

## Rebase на master

Во время тестирования сделали rebase на master. Конфликты в 3 файлах:

1. **`UpdateKafkaCruiseConfigInputData`** — master рефакторил `brokers` →
   `brokerDcs` (DCs, не hosts), branch добавлял `diskGb`→`brokerDiskGb`/
   `lanInMb`→`brokerLanInMb`/`lanOutMb`→`brokerLanOutMb`. Разрешили объединением:
   ```java
   public record UpdateKafkaCruiseConfigInputData(
       Namespace namespace,
       String pmsHostName,
       String queue,
       List<String> brokerDcs,           // master
       UUID clusterId,
       CruiseControlDto cruiseControl,
       @Nullable Long brokerDiskGb,      // branch
       @Nullable Long brokerLanInMb,     // branch
       @Nullable Long brokerLanOutMb,    // branch
       boolean forceUpdate,
       Duration workflowTtl
   ) {}
   ```

2. **`ModifyKafkaClusterMapper.toUpdateCruiseConfigData`** — master использует
   `toServiceResources(dto.brokerResources()).dcs()` вместо старого
   `dto.brokerResources().dcs()`. Branch переименовал поля. Разрешили:
   ```java
   final var brokerResources = toServiceResources(dto.brokerResources());
   if (cruiseControl == null || brokerResources == null) {
     return null;
   }
   return new UpdateKafkaCruiseConfigInputData(
       dto.namespace(),
       dto.pmsHostName(),
       dto.queue(),
       brokerResources.dcs(),            // master
       clusterId,
       cruiseControl,
       extractDiskGb(dto.brokerResources()).orElse(null),  // branch
       extractLanInMb(dto.brokerResources()).orElse(null), // branch
       extractLanOutMb(dto.brokerResources()).orElse(null),// branch
       Boolean.TRUE.equals(dto.forceUpdate()),
       Objects.requireNonNullElse(dto.workflowTtl(), Duration.ofHours(1))
   );
   ```

3. **`CreateKafkaCruiseWorkflowImpl.reloadBrokersWithCruiseMetrics`** — master
   изменил конструктор `UpdateBrokerConfigInputData` (добавил `queue` + `dcs`
   вместо неявного извлечения). Исправили:
   ```java
   final var brokerDcs = request.brokerHosts().stream()
       .map(HostUtil::parseCloudFromHost)
       .distinct()
       .toList();
   final var data = new UpdateBrokerConfigInputData(
       request.clusterId(),
       request.namespace(),
       request.pmsHostName(),
       request.queue(),
       brokerDcs,
       request.brokerParameters(),
       null,           // heapSizeMB
       null,           // tosAgentEnabled
       false,          // forceUpdate
       request.workflowTtl()
   );
   ```

4. **`CreateKafkaCruiseWorkflowImplTest`** —
   - `new CreateKafkaCruiseWorkflowImpl(OPTIONS, OPTIONS, OPTIONS)` (3 бина
     вместо 5)
   - `CloudServiceInfo` конструктор: master добавил `List<ContainerInfo>` 6-м
     аргументом → `new CloudServiceInfo(SERVICE_NAME, CloudState.RUNNING, null, null, List.of(runningCruiseInstance()), List.of())`

5. **`ModifyKafkaClusterMapperTest`** — conflict в
   `toInputData_shouldReturnNullCruiseConfigWhenBrokerResourcesNull`:
   master-логика возвращает `null` целиком (через `toServiceResources == null`),
   branch-логика возвращала `UpdateKafkaCruiseConfigInputData` с null-полями.
   Оставили master-логику (`assertThat(inputData.cruiseUpdateConfigData()).isNull()`).

`./gradlew check` — green.

## Подводные камни

1. **DI: 3 бина, не 5**. `CreateKafkaCruiseWorkflowImpl` конструктор принимает
   ровно 3 `ActivityOptions`: `pmsActivityOptions`, `cloudActivityOptions`,
   `kafkaActivityOptions`. Vault и manifest activity **переиспользуют**
   `kafkaActivityOptions` — не нужно создавать отдельные бины. Если добавить
   `vaultActivityOptions`/`manifestActivityOptions` в конструктор →
   `NoUniqueBeanDefinitionException` (их нет в `ActivityOptionsFactory`).

2. **cruiseControlDc должен быть DC, где существует cloud-очередь**. Пробовали
   `ic` → `submitServiceManifest` упал с `404 Queue ... not found` (очередь
   `test-43version-4-mdbdev-kafka.mdbdev.db.production.mdb.prod` не существует
   в ic). Переключились на `kc` — очередь есть, submit прошёл. Проверить
   наличие очереди в DC можно через `cloud.getServiceInfo` или
   `cloud.getInfosForServices` ДО запуска workflow.

3. **rtconfig `NoSuchFileException` в docker worker**. mdb-processing docker
   worker запускается с CWD=`/app`, а `LocalRealtimeConfig` резолвит
   `src/main/resources/rtconfig/<profile>.json` через filesystem path (не
   classpath). В docker этого path нет. Решение: запускать mdb-processing через
   `bootRun --args='--spring.profiles.active=local'` (CWD=project root, путь
   резолвится). Маунт rtconfig через docker-compose тоже работает, но
   пользователь просил не маунтить — bootRun чище.

4. **`local` профиль пишет в РЕАЛЬНЫЙ PMS** (`pms.cloud.vk.team`) и ходит к
   РЕАЛЬНЫМ cloud masters (`master.dc.odkl.ru:443`). mTLS-сертификат из
   `~/.mccloud/` работает. Тестировать только на dev-кластерах (mdbdev,
   project 160). Никогда не запускать на проде.

5. **Vault `zkv` должен быть включён вручную**. `localrun.sh` проглатывает
   ошибку `vault secrets enable -path=zkv kv-v2` если уже включено или если
   vault не пустой. Перед тестом:
   ```bash
   docker exec mdb-processing-vault vault secrets list | grep zkv
   # если нет:
   docker exec mdb-processing-vault vault secrets enable -path=zkv kv-v2
   ```

6. **SSH-exec на cruise host** (`restartCruiseInstanceSsh`,
   `pingSshRestartedCruiseInstanceReady`) использует `one-cloud-client
   SshSupportImpl` с raw-socket streaming (`TerminalCall.readUntil(" ")`).
   Это работает только против реальных cloud masters, **не** против wiremock
   (wiremock возвращает обычный HTTP, не raw socket). Поэтому профиль `local`
   обязателен — `docker-local` профиль (с wiremock) упадёт на
   `HttpException: Invalid type of response received`.

7. **Activity retry на event 77** — одна из activity в `createCruiseControl`
   упала и автоматически заретраилась (timer backoff → reschedule → success).
   Это нормальное поведение temporal для transient-ошибок cloud API. Workflow
   COMPLETED — ретрай сработал. Не нужно паниковать при виде
   `ACTIVITY_TASK_FAILED` в истории, если за ним следует
   `ACTIVITY_TASK_COMPLETED`.

8. **Cleanup перед повторным прогоном** (если workflow упал и нужно
   перезапустить с тем же workflow_id):
   ```sql
   DELETE FROM operations WHERE cluster_id='96777c6a-52c5-40db-8e4c-2d7f824301f6';
   ```
   + `WorkflowIdReusePolicy.ALLOW_DUPLICATE` в опциях (или использовать новый
   workflow_id).

## Ссылки

- Workflow: `src/main/java/one/cloud/mdb/processing/kafka/workflow/create/cruise/CreateKafkaCruiseWorkflowImpl.java`
- Request record: `src/main/java/one/cloud/mdb/processing/kafka/model/create/cruise/CreateKafkaCruiseRequest.java`
- Тест: `src/test/java/one/cloud/mdb/processing/kafka/workflow/create/cruise/CreateKafkaCruiseWorkflowImplTest.java`
- Vault activity: `KafkaCruiseVaultActivity` / `KafkaCruiseVaultActivityImpl`
- Manifest activity: `KafkaCruiseManifestActivity` / `KafkaCruiseManifestActivityImpl`
- Предыдущий cruise history: `MDBDEV-2349-cruise-capacity-2026-07-01.md` (capacity activity)
- Предыдущий cruise template history: `MDBDEV-2402-cruise-control-template-2026-06-30.md`
