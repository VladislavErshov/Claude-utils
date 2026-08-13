# MDBDEV-2882: createKafkaCruise — 2026-08-13

Успешный запуск `createKafkaCruise` на кластере `kafka-events-staging` (cluster_id
`761a5be5-dca6-4186-bff4-f2ebfd9fb26c`), DC `hc`. COMPLETED за 132 секунды.

## Что изменилось по сравнению с историей 2026-08-06

1. **Workflow type переименован**: `createCruiseControl` → `createKafkaCruise`
   (`CreateKafkaCruiseWorkflow.java:15`, `@WorkflowMethod(name = "createKafkaCruise")`).
2. **Request record переименован**: `CreateCruiseControlRequest` → `CreateKafkaCruiseRequest`
   (`model/create/cruise/CreateKafkaCruiseRequest.java`).
3. **Удалены поля request**: `namespaceDomain` и `brokerParameters` — их больше нет в record.
4. **`one_cloud_meta` обязательна** — без записи `params_type='cruise-control-service'`
   workflow падает на `MdbDataKafkaHostsActivityImpl.savedCreatedKafkaCruiseInfo`
   (MdbDataKafkaHostsActivityImpl.java:100) с `404 OneCloud meta params (type:
   cruise-control-service) not found`. В предыдущей истории этого не было, потому что
   тогда кластер уже имел cruise meta.

## Кластер

- **cluster_id**: `761a5be5-dca6-4186-bff4-f2ebfd9fb26c` (name `kafka-events-staging`)
- **namespace**: `infra` (в JSON — `"INFRA"` uppercase)
- **queue**: `kafka-events-staging-adtech-kafka`
- **fullQueue**: `kafka-events-staging-adtech-kafka.adtech.db.production.mdb.prod`
- **pmsHostName**: `kafka-events-staging-adtech-kafka.clouds`
- **certsHostName**: `cruise.kafka-events-staging-adtech-kafka.clouds`
- **serviceName**: `cruise.kafka-events-staging-adtech-kafka`
- **projectName**: `adtech` (id=55)
- **brokerDcs**: `hc`, `kc`, `pc`
- **cruiseControlDc**: `hc` — очередь в hc уже существовала (там же живут брокеры),
  `submitQueueIfNeeded` отработал как no-op
- **cruise host** уже был в `host_state` (id 45321, FQDN
  `1.cruise.kafka-events-staging-adtech-kafka.hc.one-infra.ru`) — cruise-сервис
  существовал в облаке до запуска, `submitServiceManifest` с `force-submit=true`
  пересабмитил безопасно
- **cruiseUserPassword**: `sdd543y3NhRzMFm7y8FA`
- **docker**: `ubuntu20-mdb-cruisecontrol-2.5.147` / `1.0.2`
- **broker resources** (из последней `db_cluster_version` 184338): diskGb=256,
  lanIn=1500, lanOut=1500, jvmHeapSizeMb(broker)=8192
- **cruiseControl.jvmHeapSizeMb**: 2048 (в кластере был `null` → взяли дефолт из истории)

## Запуск

Сначала засев БД через `/db-seed` одним SQL через `jsonb_build_object` (см.
обновлённый шаблон в `SKILL.md`). Тянем ОБЯЗАТЕЛЬНО:

- `db_cluster`, `db_cluster_version` (LIMIT 3), `host_state`
- **`one_cloud_meta`** со всеми `params_type` (`cruise-control-service`, `db-service`,
  `kafka-controller-service`) — без cruise-control-service workflow падает
- `projects`, `namespaces`, `hardware_presets`

После засева — workflow через tctl напрямую (mdb-data эндпоинт этот flow не дёргает):

```bash
docker cp /tmp/cruise-request.json mdb-processing-temporal:/tmp/cruise-request.json
docker exec mdb-processing-temporal sh -c "tctl --address 172.19.0.5:7233 workflow run \
  --taskqueue kafka-activities-queue \
  --workflow_type createKafkaCruise \
  --workflow_id cruise-create-hc-<timestamp> \
  --execution_timeout 3600 \
  --input_file /tmp/cruise-request.json"
```

`CreateKafkaCruiseRequest` (актуальный record, 19 полей — БЕЗ `namespaceDomain`/`brokerParameters`):

```json
{
  "clusterId": "761a5be5-dca6-4186-bff4-f2ebfd9fb26c",
  "namespace": "INFRA",
  "queue": "kafka-events-staging-adtech-kafka",
  "fullQueue": "kafka-events-staging-adtech-kafka.adtech.db.production.mdb.prod",
  "rootQueue": "prod",
  "projectName": "adtech",
  "pmsHostName": "kafka-events-staging-adtech-kafka.clouds",
  "certsHostName": "cruise.kafka-events-staging-adtech-kafka.clouds",
  "serviceName": "cruise.kafka-events-staging-adtech-kafka",
  "cruiseControlDc": "hc",
  "isWan": false,
  "cruiseControl": {
    "dc": "hc",
    "autoRebalanceEnabled": true,
    "jvmHeapSizeMb": 2048
  },
  "brokerDcs": ["hc", "kc", "pc"],
  "brokerDiskGb": 256,
  "brokerLanInMb": 1500,
  "brokerLanOutMb": 1500,
  "dockerName": "ubuntu20-mdb-cruisecontrol-2.5.147",
  "dockerTag": "1.0.2",
  "cruiseUserPassword": "sdd543y3NhRzMFm7y8FA",
  "workflowTtl": "PT1H"
}
```

## Результат в temporal

```
WORKFLOW_EXECUTION_STATUS_COMPLETED  cruise-create-hc-1786620225  (132s)
  ├─ child: updateConfigKafkaBroker (COMPLETED)
  │   ├─ reloadKafkaBrokerInstance_hc (COMPLETED)
  │   ├─ reloadKafkaBrokerInstance_kc (COMPLETED)
  │   └─ reloadKafkaBrokerInstance_pc (COMPLETED в child)
```

Порядок activity (по history):
```
cloud:getInfosForServices (discoverBrokerHosts)
pms:upsertCruiseControlLogConfig
pms:upsertCruiseControlConfig
pms:upsertCruiseControlCapacity
pms:upsertCruiseControlJaas
pms:upsertCruiseControlSysconfig
pms:upsertCruiseControlPyvaultConf
vault:createCruiseUserSecret
cloud:submitQueueIfNeeded        # no-op, очередь в hc уже была
manifest:renderCruiseServiceManifest
cloud:submitServiceManifest      # force-submit в hc
pms:addBrokerProperties          # metric.reporters=CruiseControlMetricsReporter
[child: updateBrokerConfig → 3 reloadKafkaBrokerInstance для hc/kc/pc]
mdb-data:savedCreatedKafkaCruiseInfo  # финальный шаг, пишет в one_cloud_meta через mdb-data API
cloud:getServiceInfo (waitServiceRunning)
```

## Что записалось в локальную БД

**Ничего.** Workflow запускался через tctl напрямую, минуя mdb-data REST API (т.к.
кодогенерации эндпоинта для cruise-creation пока нет). Activity
`savedCreatedKafkaCruiseInfo` вызывает mdb-data API, но в локальной БД:

- `operations`: 0 строк (workflow не создавал operation)
- `one_cloud_meta`: 3 записи — те же что засеяны, не обновлялись
- `host_state`: 7 хостов — те же что засеяны, cruise host 45321 не обновлялся

Workflow пишет в **PMS**, **Vault** и **cloud** — это реальные внешние системы
(local-профиль mdb-processing ходит в `pms.cloud.vk.team`, не в wiremock). Проверка
PMS-переменных — через скилл `kafka-config-inspector`.

## Подводные камни

1. **`one_cloud_meta` обязательна** — без `params_type='cruise-control-service'` workflow
   падает с `404` на финальном шаге `savedCreatedKafkaCruiseInfo`. В шаблоне db-seed
   в `SKILL.md` теперь явно указано тянуть эту таблицу.

2. **Имя workflow-типа — `createKafkaCruise`**, не `createCruiseControl` (устарело).

3. **Request record — `CreateKafkaCruiseRequest`**, 19 полей. `namespaceDomain` и
   `brokerParameters` убраны после рефакторинга.

4. **`namespace` — uppercase `"INFRA"`**, не `"infra"`. Temporal payload codec для
   `Namespace` enum требует uppercase.

5. **`workflowTtl` — ISO-8601 строка `"PT1H"`**, не число.

6. **Повторный запуск безопасен**: `submitServiceManifest` использует `force-submit=true`.
   Можно перезапускать workflow с тем же `workflow_id` (или новым — старый останется в
   history как COMPLETED).

7. **Cruise host уже в `host_state`** — если кластер ранее уже имел cruise (например,
   был пересоздан), `submitServiceManifest` корректно обрабатывает существующий сервис.

8. **Local-профиль пишет в РЕАЛЬНЫЙ PMS** — `pms.cloud.vk.team`. Тестировать только на
   dev-кластерах. Кластер `761a5be5` (kafka-events-staging, project adtech) — staging,
   не prod, подходит для тестов.

## Ссылки

- Workflow: `src/main/java/one/cloud/mdb/processing/kafka/workflow/create/cruise/CreateKafkaCruiseWorkflowImpl.java`
- Request record: `src/main/java/one/cloud/mdb/processing/kafka/model/create/cruise/CreateKafkaCruiseRequest.java`
- Activity с 404: `MdbDataKafkaHostsActivityImpl.java:100` (`savedCreatedKafkaCruiseInfo`)
- Предыдущая история: `MDBDEV-2882-create-cruise-control-2026-08-06.md` (там ещё старое
  имя `createCruiseControl` и поля `namespaceDomain`/`brokerParameters`)
