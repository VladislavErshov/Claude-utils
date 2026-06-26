# MDBDEV-2349: upsertCruiseControlCapacity — 2026-07-01

Новый activity `KafkaPmsActivityImpl.upsertCruiseControlCapacity` — при modify kafka
cluster с изменением broker resources (CPU/disk/lan/ram) рендерит
`kafka-cruise-control-capacity.template` и записывает в PMS-переменную
`kafka.cruisecontrol.capacity.json`.

## Что изменилось

### mdb-processing (коммит 82b480fc)

1. **`UpdateKafkaCruiseConfigInputData`** — добавлены 3 nullable поля:
   `diskGb`, `lanInMb`, `lanOutMb`.

2. **`ModifyKafkaClusterMapper.toUpdateCruiseConfigData`** —填充 их из
   `dto.brokerResources()`:
   - `diskGb` — размер data-диска брокера в GB (`volumes.disks[name=="data"].sizeGb`)
   - `lanInMb` — `brokerResources.resources.lanInM`
   - `lanOutMb` — `brokerResources.resources.lanOutM`
   Если `brokerResources == null` — все три `null`.

3. **`UpdateKafkaCruiseConfigWorkflowImpl.updateCruiseConfig`** — перед
   `upsertCruiseControlConfig` вызывает
   `pmsActivity.upsertCruiseControlCapacity(namespace, pmsHostName, diskGb, lanInMb, lanOutMb)`.

4. **`KafkaPmsActivity.upsertCruiseControlCapacity`** — сигнатура:
   ```java
   void upsertCruiseControlCapacity(Namespace, String pmsHostName,
       @Nullable Long diskGb, @Nullable Long lanInMb, @Nullable Long lanOutMb);
   ```

5. **`KafkaPmsActivityImpl.upsertCruiseControlCapacity`** — если любой параметр
   `null`, логирует "Skipping" и выходит. Иначе:
   - `pmsConfigLoader.load(DbType.KAFKA, "kafka-cruise-control-capacity.template")`
   - подстановки: `${DISK} = diskGb * 1024` (GB→MB), `${NW_IN} = lanInMb * 1024`
     (MB→KB), `${NW_OUT} = lanOutMb * 1024` (MB→KB)
   - `pmsService.createOrUpdateVariable(namespace, pmsHostName,
     "kafka.cruisecontrol.capacity.json", rendered, null)`

6. **`KafkaPmsProperty.CRUISE_CONTROL_CAPACITY_JSON`** = `"kafka.cruisecontrol.capacity.json"`.

7. **`src/main/resources/pms/kafka/kafka-cruise-control-capacity.template`** — новый:
   ```json
   {
     "brokerCapacities":[
       {
         "brokerId": "-1",
         "capacity": {
           "DISK": "${DISK}",
           "CPU": "100",
           "NW_IN": "${NW_IN}",
           "NW_OUT": "${NW_OUT}"
         },
         "doc": "This is the default capacity. Capacity unit used for disk is in MB, cpu is in percentage, network throughput is in KB."
       }
     ]
   }
   ```
   (`kafka-cruise-control-config.template` переехал из `resources/` в `resources/pms/kafka/`.)

### mdb-data

Без изменений в этом коммите. `brokerResources` уже заполнялся в
`KafkaClusterModificationMapper.toModifyKafkaClusterDto` только когда
`brokerResourcesDiff == true` (detector `hasBrokerResourcesDiff`: изменились
`lanIn`, `lanOut`, `diskGb`, `diskType`, `jvmHeapSizeMb` или `hardwarePresetId`).

## Кластер

- **cluster_id**: `9e0336c7-50da-4487-8746-d332357180d3` (name `test-update-resize1`)
- **dc**: `dc`
- **cruise-инстанс**: `1.cruise.test-update-resize1-mdbdev-kafka.dc.one-infra.ru`
- **pmsHostName**: `test-update-resize1-mdbdev-kafka.clouds`
- Брокеры в 3 DC: `dc`, `kc`, `zc`
- Текущие broker-ресурсы: lanIn=10, lanOut=20, diskGb=8, jvmHeapSizeMb=1024, hwPresetId=100

## Сценарий: modify broker network → lanIn=15, lanOut=25

Файл: `/tmp/modify_capacity.json`. PATCH `/api/v2/mdb/kafka/clusters/{id}/modify`.

Изменения: `lanIn: 10 → 15`, `lanOut: 20 → 25` (+ `cruiseControl` non-null, чтобы
`hasCruiseControlDiff == true` и `toUpdateCruiseConfigData` не вернул null —
update cruise config workflow запускается только когда cruiseControl != null).
`hardwarePresetId: 100` без изменений, `diskGb/diskType/jvmHeapSizeMb` без изменений.

```json
{
  "params": {
    "name": "test-update-resize1",
    "lanIn": 15,
    "lanOut": 25,
    "diskGb": 8,
    "diskType": "nvme",
    "projectId": 160,
    "rootQueue": "prod",
    "isWan": false,
    "needLanIpv6": true,
    "needWanIpv4": false,
    "needWanIpv6": false,
    "kafkaParams": {
      "controller": {
        "controllerDcs": ["dc","kc","zc"],
        "controllerLanIn": 15, "controllerLanOut": 50,
        "controllerMemGb": 4, "controllerVcores": 4,
        "controllerDiskGb": 10, "controllerDiskType": "nvme",
        "controllerJvmHeapSizeMb": 2048,
        "controllerConfig": {"config": {}}
      },
      "brokerConfig": {"config": {}},
      "cruiseControl": {
        "cruiseControlDc": "dc",
        "autoRebalanceEnabled": true,
        "autoRebalanceOnBrokerFailEnabled": true,
        "replicationThrottleMb": 10
      },
      "jvmHeapSizeMb": 1024
    }
  },
  "hardwarePresetId": 100,
  "attempts": 1
}
```

Важно: `brokerConfig.config` должен совпадать с текущим (`{"config": {}}`), иначе
запустится update broker config workflow. `controllerConfig` аналогично.

## Результат в temporal

operation id `e7c250a3-def0-4841-82cd-9e5496fd1b64`. Все workflow `COMPLETED`:

- `modifyKafkaCluster` → COMPLETED
- `modifyController` → COMPLETED (no diff)
- `modifyBroker` → COMPLETED (resize: dc/kc/zc каждый через `kafkaResizeBrokerInstance`)
- `modifyCruise` → COMPLETED
- `updateCruiseConfig` → COMPLETED

### Input `modifyKafkaCluster` → `cruiseUpdateConfigData` (декодирован)

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
  "diskGb": 8,
  "lanInMb": 15,
  "lanOutMb": 25,
  "forceUpdate": false,
  "workflowTtl": 3600.000000000
}
```

`diskGb=8`, `lanInMb=15`, `lanOutMb=25` — проброс из `brokerResources` работает.

### Activities в `updateCruiseConfig`

```
upsertCruiseControlConfig       # пишет kafka.cruisecontrol.properties (из MDBDEV-2402)
upsertCruiseControlCapacity     # НОВОЕ: пишет kafka.cruisecontrol.capacity.json
cloud_getServiceInfo
cloud_getInfoForInstances
kafka_host_restartCruiseInstanceSsh
kafka_host_pingSshRestartedCruiseInstanceReady
cloud_getInfoForInstances
```

### Лог mdb-processing

```
Applied Cruise Control capacity to PMS for host test-update-resize1-mdbdev-kafka.clouds:
  disk=8192 Mb, nwIn=15360 Kb, nwOut=25600 Kb
```

Конверсии единиц корректные:
- `diskGb * 1024` = 8 * 1024 = **8192** Mb
- `lanInMb * 1024` = 15 * 1024 = **15360** Kb
- `lanOutMb * 1024` = 25 * 1024 = **25600** Kb

## PMS в local-профиле

`application-local.yaml`: `pms.baseUrl: https://pms.cloud.vk.team` — реальный PMS.
Тело POST не видно в wiremock. Подтверждение записи — лог activity
`Applied Cruise Control capacity to PMS for host ...`.

## Подводные камни

1. **Capacity обновится только если `brokerResources != null`** — а он non-null
   только когда `brokerResourcesDiff == true`. Cruise-only PATCH (без изменения
   lanIn/lanOut/diskGb/diskType/jvmHeapSizeMb/hardwarePresetId) оставит
   `diskGb/lanInMb/lanOutMb = null` → `upsertCruiseControlCapacity` логирует
   "Skipping" и не делает PMS-call. Это ожидаемое поведение.

2. **Изменение broker resources запускает `kafkaResizeBrokerInstance`** для
   каждого DC — реально меняет инстанс в cloud (через one-cloud master). Может
   занять ~15 минут (resize 3 брокеров последовательно). Без реального cloud/ssh
   workflow упадёт на `kafka_host_isBrokerServiceFailed` (ssh-ping).

3. **`hasCruiseControlDiff` = `cruiseControl != null`** — чтобы
   `updateCruiseConfig` workflow запустился, в запросе обязательно нужно передать
   `cruiseControl` (можно с теми же значениями, что в БД). Иначе
   `toUpdateCruiseConfigData` вернёт null и cruise-branch не запустится, даже если
   broker resources изменились — capacity не обновится.

4. **Cleanup перед прогоном**:
   ```sql
   DELETE FROM operations WHERE cluster_id='9e0336c7-50da-4487-8746-d332357180d3';
   DELETE FROM db_cluster_version WHERE cluster_id='9e0336c7-50da-4487-8746-d332357180d3'
     AND status IN ('draft','failed','need_retry','scheduled');
   ```

5. **Запуск `updateCruiseConfig` напрямую (для изолированного теста capacity)**
   — если нужно проверить только capacity без broker resize:
   ```bash
   docker cp /tmp/cruise_input.json mdb-processing-temporal:/tmp/cruise_input.json
   docker exec mdb-processing-temporal sh -c 'tctl --address 172.21.0.5:7233 \
     workflow run --taskqueue kafka-activities-queue \
     --workflow_type updateCruiseConfig \
     --wid manual-cruise-capacity-test --if /tmp/cruise_input.json'
   ```
   Внутри контейнера temporal-server слушает на `172.21.0.5:7233` (не на 127.0.0.1),
   поэтому нужен `--address 172.21.0.5:7233`. IP может отличаться — проверить через
   `docker exec mdb-processing-temporal ss -tlnp | grep 7233`.

## Unit-тесты

`KafkaPmsActivityImplTest`:
- `upsertCruiseControlCapacity_shouldRenderWithUnitConversions` — передаёт
  `(100L, 200L, 300L)`, проверяет `DISK=102400`, `NW_IN=204800`, `NW_OUT=307200`,
  отсутствие нераскрытых `${KEY}`.
- `upsertCruiseControlCapacity_shouldSkipWhenAnyParamNull` — любой null →
  `verifyNoInteractions(pmsService)` и `verifyNoInteractions(pmsConfigLoader)`.
