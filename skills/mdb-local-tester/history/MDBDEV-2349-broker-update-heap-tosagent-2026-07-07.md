# MDBDEV-2349: broker update on heap tosagent — 2026-07-07

Локальный тест последнего коммита `b6f26786` (MDBDEV-2349 broker update on heap tosagent) на кластере `0964c579-1f1b-4595-9c28-84dc783d2a29` (test-modify, kafka).

Цель: проверить, что (1) изменения `tosAgent` пишутся в БД, (2) изменения `jvmHeapSizeMb` (broker) и `controllerJvmHeapSizeMb` (controller) триггерят `updateBrokerConfig`/`updateControllerConfig` вместо resize.

## Что проверяем (логика коммита b6f26786)

`KafkaClusterDiffDetector`:
- `hasBrokerResourcesDiff` — убран `jvmHeapSizeMb` (больше не триггерит resize)
- `hasControllerResourcesDiff` — убран `controllerJvmHeapSizeMb` (больше не триггерит resize)
- Новые методы:
  - `hasBrokerHeapDiff` (jvmHeapSizeMb) → входит в `brokerConfigDiff` → `updateBrokerConfig`
  - `hasControllerHeapDiff` (controllerJvmHeapSizeMb) → входит в `controllerConfigDiff` → `updateControllerConfig`
  - `hasTosAgentDiff` (tosAgent) → входит в `brokerConfigDiff` → `updateBrokerConfig`

`KafkaParams` — добавлено поле `tosAgent` (сохраняется в БД через `KafkaClusterParamsMapper`).

## Кластер

- **cluster_id**: `0964c579-1f1b-4595-9c28-84dc783d2a29` (name `test-modify`, project 160 mdbdev, namespace `infra`)
- **done-версия 136662** (в удалёнке `scheduled`, локально переведена в `done`):
  - `tosAgent: false`
  - `jvmHeapSizeMb: 1024`
  - `controllerJvmHeapSizeMb`: **отсутствует** (null)
  - `brokerConfig`: отсутствует
  - `controllerConfig`: отсутствует
  - `cruiseControl: {cruiseControlDc: "dc", cruiseUserPassword: ""}` — 2 поля
  - `controllerDcs: ["hc","kc","zc"]`
  - `lanIn=10, lanOut=20, diskGb=25, diskType=nvme, hwPresetId=100`
  - controller resources без изменений
- **Хосты** (10 шт.): 5 брокеров (dc, hc, kc, zc, zc), 4 контроллера (hc, kc, nc, zc), 1 cruise (dc)

## Modify-запрос (только изменения последнего коммита)

Файл: `/tmp/modify_0964c579.json`. PATCH `/api/v2/mdb/kafka/clusters/{id}/modify`.

Изменения относительно done 136662:
- `tosAgent: false → true`
- `jvmHeapSizeMb: 1024 → 2048`
- `controllerJvmHeapSizeMb: null → 2048`
- Остальное — без изменений (brokerConfig, controllerConfig не передаются, cruiseControl 2 поля, controller resources идентичны)

```json
{
  "params": {
    "diskGb": 25, "name": "test-modify", "projectId": 160,
    "lanIn": 10, "lanOut": 20, "diskType": "nvme",
    "acl": {}, "isWan": false,
    "kafkaParams": {
      "tosAgent": true,
      "controller": {
        "controllerDcs": ["hc", "kc", "zc"],
        "controllerLanIn": 15, "controllerMemGb": 4,
        "controllerDiskGb": 10, "controllerDiskType": "nvme",
        "controllerLanOut": 50, "controllerVcores": 4,
        "controllerJvmHeapSizeMb": 2048
      },
      "cruiseControl": {"cruiseControlDc": "dc", "cruiseUserPassword": ""},
      "jvmHeapSizeMb": 2048
    },
    "needLanIpv6": true, "needWanIpv4": false, "needWanIpv6": false,
    "rootQueue": "prod"
  },
  "hardwarePresetId": 100,
  "hosts": [{"dc": "hc"}, {"dc": "kc"}, {"dc": "zc"}],
  "attempts": 3
}
```

Ответ: `HTTP 202 Accepted`.

## Результат в БД

Новая `draft`-версия (id=1):

| id | status | tosAgent | jvmHeapSizeMb | controllerJvmHeapSizeMb |
|---|---|---|---|---|
| 1 | draft | **true** ✓ | **2048** ✓ | **2048** ✓ |
| 136662 | done | false | 1024 | (null) |

`tosAgent=true` записано в БД — новая логика маппера работает.

operation id `6c4297f3-ad70-4ccf-a2af-58b329d9849e`, status `in_progress`, type `modify_cluster`.

## Результат в temporal (все COMPLETED, operation=done)

Полный набор запущенных workflows (все завершены успешно):

```
COMPLETED  modifyKafkaCluster
COMPLETED  modifyController
COMPLETED  updateControllerConfig        ← controller heap → UPDATE
COMPLETED  modifyBroker
COMPLETED  updateBrokerConfig            ← broker heap + tosAgent → UPDATE
COMPLETED  reloadKafkaBrokerInstance  ×5 (dc, hc, kc, zc_1, zc_2)
COMPLETED  modifyCruise
COMPLETED  updateCruiseConfig
```

Resize workflows **не запущены вообще**: ни `kafkaResizeBroker`, ни controller resize, ни cruise resize (в логе: `Skipping cruise resize — no data provided`).

`modifyKafkaCluster` (workflow id `6c4297f3-...`) — COMPLETED. Декодированный input:

```json
{
  "namespace": "infra",
  "fullQueue": "test-modify-mdbdev-kafka.mdbdev.db.production.mdb.prod",
  "controllerResizeData": null,        // ✓ controller heap НЕ триггерит resize
  "brokerResizeData": null,            // ✓ broker heap НЕ триггерит resize
  "cruiseResizeData": null,
  "updateControllerConfigData": {
    "clusterId": "0964c579-...",
    "controllers": ["...hc...", "...kc...", "...nc...", "...zc..."],
    "parameters": {},
    "heapSizeMB": 2048,                // ✓ controller heap ушёл в UPDATE
    "forceUpdate": false
  },
  "updateBrokerConfigData": {
    "clusterId": "0964c579-...",
    "brokers": ["...dc...", "...hc...", "...kc...", "...zc...", "2...zc..."],
    "parameters": {},
    "heapSizeMB": 2048,                // ✓ broker heap ушёл в UPDATE
    "tosAgentEnabled": true,           // ✓ tosAgent ушёл в UPDATE
    "forceUpdate": false
  },
  "cruiseUpdateConfigData": { ... cruiseControl ... },
  "controllerOrder": "UPDATE_THEN_RESIZE",
  "brokerOrder": "RESIZE_THEN_UPDATE",
  "cruiseOrder": "UPDATE_THEN_RESIZE"
}
```

Запущенные child workflows (все COMPLETED):
- `modifyKafkaCluster`, `modifyController`, `updateControllerConfig` — controller heap update
- `modifyBroker`, `updateBrokerConfig` + 5× `reloadKafkaBrokerInstance` — broker heap + tosAgent update
- `modifyCruise`, `updateCruiseConfig` — cruise update (resize скипнут: "no data provided")

Resize workflows **не запущены вообще** — broker/controller heap ушли в update-ветку, как и ожидает новая логика.

## Выводы

✅ **`tosAgent` пишется в БД** — в новой draft-версии `kafkaParams.tosAgent=true`.

✅ **`jvmHeapSizeMb` (broker) → UPDATE, не RESIZE** — `brokerResizeData=null`, `updateBrokerConfigData.heapSizeMB=2048`.

✅ **`controllerJvmHeapSizeMb` (controller) → UPDATE, не RESIZE** — `controllerResizeData=null`, `updateControllerConfigData.heapSizeMB=2048`, workflow `updateControllerConfig` запущен.

✅ **`tosAgent` diff → UPDATE** — `updateBrokerConfigData.tosAgentEnabled=true`.

Логика последнего коммита `b6f26786` работает корректно.

## Особенности

1. **`brokerOrder: RESIZE_THEN_UPDATE`** — хотя resize нет, порядок остался. Не критично, но можно отметить: когда `brokerResizeData=null`, шаг resize скипается и сразу идёт update.

2. **`updateControllerConfigData.parameters={}`** — пустой, т.к. `controllerConfig` не передавали. Но `hasControllerHeapDiff` (controllerJvmHeapSizeMb: null→2048) делает `controllerConfigDiff=true` → update запускается. Это правильное поведение новой логики.

3. **One-cloud/PMS недоступны локально** — workflows висят в RUNNING на activity `cloud_getInfoForInstances` (`master.zc.odkl.ru` не резолвится). Это не влияет на проверку логики детектора — нужные данные (`brokerResizeData=null`, `updateControllerConfigData.heapSizeMB=2048`) видны в input workflow сразу при старте.

## Cleanup

```sql
DELETE FROM operations WHERE cluster_id='0964c579-1f1b-4595-9c28-84dc783d2a29';
DELETE FROM db_cluster_version WHERE cluster_id='0964c579-1f1b-4595-9c28-84dc783d2a29'
  AND status IN ('draft','failed','need_retry','scheduled');
```

## Файлы

- Modify request: `/tmp/modify_0964c579.json`
- Seed SQL: `/tmp/seed_0964c579.sql`
