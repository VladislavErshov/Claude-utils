# MDBDEV-2162: JVM Heap Size — три order для brokers / controllers / cruise (2026-06-26)

Тест обновлённой DTO `ModifyKafkaClusterDto` (processing): вместо одного `order` теперь три поля —
`controllerOrder`, `brokerOrder`, `cruiseOrder`. Соответственно mapper на стороне mdb-data
`KafkaClusterModificationMapper` тоже обновлён: `resolveOrder` разбит на `resolveBrokerOrder` +
`resolveControllerOrder` + константу `cruiseOrder = UPDATE_THEN_RESIZE`.

## Кластер

- **cluster_id**: `9e0336c7-50da-4487-8746-d332357180d3`
- **name**: `test-update-resize1`, type `kafka`
- Baseline `db_cluster_version` (status=done, id=135287):
  - `kafkaParams.jvmHeapSizeMb`: 1024 (broker)
  - `kafkaParams.controller.controllerJvmHeapSizeMb`: "2048" (строка в БД!)
  - `kafkaParams.cruiseControl`: `{"cruiseControlDc": "dc", "cruiseUserPassword": ""}` — без `jvmHeapSizeMb`

## Запрос

Файл: `/tmp/modify_request_jvm_heap.json`.

```json
{
  "params": {
    "acl": {},
    "name": "test-update-resize1",
    "isWan": false,
    "lanIn": 10,
    "diskGb": 8,
    "lanOut": 20,
    "diskType": "nvme",
    "projectId": 160,
    "rootQueue": "prod",
    "kafkaParams": {
      "controller": {
        "controllerDcs": ["dc", "kc", "zc"],
        "controllerLanIn": 15,
        "controllerMemGb": 4,
        "controllerDiskGb": 10,
        "controllerLanOut": 50,
        "controllerVcores": 4,
        "controllerDiskType": "nvme",
        "controllerJvmHeapSizeMb": 3072,
        "controllerConfig": {"config": {"log.retention.hours": "168"}}
      },
      "cruiseControl": {
        "cruiseControlDc": "dc",
        "cruiseUserPassword": "",
        "jvmHeapSizeMb": 4095
      },
      "brokerConfig": {
        "config": {"compression.type": "uncompressed"}
      },
      "jvmHeapSizeMb": 2048
    },
    "needLanIpv6": true,
    "needWanIpv4": false,
    "needWanIpv6": false
  },
  "hardwarePresetId": 100,
  "hosts": [{"dc": "dc"}, {"dc": "kc"}, {"dc": "zc"}],
  "attempts": 3
}
```

Endpoint: `PATCH http://localhost:8081/api/v2/mdb/kafka/clusters/9e0336c7-50da-4487-8746-d332357180d3/modify`
Ответ: `202 Accepted`, operation `4732ffa1-0b2f-49ba-a105-76b7914c45d0`.

Сценарий выбран чтобы проверить **оба** направления order:
- broker 1024 → 2048 (increase) → ожидается `brokerOrder = RESIZE_THEN_UPDATE`
- controller 2048 → 3072 (increase, в пределах 0.8 × 4 × 1024 = 3276) → ожидается `controllerOrder = RESIZE_THEN_UPDATE`
- cruise 4095 (current отсутствует) → ожидается `cruiseOrder = UPDATE_THEN_RESIZE` (всегда)

## Валидация (KafkaClusterModificationValidator)

- **Broker**: `JVM_HEAP_MIN_MB = 1024`. Запрошенный 2048 проходит.
- **Controller**: `< 1024` → reject; `> 0.8 × controllerMemGb × 1024` → reject. При `controllerMemGb=4` → max=3276. 3072 проходит.
- **Cruise**: `> CRUISE_RAM_GB (6) × 1024 = 6144` → reject. 4095 проходит.

## Что ушло в temporal

`modifyKafkaCluster` workflowId `4732ffa1-0b2f-49ba-a105-76b7914c45d0`. Декодированный input:

```json
{
  "namespace": "infra",
  "pmsHostName": "test-update-resize1-mdbdev-kafka.clouds",
  "controllerResizeData": {...},
  "brokerResizeData": {...},
  "cruiseResizeData": null,
  "updateControllerConfigData": {
    "clusterId": "9e0336c7-50da-4487-8746-d332357180d3",
    "controllers": ["...dc...", "...kc...", "...zc..."],
    "parameters": {"log.retention.hours": "168"},
    "heapSizeMB": 3072,
    "forceUpdate": false,
    "workflowTtl": 10800
  },
  "updateBrokerConfigData": {
    "clusterId": "9e0336c7-50da-4487-8746-d332357180d3",
    "brokers": ["...dc...", "...kc...", "...zc..."],
    "parameters": {"compression.type": "uncompressed"},
    "heapSizeMB": 2048,
    "forceUpdate": false,
    "workflowTtl": 10800
  },
  "controllerOrder": "RESIZE_THEN_UPDATE",
  "brokerOrder": "RESIZE_THEN_UPDATE",
  "cruiseOrder": "UPDATE_THEN_RESIZE",
  "socLoggerData": null,
  "workflowTtl": 14400
}
```

### Проверки propagation

| Поле в request                          | Поле в DTO                                | Поле в workflow input                | Статус       |
|-----------------------------------------|-------------------------------------------|--------------------------------------|--------------|
| broker `jvmHeapSizeMb=2048`             | `brokerHeapSizeMB=2048`                   | `updateBrokerConfigData.heapSizeMB=2048` | ✅ propagated |
| controller `controllerJvmHeapSizeMb=3072` | `controllerHeapSizeMB=3072`             | `updateControllerConfigData.heapSizeMB=3072` | ✅ propagated |
| cruise `jvmHeapSizeMb=4095`             | `cruiseHeapSizeMB=4095`                   | **отсутствует**                      | ❌ dropped    |
| broker heap 1024 → 2048 (increase)      | —                                         | `brokerOrder=RESIZE_THEN_UPDATE`     | ✅ correct    |
| controller heap 2048 → 3072 (increase)  | —                                         | `controllerOrder=RESIZE_THEN_UPDATE` | ✅ correct    |
| cruise heap (нет current)               | —                                         | `cruiseOrder=UPDATE_THEN_RESIZE`     | ✅ correct (constant) |

### Условие propagation на processing-стороне

`ModifyKafkaClusterMapper.toInputData()` (`mdb-processing/src/main/java/.../kafka/mapper/ModifyKafkaClusterMapper.java`):
- `controllerHeapSizeMB` → попадает в `UpdateControllerConfigInputData` **только если** `controllers != null && !empty && controllerParameters != null` (т.е. есть `controllerConfigDiff`).
- `brokerHeapSizeMB` → попадает в `UpdateBrokerConfigInputData` **только если** `brokers != null && !empty && brokerParameters != null` (т.е. есть `brokerConfigDiff`).
- `cruiseHeapSizeMB` → **не читается вообще**, ни в один `InputData` не попадает. Открытая находка из предыдущего теста — не закрыта.
- `controllerOrder` / `brokerOrder` / `cruiseOrder` → передаются как есть, processing дефолтит null → `UPDATE_THEN_RESIZE`.

## Логика order на стороне mdb-data

`KafkaClusterModificationMapper`:

```java
// broker: простое сравнение
newBrokerHeap > currentBrokerHeap ? RESIZE_THEN_UPDATE : UPDATE_THEN_RESIZE

// controller: если любой из heap null → UPDATE_THEN_RESIZE, иначе сравнение
if (newControllerHeap == null || currentControllerHeap == null) return UPDATE_THEN_RESIZE;
return newControllerHeap > currentControllerHeap ? RESIZE_THEN_UPDATE : UPDATE_THEN_RESIZE;

// cruise: константа
cruiseOrder = UPDATE_THEN_RESIZE;
```

Why: cruise heap не хранится в `KafkaClusterParams` (data model) — сравнивать не с чем. Решено всегда возвращать `UPDATE_THEN_RESIZE` (безопасный дефолт, processing и так его применяет для cruise).

## Находка: cruise heap дропается (открыто)

mdb-data отправляет `cruiseHeapSizeMB=4095` в `ModifyKafkaClusterDto`, но mdb-processing его не использует. В `ModifyKafkaClusterInputData`, `KafkaResizeBrokerInputData`, `UpdateBrokerConfigInputData`, `UpdateControllerConfigInputData` нет поля для cruise heap. Требует доработки на стороне processing.

## Запуск инфраструктуры

- `docker compose up -d pg_backstage` + `bootRun mdb-data --server.port=8081`
- `cd mdb-processing/localrun && ./localrun.sh` + `bootRun mdb-processing` (8080)
- Seed: используется существующий кластер `9e0336c7-...` (создан в предыдущих запусках)

## Повторный запуск

```sql
-- отмена незавершённых операций
UPDATE operations SET status='canceled'
WHERE cluster_id='9e0336c7-50da-4487-8746-d332357180d3' AND status IN ('in_progress','failed');
-- удаление draft-версии
DELETE FROM db_cluster_version
WHERE cluster_id='9e0336c7-50da-4487-8746-d332357180d3' AND status='draft';
```

```bash
# cleanup temporal workflows (опционально)
curl -s -X DELETE "http://localhost:8233/api/v1/namespaces/default/workflows/<workflowId>"
```
