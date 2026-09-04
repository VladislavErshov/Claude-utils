# modifyKafkaCluster — socLogger / tosAgent / cruiseControl validation + E2E — 2026-07-13

Тест нового единого workflow `modifyKafkaCluster` (заменил `KafkaResizeWorkflow` + `UpdateKafkaClusterWorkflow` в mdb-processing, которые раньше дёргал Backstage напрямую). Проверены: (1) валидатор `KafkaClusterModificationValidator`, (2) сквозное прохождение полей в temporal.

## Инфраструктура

- mdb-data на :8081 (`./gradlew bootRun --args='--spring.profiles.active=local --server.port=8081'`)
- mdb-processing на :8080
- temporal UI на :8233, postgres `pg_backstage_plugin_mdb` на :6434
- Endpoints: `PATCH /api/v2/mdb/kafka/clusters/{id}/modify` (mdb-data, direct)

## Кластеры (seed из prod)

| cluster_id | name | docker (service) | tosAgent | socLogger.enabled |
|---|---|---|---|---|
| 7569c837-… | test-resize | **2.4.0** | true | true |
| 9e0336c7-… | test-update-resize1 | **2.4.0** | true | true |
| 9f414331-… | test-sel-1 | **2.3.3** | false | false |

Все три — project 160 (mdbdev), namespace 2 (infra), environment production. Baseline-версия отмечена `status=scheduled` (current live), чтобы `findLatestClusterVersion` её подхватил.

Seed: `/tmp/seed_kafka_modify.sql` (3 кластера + 3 версии + 24 хоста + 9 one_cloud_meta + hardware_presets 168/169/30).

## Сценарии и результаты

| # | Кластер | Запрос | HTTP | Результат |
|---|---|---|---|---|
| **A1** | 7569c837 (2.4.0) | tosAgent=true, socLogger.enabled=true, cruise heap=5120, broker heap=2048 | 202 | ✓ workflow `modifyKafkaCluster` стартует, `socLoggerData.enabled=true`, `updateBrokerConfigData.tosAgentEnabled=true`, `updateBrokerConfigData.heapSizeMB=2048`, `cruiseUpdateConfigData.cruiseControl.jvmHeapSizeMb=5120`, `brokerResizeData=null` (heap → update, не resize) |
| **A2** | 9e0336c7 (2.4.0) | tosAgent=false (disable), socLogger.enabled=false (disable), cruise heap=5120 | 202 | ✓ `updateBrokerConfigData.tosAgentEnabled=false`, `socLoggerData.enabled=false`, `cruiseUpdateConfigData.cruiseControl.jvmHeapSizeMb=5120` |
| **B** | 9f414331 (2.3.3) | tosAgent=true | **400** | ✓ `params.kafkaParams.tosAgent`: "Включение tosAgent требует docker-образ версии DockerTagVersion[major=2, minor=4, patch=0] или выше" |
| **C** | 9f414331 (2.3.3) | socLogger.enabled=true, валидный endpoint | 202 | ✓ validator пропускает (2.3.3 >= SOC_LOGGER_MIN_DOCKER_VERSION 2.3.3), `socLoggerData.enabled=true` в workflow |
| **D** | 7569c837 (2.4.0) | socLogger.endpoint=[] | **400** | ✓ "endpoint должен содержать хотя бы один адрес" |
| **E** | 7569c837 (2.4.0) | socLogger.passwordVaultPath="" | **400** | ✓ "password vault path не должен быть пустым" |
| **F** | 7569c837 (2.4.0) | cruiseControl.jvmHeapSizeMb=7000 (>6144) | **400** | ✓ "Cruise Control JVM Heap size не должен превышать RAM Cruise Control (6 Гб)" |
| **G** | 7569c837 (2.4.0) | cruiseControl.jvmHeapSizeMb=4096 | 202 | ✓ mdb-data пропускает (нет min 4GB); `cruiseUpdateConfigData.cruiseControl.jvmHeapSizeMb=4096`. ⚠️ Backstage бы отказал (требует > 4096 строго) — подтверждает расхождение |
| **H** | 7569c837 (2.4.0) | broker jvmHeapSizeMb=14000 (>12288 и >80% RAM) | **400** | ✓ обе ошибки сразу: "JVM Heap size не должен превышать 12 Гб" + "JVM Heap size не должен превышать 80% RAM" |
| **I** | 9f414331 (2.3.3) | socLogger.endpoint=["   "] (blank) | **400** | ✓ "endpoint не должен содержать пустых значений" |

## Подтверждённые propagation-цепочки (modify → temporal `modifyKafkaCluster` input)

| Поле в ModifyKafkaClusterRequest | Поле в workflow input | Статус |
|---|---|---|
| `kafkaParams.socLogger.enabled=true` | `socLoggerData.enabled=true` | ✅ |
| `kafkaParams.socLogger.enabled=false` | `socLoggerData.enabled=false` | ✅ (disable тоже propagate) |
| `kafkaParams.socLogger.endpoint/passwordVaultPath/topic/user` | `socLoggerData.*` | ✅ |
| `kafkaParams.tosAgent=true/false` | `updateBrokerConfigData.tosAgentEnabled` | ✅ |
| `kafkaParams.jvmHeapSizeMb` | `updateBrokerConfigData.heapSizeMB` (а не brokerResizeData) | ✅ heap → update, не resize |
| `kafkaParams.cruiseControl.jvmHeapSizeMb` | `cruiseUpdateConfigData.cruiseControl.jvmHeapSizeMb` | ✅ |
| `kafkaParams.controller.controllerJvmHeapSizeMb` (без изменений) | `updateControllerConfigData=null` | ✅ no diff → no update |
| `brokerOrder/controllerOrder/cruiseOrder` | все `UPDATE_THEN_RESIZE` или `RESIZE_THEN_UPDATE` | ✅ |

## Находки

### 1. UX-баг: `DockerTagVersion.toString()` в сообщении об ошибке (сценарий B)

В сообщении: `"Включение tosAgent требует docker-образ версии DockerTagVersion[major=2, minor=4, patch=0] или выше"`.

В `KafkaClusterModificationValidator.java:230`:
```java
final var message = "Включение tosAgent требует docker-образ версии " + TOS_AGENT_MIN_DOCKER_VERSION + " или выше";
```
`DockerTagVersion` не переопределяет `toString()` → выводит default Lombok `ClassName[major=X, minor=Y, patch=Z]`. Та же проблема в `validateSocLogger` (строка 250). Стоит либо добавить `@Override toString()` в `DockerTagVersion`, либо форматировать явно.

### 2. Подтверждено расхождение с Backstage по cruise heap (сценарий G)

mdb-data: `cruiseControl.jvmHeapSizeMb=4096` проходит валидацию (проверяется только верх `<= 6144`).
Backstage: требует `> 4096` строго (`jvmHeapSizeMb > 8192 || jvmHeapSizeMb < 4096` → fail).

Это та самая находка из сравнения с `DbParamsValidator` — Java-валидатор **не проверяет нижнюю границу 4GB** для cruise heap. Если modify идёт через Backstage → отказ на стороне Backstage; если напрямую в mdb-data — проходит.

### 3. Конфликт операций от незавершённых draft-версий

После позитивного сценария новая modify-операция на том же кластере возвращала `409 Already has active or failed operation`, хотя предыдущая operation была `done`. Причина — оставшаяся `draft`-версия от предыдущего запроса. Решается `DELETE FROM db_cluster_version WHERE cluster_id=... AND status='draft'`. Это ожидаемое поведение, но стоит учесть для тестовых прогонов.

### 4. `socLoggerData` всегда propagated (даже при enabled=false)

В A2 с `socLogger.enabled=false` в workflow input всё равно приехал `socLoggerData` с `enabled=false` и всеми полями. Если processing-сторона не обрабатывает `enabled=false` как disable — это потенциальная проблема. Стоит проверить в `ModifyKafkaClusterTaskProcessor` / workflow-активностях.

## Cleanup

```sql
-- Удалить тестовые операции и draft-версии
DELETE FROM operations WHERE cluster_id IN (
  '9f414331-8ec4-4102-b4c3-0c0bbf7b326d',
  '9e0336c7-50da-4487-8746-d332357180d3',
  '7569c837-37ba-4041-9046-92329683237e'
) AND created_ts > '2026-07-13';

DELETE FROM db_cluster_version WHERE cluster_id IN (
  '9f414331-8ec4-4102-b4c3-0c0bbf7b326d',
  '9e0336c7-50da-4487-8746-d332357180d3',
  '7569c837-37ba-4041-9046-92329683237e'
) AND status='draft';
```

Запущенные temporal workflows (на 78bc3671, 2868837a, c2fced9e, de4a9bea) остаются в RUNNING/COMPLETED — не влияют на следующие тесты, но при желании можно terminate через UI :8233.

## Файлы

- Seed: `/tmp/seed_kafka_modify.sql`
- Modify requests: `/tmp/modify_{A1,A2,B,C,D,E,F,G,H,I}.json`
- Responses: `/tmp/resp_{A1,A2,B,C,D,E,F,G,H,I}.txt`
