# MDBDEV-2900 — Kafka partition reassign workflow (RF change) — 2026-08-17

Тест workflow `reassignKafkaPartitions` на изменение replication factor (RF 3 → 4) для topic `test1` на кластере `test-cruise5` (mdbdev, kafka 3.8). Workflow использует AdminClient API: `describePartitions` → round-robin plan с `targetReplicationFactor` → `alterPartitionReassignments`.

## Кластер

| Поле | Значение |
|---|---|
| cluster_id | `789b22f3-7923-4dbb-b9e4-c049e19d203c` |
| name | test-cruise5 |
| type | kafka |
| namespace | infra |
| queue | test-cruise5-mdbdev-kafka |
| brokers | 8 хостов (20001-20004, 21001, 22001, 23001, 23002) |
| docker | ubuntu20-kafka-3.8.0:2.4.3 |

## Что тестируется

`POST /api/v1/mdb/processing/kafka/clusters/{id}/partitions/reassign` — endpoint в новом контроллере `KafkaPartitionsController` (после рефакторинга, вынесенного из `KafkaHostsController`).

DTO (актуальное состояние после рефакторинга):
```java
record ReassignKafkaPartitionsDto(
    UUID operationId,
    QueueInfoDto queueInfo,
    KafkaConnectionParamsDto connectionParams,   // переименовано из kafkaConnectionParams
    List<String> topics,
    List<Integer> targetBrokerIds,
    @Nullable @Positive Integer targetReplicationFactor,  // новый — если null, RF сохраняется
    @Nullable Duration workflowTtl
) {}
```

Workflow `ReassignKafkaPartitionsWorkflowImpl`:
- `describePartitions` activity → текущие `PartitionReplicasDto`
- `validateTargetBrokers` — `targetBrokerIds.size() >= (targetRf != null ? targetRf : currentMaxRf)`, иначе non-retryable `REASSIGN_INVALID_TARGET_BROKERS`
- `buildRoundRobinPlan` — для каждой партиции:
  - `replicaCount = targetRf != null ? targetRf : dto.replicas().size()`
  - `offset = partition % brokerCount`
  - `newReplicas[i] = targetBrokerIds.get((offset + i) % brokerCount)`
- `alterPartitionReassignments` activity → запуск reassign'а

## Подготовка локальной инфры

### Vault секреты

Локальный vault на :8200 (token=root), namespace=infra → `app.kafka.namespaces.infra.ssl-truststore-location`. Vault path: `mdb/mdbdev/kafka/<fullQueue>/super` (kv-v2 на mount `mdb/`).

```bash
export VAULT_ADDR='http://localhost:8200' VAULT_TOKEN='root'
vault secrets enable -path=mdb -version=2 kv 2>/dev/null || true
vault kv put 'mdb/mdbdev/kafka/test-cruise5-mdbdev-kafka.mdbdev.db.production.mdb.prod/super' \
  password='<SUPER_USER_PASSWORD>'
```

### SSL truststore

`KafkaConnectionPropertiesConverter` ставит `ssl.truststore.type=PEM` и `ssl.truststore.location` из `app.kafka.namespaces.infra.ssl-truststore-location`. Скачиваем CA cert с broker хоста:

```bash
mkdir -p /tmp/kafka-secrets
mcc --local -n infra scp 1.broker.test-cruise5-mdbdev-kafka.dc.one-infra.ru:/opt/kafka/ssl/tls_ca.crt /tmp/kafka-secrets/
cp /tmp/kafka-secrets/tls_ca.crt ~/.mccloud/kafka-tls-ca.crt
```

В `src/main/resources/application-local.yaml`:
```yaml
app:
  kafka:
    namespaces:
      infra:
        ssl-truststore-location: ${HOME}/.mccloud/kafka-tls-ca.crt
```

### Temporal workflow config

В `src/main/resources/application.yaml`, `deploy/mdb-processing/templates/etc/application.yaml.j2`, `src/test/resources/application-test.yaml` — в `app.temporal.workflow-options`:
```yaml
kafka-reassign-partitions-workflow:
  task-queue: *kafka-activities-queue
  id-reuse-policy: "WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE_FAILED_ONLY"
```

## Запрос RF increase 3→4

### Execute

```bash
BROKERS="1.broker.test-cruise5-mdbdev-kafka.dc.one-infra.ru:9092,1.broker.test-cruise5-mdbdev-kafka.ic.one-infra.ru:9092,2.broker.test-cruise5-mdbdev-kafka.dc.one-infra.ru:9092"
OP_ID=$(uuidgen | tr 'A-Z' 'a-z')

curl -X POST "http://localhost:8080/api/v1/mdb/processing/kafka/clusters/789b22f3-7923-4dbb-b9e4-c049e19d203c/partitions/reassign" \
  -H "Content-Type: application/json" \
  -d "{
    \"operationId\": \"$OP_ID\",
    \"queueInfo\": {
      \"queueName\": \"test-cruise5-mdbdev-kafka\",
      \"queueShortName\": \"test-cruise5-mdbdev-kafka\",
      \"pmsHost\": \"test-cruise5-mdbdev-kafka.clouds\",
      \"namespace\": \"INFRA\"
    },
    \"connectionParams\": {
      \"kafkaBrokerHosts\": \"$BROKERS\",
      \"vaultPasswordPath\": \"mdb/mdbdev/kafka/test-cruise5-mdbdev-kafka.mdbdev.db.production.mdb.prod/super\"
    },
    \"topics\": [\"test1\"],
    \"targetBrokerIds\": [20001, 20002, 20003, 20004],
    \"targetReplicationFactor\": 4
  }"
```

HTTP 202 — workflow стартовал в Temporal.

## Результат

### ✅ Что работает

- Endpoint `POST /partitions/reassign` → HTTP 202, workflow стартует в Temporal
- Workflow `reassignKafkaPartitions` зарегистрирован (`kafka-reassign-partitions-workflow` type)
- `describePartitions` → ACTIVITY_TASK_COMPLETED (eventId 7)
- `validateTargetBrokers` — OK (4 брокера ≥ RF 4)
- `buildRoundRobinPlan` с `targetReplicationFactor=4` — план построен (workflow task 10 completed)
- `alterPartitionReassignments` → ACTIVITY_TASK_COMPLETED (eventId 13)
- Workflow → `WORKFLOW_EXECUTION_STATUS_COMPLETED` (eventId 17)
- Vault auth (super user) — OK
- SSL truststore — OK
- Kafka connection (SASL_SSL) — OK

### ✅ Финальный результат на test1 (RF 3→4)

```
Topic: test1	Partition: 0	Leader: 20001	Replicas: 20001,20002,20003,20004	Isr: 20001,20002,20003,20004
Topic: test1	Partition: 1	Leader: 20002	Replicas: 20002,20003,20004,20001	Isr: 20003,20002,20001,20004
Topic: test1	Partition: 2	Leader: 20003	Replicas: 20003,20004,20001,20002	Isr: 20003,20001,20002,20004
```

Все 4 реплики в ISR — Kafka успела догнать нового брокера (20004). Workflow `28488f57-5eb3-4ca7-90dc-7c50ecb76560` → `WORKFLOW_EXECUTION_STATUS_COMPLETED`.

Round-robin план полностью совпал с ожиданием:
- P0 (offset=0): `[20001, 20002, 20003, 20004]` ✓
- P1 (offset=1): `[20002, 20003, 20004, 20001]` ✓
- P2 (offset=2): `[20003, 20004, 20001, 20002]` ✓

### ⚠️ Важные детали

- **`targetBrokerIds` должен быть `List`, не `Set`** — `HashSet` рандомит порядок, round-robin ломается. На предыдущих тестах при `Set<Integer>` 3 брокера из 8 вообще не попали ни в одну партицию.
- **`connectionParams` (не `kafkaConnectionParams`)** — поле переименовано в DTO и Request после рефакторинга.
- **Broker IDs на проде — 5-значные**: `20001` (1.broker.dc), `21001` (1.broker.ic), `22001` (1.broker.uc), `23001` (1.broker.pc), `20002` (2.broker.dc), `23002` (2.broker.pc), `20003` (3.broker.dc), `20004` (4.broker.dc). Формат: `<DC_PREFIX><broker_index>` где DC_PREFIX: 20000=dc, 21000=ic, 22000=uc, 23000=pc.
- **`__consumer_offsets` не describe'ится через super user** — `UnknownTopicOrPartitionException`. Использовать user-created topic.
- **RF shrink**: для уменьшения RF (напр. 4→2) — `targetReplicationFactor=2` + `targetBrokerIds` с 2+ брокерами. Kafka убирает последние реплики из ISR. Безопасно если `min.insync.replicas <= newRF`.

## Рефакторинг (отдельно от RF change)

В этой же задаче сделан рефакторинг:
- Создан `KafkaPartitionsController` + `KafkaPartitionsService` + `KafkaPartitionsProcessingApi` (отдельно от `KafkaHosts*`)
- Удалён `verifyReassignKafkaPartitions` endpoint (и весь слой: service, DTO `VerifyReassignKafkaPartitionsDto`, activity `listInProgressReassignments`, client method `listInProgressReassignments`)
- Удалён неиспользуемый `KafkaAdminClient.describeClusterNodes()`
- URL изменён: `POST /hosts/reassign-partitions` → `POST /partitions/reassign`

## Файлы

- `api/.../dto/ReassignKafkaPartitionsDto.java` — DTO с `targetReplicationFactor`, `connectionParams`, `workflowTtl`
- `api/.../KafkaPartitionsProcessingApi.java` — новый API interface
- `src/.../kafka/controller/KafkaPartitionsController.java` — новый контроллер
- `src/.../kafka/service/KafkaPartitionsService.java` + `Impl.java` — новый сервис
- `src/.../kafka/model/reassign/ReassignKafkaPartitionsRequest.java` — request model
- `src/.../kafka/workflow/reassign/ReassignKafkaPartitionsWorkflow.java` + `Impl.java`
- `src/.../kafka/activity/KafkaHostActivity.java` + `Impl.java` — 2 activity (describePartitions, alterPartitionReassignments)
- `src/.../kafka/client/KafkaAdminClient.java` + `Impl.java` — 2 метода (нативные Kafka типы)
- `src/.../kafka/workflow/ApplicationFailureTypes.java` — `REASSIGN_INVALID_TARGET_BROKERS`
- `src/main/resources/application.yaml`, `deploy/.../application.yaml.j2`, `src/test/resources/application-test.yaml` — workflow config
- `docs/kafka/reassign-partitions.md`
