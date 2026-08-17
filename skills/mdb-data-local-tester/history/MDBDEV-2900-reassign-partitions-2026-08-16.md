# MDBDEV-2900 — Kafka partition reassign workflow — 2026-08-16

Тест workflow `reassignKafkaPartitions` (новый, отдельный от modify) на кластере `test-cruise5` (mdbdev, kafka 3.8). Workflow делает execute через AdminClient API: `describePartitions` → round-robin plan → `alterPartitionReassignments`. Verify — отдельный синхронный REST endpoint.

## Кластер

| Поле | Значение |
|---|---|
| cluster_id | `789b22f3-7923-4dbb-b9e4-c049e19d203c` |
| name | test-cruise5 |
| type | kafka |
| namespace | infra |
| queue | test-cruise5-mdbdev-kafka |
| brokers | 8 хостов (1.broker...dc/ic/pc/uc, 2.broker...dc/pc, 3.broker...dc, 4.broker...dc) |
| docker | ubuntu20-kafka-3.8.0:2.4.3 |

## Что тестируется

1. `POST /api/v1/mdb/processing/kafka/clusters/{id}/hosts/reassign-partitions` — запускает temporal workflow
2. `POST /api/v1/mdb/processing/kafka/clusters/{id}/hosts/reassign-partitions/verify` — синхронный REST, возвращает партиции в активном reassign'е

Workflow `ReassignKafkaPartitionsWorkflowImpl`:
- `describePartitions` activity → `PartitionReplicasDto` list
- `validateTargetBrokers` (non-retryable `REASSIGN_INVALID_TARGET_BROKERS` если count < max RF)
- `buildRoundRobinPlan` — round-robin по targetBrokerIds с offset = partition % brokerCount
- `alterPartitionReassignments` activity → запуск reassign'а

## Подготовка локальной инфры

### 1. Vault секреты

Локальный vault на :8200 (token=root), namespace=infra → `app.kafka.namespaces.infra.ssl-truststore-location`. Vault path формат: `mdb/mdbdev/kafka/<fullQueue>/super` (kv-v2 на mount `mdb/`).

Заливка super user пароля (из прода onesecret `mdb/mdbdev/kafka/test-cruise5-mdbdev-kafka.mdbdev.db.production.mdb.prod/super`):
```bash
export VAULT_ADDR='http://localhost:8200' VAULT_TOKEN='root'
vault secrets enable -path=mdb -version=2 kv 2>/dev/null || true
vault kv put 'mdb/mdbdev/kafka/test-cruise5-mdbdev-kafka.mdbdev.db.production.mdb.prod/super' \
  password='<SUPER_USER_PASSWORD>'
```

### 2. SSL truststore

`KafkaConnectionPropertiesConverter` ставит `ssl.truststore.type=PEM` и `ssl.truststore.location` из `app.kafka.namespaces.infra.ssl-truststore-location`. В `application.yaml` поле пустое → Kafka client падает с `Failed to load PEM SSL keystore` / `Is a directory`.

Решение — скачать CA cert с broker хоста и прописать путь:
```bash
mkdir -p /tmp/kafka-secrets
mcc --local -n infra scp 1.broker.test-cruise5-mdbdev-kafka.dc.one-infra.ru:/opt/kafka/ssl/tls_ca.crt /tmp/kafka-secrets/
cp /tmp/kafka-secrets/tls_ca.crt ~/.mccloud/kafka-tls-ca.crt
```

В `src/main/resources/application-local.yaml` добавить:
```yaml
app:
  kafka:
    namespaces:
      infra:
        ssl-truststore-location: ${HOME}/.mccloud/kafka-tls-ca.crt
```

### 3. Temporal workflow config

В `src/main/resources/application.yaml`, `deploy/mdb-processing/templates/etc/application.yaml.j2`, `src/test/resources/application-test.yaml` — добавить в `app.temporal.workflow-options`:
```yaml
kafka-reassign-partitions-workflow:
  task-queue: *kafka-activities-queue
  id-reuse-policy: "WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE_FAILED_ONLY"
```

Без этого — `Missing temporal workflow config for type: kafka-reassign-partitions-workflow` при попытке запуска.

## Запросы

### Execute (запуск workflow)

```bash
BROKERS="1.broker.test-cruise5-mdbdev-kafka.dc.one-infra.ru:9092,1.broker.test-cruise5-mdbdev-kafka.ic.one-infra.ru:9092,2.broker.test-cruise5-mdbdev-kafka.dc.one-infra.ru:9092"
OP_ID=$(uuidgen | tr 'A-Z' 'a-z')

curl -X POST "http://localhost:8080/api/v1/mdb/processing/kafka/clusters/789b22f3-7923-4dbb-b9e4-c049e19d203c/hosts/reassign-partitions" \
  -H "Content-Type: application/json" \
  -d "{
    \"operationId\": \"$OP_ID\",
    \"queueInfo\": {
      \"queueName\": \"test-cruise5-mdbdev-kafka\",
      \"queueShortName\": \"test-cruise5-mdbdev-kafka\",
      \"pmsHost\": \"test-cruise5-mdbdev-kafka.clouds\",
      \"namespace\": \"INFRA\"
    },
    \"kafkaConnectionParams\": {
      \"kafkaBrokerHosts\": \"$BROKERS\",
      \"vaultPasswordPath\": \"mdb/mdbdev/kafka/test-cruise5-mdbdev-kafka.mdbdev.db.production.mdb.prod/super\"
    },
    \"topics\": [\"test1\"],
    \"targetBrokerIds\": [1, 2, 3]
  }"
```

HTTP 202 — workflow стартовал в Temporal.

### Verify (синхронный)

```bash
curl -X POST "http://localhost:8080/api/v1/mdb/processing/kafka/clusters/789b22f3-7923-4dbb-b9e4-c049e19d203c/hosts/reassign-partitions/verify" \
  -H "Content-Type: application/json" \
  -d "{
    \"namespace\": \"INFRA\",
    \"kafkaConnectionParams\": {
      \"kafkaBrokerHosts\": \"$BROKERS\",
      \"vaultPasswordPath\": \"mdb/mdbdev/kafka/test-cruise5-mdbdev-kafka.mdbdev.db.production.mdb.prod/super\"
    },
    \"topics\": [\"test1\"]
  }"
```

Возвращает `[]` — нет активных reassign'ов (HTTP 200). Это ожидаемо для verify без активного execute.

## Результат

### ✅ Что работает

- Endpoint `POST /reassign-partitions` → HTTP 202, workflow стартует в Temporal
- Endpoint `POST /reassign-partitions/verify` → HTTP 200, `[]` (пустой список — нет активных reassign'ов)
- Workflow `reassignKafkaPartitions` зарегистрирован (`kafka-reassign-partitions-workflow` type)
- Activity `describePartitions` → `ACTIVITY_TASK_COMPLETED` (eventId 7) — partition state получен
- `validateTargetBrokers` — OK (count 3 ≥ RF 3 для `test1`)
- `buildRoundRobinPlan` — построен план (workflow task 10 completed)
- Activity `alterPartitionReassignments` → `ACTIVITY_TASK_COMPLETED` (eventId 13) — reassign запущен на проде
- Workflow → `WORKFLOW_EXECUTION_STATUS_COMPLETED` (eventId 17)
- Vault auth (super user) — OK
- SSL truststore — OK (после заливки CA cert)
- Kafka connection (SASL_SSL) — OK
- `PartitionReplicasDto` conversions в activity — работают (код доходит до admin client вызовов)

### ✅ Финальный результат

Workflow `efdd44d6-3282-4d06-9fb4-ea6349a27094` — `WORKFLOW_EXECUTION_STATUS_COMPLETED`. Все 3 activity (`describePartitions`, `alterPartitionReassignments` через workflow) завершены успешно. Verify после execute возвращает `[]` — reassign на `test1` (3 partitions, RF=3) уже завершился.

### ⚠️ Важные детали

- **Broker IDs на проде — 5-значные**: `20001` (1.broker.dc), `21001` (1.broker.ic), `22001` (1.broker.uc), `23001` (1.broker.pc), `20002` (2.broker.dc), `23002` (2.broker.pc), `20003` (3.broker.dc), `20004` (4.broker.dc). Формат: `<DC_PREFIX><broker_index>` где DC_PREFIX: 20000=dc, 21000=ic, 22000=uc, 23000=pc. 4-значные (`2000`, `2100`, ...) НЕ существуют — Kafka вернёт `InvalidReplicaAssignmentException: no such broker is registered`.
- **`__consumer_offsets` не describe'ится через super user** — `UnknownTopicOrPartitionException: This server does not host this topic-partition`. Использовать обычный user-created topic (например `test1`, `dima`, `test1000`).

## Замечания по коду

- `@NullMarked` на `KafkaHostActivity` interface ломает `KafkaHostWaiter` (передаёт `@Nullable Duration` в `pingSshRestarted*InstanceReady`). Откатил `@NullMarked` с interface, оставил per-method на новых методах (`describePartitions`, `alterPartitionReassignments`, `listInProgressReassignments`).
- `KafkaAdminClient` использует стандартные Kafka типы (`Map<TopicPartition, TopicPartitionInfo>`, `Map<TopicPartition, NewPartitionReassignment>`, `Map<TopicPartition, PartitionReassignment>`). `PartitionReplicasDto` остаётся только на Temporal activity-границе (Jackson-сериализация).
- `KafkaAdminClientImpl.describePartitions` возвращает `LinkedHashMap` с детерминированным порядком (sorted by topic name).

## Файлы

- `api/.../dto/ReassignKafkaPartitionsDto.java`, `VerifyReassignKafkaPartitionsDto.java`, `PartitionReplicasDto.java`
- `src/.../kafka/workflow/reassign/ReassignKafkaPartitionsWorkflow.java` + `Impl.java`
- `src/.../kafka/activity/KafkaHostActivity.java` + `Impl.java` — 3 новых activity
- `src/.../kafka/client/KafkaAdminClient.java` + `Impl.java` — 3 новых метода (нативные Kafka типы)
- `src/.../kafka/service/KafkaHostsService.java` + `Impl.java` — `startReassignKafkaPartitions`, `verifyReassignKafkaPartitions`
- `src/.../kafka/controller/KafkaHostsController.java` — 2 handler
- `api/.../KafkaHostsProcessingApi.java` — 2 endpoint
- `src/.../kafka/workflow/ApplicationFailureTypes.java` — `REASSIGN_INVALID_TARGET_BROKERS`
- `src/main/resources/application.yaml`, `deploy/.../application.yaml.j2`, `src/test/resources/application-test.yaml` — workflow config
- `docs/kafka/reassign-partitions.md`
