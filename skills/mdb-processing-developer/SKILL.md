---
name: mdb-processing-developer
description: Используй этот скилл, когда пользователь просит написать код, реализовать функцию, создать эндпоинт, workflow или activity в проекте mdb-processing (Java 21 + Spring Boot 3 + Gradle + Temporal). Включает разбор Kafka AdminClient API для выбора между AdminClient и SSH CLI при реализации Kafka-операций.
allowed-tools: [Read, Write, Edit, Bash, Grep, Glob]
---

# Скилл разработчика mdb-processing

Проект `mdb-processing` — Temporal-worker микросервис, вызывается из `mdb-data`. Управляет workflow для разных БД (PostgreSQL, Kafka, ClickHouse, Redis). Код организован по доменам в `src/main/java/one/cloud/mdb/processing/{domain}/`.

## 🤖 Режим работы и тон

- Общайся на русском без вводных вежливых фраз.
- **Simplicity First:** минимальный код, решающий задачу. Без спекулятивных фич и абстракций для одноразового использования.
- **Think Before Coding:** перед написанием зафиксируй предположения. Если в ТЗ двусмысленность — остановись и спроси.
- **Surgical Changes:** затрагивай только то, что необходимо. Не форматируй соседний код, не удаляй старый мертвый код без запроса.

## 🛠️ Стек и стандарты

### Java 21 + Spring Boot 3.x + Gradle

- **Синтаксис:** Records для DTO/моделей на границах слоёв, Pattern Matching, Switch Expressions.
- **Классы:** все новые классы — `final` или `abstract`.
- **Переменные:** `final var` для локальных внутри методов.
- **Null-безопасность:** `@NullMarked` (package-level или class-level), `@Nullable` из `org.jspecify.annotations` для отдельных полей/параметров.
- **Lombok:** `@RequiredArgsConstructor`, `@Slf4j`, `@Builder` для records через `@Jacksonized`.
- **Коллекции:** Stream API в бизнес-логике, неизменяемые коллекции (`List.of`, `.toList()`, `Collections.emptyList()`).
- **Checkstyle:** Google Java Style, длина строк ≤120, лексикографический порядок импортов. Запуск `./gradlew check` перед завершением.

### Temporal workflow/activity паттерны

- **3-tier:** Parent workflow → Child workflow (per-DC) → Activity.
- **Идемпотентность:** проверки `currSize <= targetSize` в начале child, `ChildWorkflowUtils.ignoreAlreadyStarted` для параллельных детей.
- **Activity:** `@ActivityInterface` + `@ActivityMethod(name = "kafka_host_<name>")` с namePrefix, `@ActivityImpl(workers = "...")` на impl.
- **Workflow:** `@WorkflowInterface`, `WORKFLOW_TYPE` константа (может делиться между upscale/downscale — Temporal различает по `@WorkflowMethod` name), `DEFAULT_TTL`.
- **Ошибки:** `ApplicationFailureTypes` — константы типов в `one.cloud.mdb.processing.kafka.workflow`, бросать через `ApplicationFailure.newFailure`.
- **SSH-активности:** `cloudService.sshExec(instance, command)` возвращает stdout, поддерживает heredoc для записи файлов.

### Документация

Перед работой над темой из таблицы в `CLAUDE.md` (раздел 9) — **обязательно** прочитай соответствующий файл из `docs/`. При добавлении/изменении workflow или activity обновляй соответствующий `docs/` файл.

## 🔍 Разбор Kafka AdminClient API

Перед реализацией любой Kafka-операции в mdb-processing **сначала изучи, что есть в AdminClient** (`org.apache.kafka.clients.admin.AdminClient` / `Admin` интерфейс). Это предпочтительный путь перед SSH CLI — чище код, единые паттерны ошибок, без парсинга stdout.

### Где смотреть

1. **Интерфейс в проекте:** `src/main/java/one/cloud/mdb/processing/kafka/client/KafkaAdminClient.java` — уже обёрнутые методы.Impl — `client/impl/KafkaAdminClientImpl.java`.
2. **Исходники kafka-clients:** в gradle-кеше `~/.gradle/caches/modules-2/files-2.1/org.apache.kafka/kafka-clients/<version>/<hash>/kafka-clients-<version>-sources.jar`. Распакуй и grep по `Admin.java`:
   ```bash
   /usr/bin/unzip -o -q <sources-jar> -d /tmp/kafka-clients-src
   /usr/bin/grep -n "<keyword>" /tmp/kafka-clients-src/org/apache/kafka/clients/admin/Admin.java
   ```
3. **Паттерн вызова в activity:** `KafkaHostActivityImpl.getLeaderId` (строка ~161) — образец: `vaultPasswordService.getPassword` → `KafkaConnectionData.builder()` → `kafkaAdminClientFactory.createClient(...)` → `try-with-resources` → вызов метода → `Activity.wrap(e)` на ошибке.

### Ключевые методы AdminClient (kafka-clients 4.3.0)

| Метод | Назначение |
|---|---|
| `describeCluster().nodes()` | Список брокеров (`Node`: id, host, port). Для `--broker-list`, resolve broker id по host |
| `describeTopics(Collection<String>)` | Текущее распределение: `TopicDescription` → `TopicPartitionInfo` (replicas, leader, isr) |
| `alterPartitionReassignments(Map<TopicPartition, Optional<NewPartitionReassignment>>)` | Запустить реассигн. `NewPartitionReassignment(List<Integer> targetReplicas)`. `Optional.empty()` отменяет |
| `listPartitionReassignments(Set<TopicPartition>)` | Активные реассигны: `PartitionReassignment` (addingReplicas, removingReplicas, replicas). Пусто = завершено |
| `describeConfigs(ConfigResource)` | Конфиги топиков/брокеров (throttle и др.) |
| `incrementalAlterConfigs(Map<ConfigResource, Collection<AlterConfigOp>>)` | Изменение конфигов (для throttle при реассигне) |
| `unregisterBroker(int brokerId)` | Вывод брокера из метаданных после drain. Ловит `BrokerIdNotRegisteredException` как успех |
| `electLeaders(ElectionType, Set<TopicPartition>)` | Ручные выборы лидера |

### AdminClient vs SSH CLI — критерии выбора

**Используй AdminClient, если:**
- Нужная операция есть в API (реассигн, list topics, configs, broker nodes).
- Хочешь избежать SSH-зависимости и парсинга stdout.
- Операция вписывается в существующий паттерн `KafkaHostActivity` (vault → createClient → вызов).

**Используй SSH CLI (`kafka-reassign-partitions.sh` и т.п.), если:**
- Нужен встроенный throttle (`--throttle`) и нет желания ставить его через `incrementalAlterConfigs`.
- Нужен `--generate` (план строит сама Kafka, а не мы round-robin'ом).
- Нужны `--additional` или другие CLI-специфичные флаги.
- Операция отсутствует в AdminClient (редко).

См. также глобальный скилл `kafka-reassign-partitions` для ручного CLI-реассигна на живых кластерах.

### Паттерн добавления нового метода в KafkaAdminClient

1. **Интерфейс:** добавить метод в `KafkaAdminClient.java` с javadoc (назначение + когда использовать).
2. **Impl:** реализация в `KafkaAdminClientImpl.java` через `adminClient.<method>()`. Импорты — в лексикографическом порядке, `org.apache.kafka.common.*` отдельным блоком.
3. **Activity:** обёртка в `KafkaHostActivity` + `KafkaHostActivityImpl` по образцу `getLeaderId`. `@ActivityMethod(name = "kafka_host_<name>")`.
4. **Модель:** если метод возвращает составной тип — record в `client/model/`. Но **сначала** проверь, есть ли стандартный класс в `org.apache.kafka.common` (например, `Node` вместо своего `BrokerNode`).
5. **Workflow:** вызов через activity-интерфейс, не напрямую.

## 📋 Алгоритм работы

1. Прочитай соответствующий `docs/` файл (см. таблицу в `CLAUDE.md` раздел 9).
2. Зафиксируй предположения. Если двусмысленность — спроси.
3. Для Kafka-операций — изучи AdminClient API перед выбором подхода.
4. Сгенерируй код целиком, без плейсхолдеров.
5. `./gradlew check` перед завершением (checkstyle + компиляция).
6. Обнови `docs/` если менял workflow/activity.
