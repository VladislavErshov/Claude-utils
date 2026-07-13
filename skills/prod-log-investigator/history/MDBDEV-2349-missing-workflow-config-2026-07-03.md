# MDBDEV-2349: Missing temporal workflow config (kafka-modify-cluster-workflow)

Симптом: modify Kafka-кластера через UI на проде падает с `502 BAD_GATEWAY "500 Internal Server Error"` без деталей. Локально с теми же данными работает.

## Кластер

- **cluster_id**: `0964c579-1f1b-4595-9c28-84dc783d2a29`
- **name**: `test-modify`, type `kafka`
- **operations** (упавшие): `58d7ae32-6f3c-4834-b12c-5b10ee187a60`, `9ee042b9-fc8d-4a2e-a2d3-036c33bea6fd`

## Поиск ошибки

### Шаг 1: mdb-data логи

mdb-data stacktrace показал только `502 BAD_GATEWAY` от `KafkaClusterModificationServiceImpl.modifyKafkaCluster:129` — это `catch (RestClientException)`, значит processing вернул 5xx. Реальной причины в mdb-data логах нет.

### Шаг 2: mdb-processing логи

В `/one/logs/java.log` на `1.mdb-processing.java.hc.one-infra.ru` найден stacktrace:

```
java.lang.IllegalArgumentException: Missing temporal workflow config for type: kafka-modify-cluster-workflow
  at WorkflowOptionsBuilder.lambda$optionsForOperation$1(WorkflowOptionsBuilder.java:55)
  at Optional.orElseThrow(Optional.java:403)
  at WorkflowOptionsBuilder.optionsForOperation(WorkflowOptionsBuilder.java:54)
  at WorkflowLauncher.launch(WorkflowLauncher.java:34)
  at KafkaServiceImpl.startModifyCluster(KafkaServiceImpl.java:226)
  at KafkaController.modifyCluster(KafkaController.java:123)
```

`GlobalExceptionHandler.handleGenericException` вернул generic `{"status":500,"message":"An internal server error occurred."}` — mdb-data обернул в 502.

## Почему локально работало

`deploy/mdb-processing/templates/etc/application.yaml.j2` — Jinja2-шаблон, который рендерится при деплое и кладётся как `/etc/application.yaml`, **полностью заменяя** packaged `application.yaml` из JAR.

- **Локально** (`./gradlew bootRun`): используется packaged `src/main/resources/application.yaml`, где `kafka-modify-cluster-workflow` есть (добавлен в коммите `dbd751bd` от 2026-06-05).
- **На проде**: используется отрендеренный `application.yaml.j2`, где этого ключа НЕ было.

Сравнение ключей:

```bash
cd /Users/vl.ershov/Documents/Git/mdb-processing
python3 << 'PYEOF'
import re
def extract(path):
    with open(path) as f: content = f.read()
    m = re.search(r'\n    workflow-options:\n(.*?)(?=\n    [a-z]|\Z)', content, re.DOTALL)
    return set(re.findall(r'\n      ([a-z][a-z0-9-]*):', m.group(1))) if m else set()
packaged = extract('src/main/resources/application.yaml')
template = extract('deploy/mdb-processing/templates/etc/application.yaml.j2')
print("В packaged, НЕТ в template:", sorted(packaged - template))
# → ['kafka-controller-workflow', 'kafka-modify-cluster-workflow',
#    'kafka-update-controller-config-workflow', 'redis-cluster-workflow']
PYEOF
```

## Фикс

Добавлены 3 Kafka workflow-ключа в `deploy/mdb-processing/templates/etc/application.yaml.j2` (redis-cluster-workflow не трогали):

```yaml
kafka-update-controller-config-workflow:
  task-queue: *kafka-activities-queue
  id-reuse-policy: "WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE_FAILED_ONLY"
kafka-controller-workflow:
  task-queue: *kafka-activities-queue
  id-reuse-policy: "WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE_FAILED_ONLY"
kafka-modify-cluster-workflow:
  task-queue: *kafka-activities-queue
  id-reuse-policy: "WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE_FAILED_ONLY"
```

Дополнительно: `GlobalExceptionHandler` теперь возвращает реальное сообщение для `IllegalArgumentException` (а не generic "An internal server error occurred.") — для будущей отладки.

## Ветка

`ershov/MDBDEV-2349-improve-missing-workflow-config-error` в mdb-processing, 2 коммита:
1. `MDBDEV-2349 Return actual message for IllegalArgumentException`
2. `MDBDEV-2349 Add missing kafka workflow configs to deploy template`

## Уроки

1. **External config override** — проверяй не только packaged `application.yaml`, но и deploy-шаблоны (`deploy/*/templates/etc/application*.j2`). Они полностью заменяют packaged конфиг.
2. **Generic 500 скрывает причину** — `GlobalExceptionHandler.handleGenericException` пишет реальный stacktrace в лог, но в HTTP-ответ отдаёт hardcoded текст. mdb-data получает только текст, не stacktrace. Нужно идти в логи mdb-processing.
3. **Локально ≠ прод** — `bootRun` использует packaged yaml, прод — j2-шаблон. Конфиг-расхождения не видны локально.
