---
name: prod-log-investigator
description: Используй этот скилл, когда нужно скачать и проанализировать прод-логи mdb-data или mdb-processing для разбора ошибок (502/500/NPE и т.п.). Скачивает логи со всех прод-ДЦ через mcc scp, фильтрует шум, ищет stacktrace-ы.
allowed-tools: [bash, read_file, write_file, edit_file]
---

# Скилл для сбора и анализа прод-логов MDB Data и MDB Processing

Ты работаешь в режиме расследования инцидентов: пользователь дал ошибку (обычно 502/500 из UI), нужно найти её причину в прод-логах.

## Прод-ДЦ

На проде **4 ДЦ**: `hc`, `pc`, `uc`, `kc`. ДЦ `rc` — не продовый, его логи для прода не релевантны.

Хосты: `{1,2}.mdb-data.mdb-data.{hc,pc,uc,kc}.one-infra.ru` и `{1,2}.mdb-processing.java.{hc,pc,uc,kc}.one-infra.ru`.

## Где лежат логи

| Сервис | Путь на хосте | Что искать |
|---|---|---|
| mdb-data | `/mnt/logs/mdb-data.err.log` | stacktrace с `at one.cloud.mdb.data...` |
| mdb-processing | `/one/logs/` (директория) | `java.log` (текущий) + `java.log.{1..6}.gz` (ротация) |

⚠️ `/mnt/logs/` на mdb-processing пустой — логи в `/one/logs/`.

## Скачивание

```bash
# mdb-data
mkdir -p ~/copied_logs_data && cd ~/copied_logs_data
for DC in hc pc uc kc; do
  for NUM in 1 2; do
    HOST="${NUM}.mdb-data.mdb-data.${DC}.one-infra.ru"
    mcc scp "$HOST:/mnt/logs/mdb-data.err.log" . 2>/dev/null
    mv mdb-data.err.log "${HOST}.log" 2>/dev/null
  done
done

# mdb-processing (качает всю директорию /one/logs/)
mkdir -p ~/copied_logs_processing && cd ~/copied_logs_processing
for DC in hc pc uc kc; do
  for NUM in 1 2; do
    HOST="${NUM}.mdb-processing.java.${DC}.one-infra.ru"
    mkdir -p "$HOST"
    mcc scp "$HOST:/one/logs/" "$HOST/" 2>/dev/null
  done
done
```

**Только `mcc scp`** — `mcc ssh` не принимает аргументы с пробелами/пайпами, не используй его.

## Анализ mdb-data логов

mdb-data логи — обычный текст, stacktrace многострочный. Ключевые маркеры:
- `ERROR ... ControllerLogAspect - FAILED: <Controller>.<method>` — вход в упавший эндпоинт
- `ResponseStatusException: 502 BAD_GATEWAY "500 ..."` — downstream (processing) вернул 5xx, mdb-data обернул
- `ResponseStatusException: 500 INTERNAL_SERVER_ERROR` — упало внутри самого mdb-data
- `at one.cloud.mdb.data...` — строки stacktrace

```bash
# Найти все 502/500 ошибки
grep -n "ResponseStatusException: 50[02]" ~/copied_logs_data/*.log | head -20

# Найти stacktrace для конкретной операции
grep -n "<operationId>" ~/copied_logs_data/*.log
```

## Анализ mdb-processing логов

mdb-processing логи — JSON в формате log4j2-spring (одна строка = один event). Поля: `@timestamp`, `level`, `logger_name`, `message`, `mdc` (с `ClusterId`, `OperationId`, `WorkflowType`, ...), `exception` (с `exception_class`, `exception_message`, `stacktrace`).

### Поиск по маркеру ошибки

`GlobalExceptionHandler.handleGenericException` пишет в `message` префикс `"Unexpected error: <exception_message>"`. Это маркер необработанной ошибки, которая вернулась как 500.

```bash
# Найти все Unexpected error, отфильтровать шум metrics-поллинга
grep "Unexpected error" ~/copied_logs_processing/*/java.log | \
  grep -v "No static resource metrics" | \
  grep -v "metrics" | head -20 | cut -c1-300
```

### Поиск по операции/кластеру

MDC содержит `OperationId` и `ClusterId` — можно искать через них. Но в логах mdb-processing operationId из mdb-data может НЕ появиться, если запрос упал до старта workflow (например, при валидации конфига).

```bash
# Поиск по clusterId (UUID кластера)
grep "<clusterId>" ~/copied_logs_processing/*/java.log

# Поиск по workflow type (temporal workflow method name)
grep "modifyKafkaCluster\|ModifyKafkaClusterWorkflow" ~/copied_logs_processing/*/java.log
```

⚠️ **Важно**: `WorkflowType` в MDC — это temporal method name (например `modifyKafkaCluster`, `modifyCluster`, `kafkaResizeBrokerInstance`). Для Kafka modify это `modifyKafkaCluster`. `modifyCluster` — это ClickHouse modify, не Kafka.

⚠️ Логи регистрации воркера (`Registering auto-discovered workflow class ...ModifyKafkaClusterWorkflowImpl`) — это старт приложения, не выполнение workflow. Не путать с реальным запуском.

### Парсинг JSON-лога

```python
python3 << 'PYEOF'
import json
with open('/path/to/java.log') as f:
    for line in f:
        idx = line.find('{')
        if idx < 0: continue
        try:
            d = json.loads(line[idx:])
        except: continue
        # фильтр
        if d.get('level') != 'ERROR': continue
        msg = d.get('message','')
        if 'No static resource' in msg: continue  # фильтр шума
        exc = d.get('exception',{})
        print(f"{d.get('@timestamp')} | {exc.get('exception_class','')} | {msg[:200]}")
        if exc.get('stacktrace'):
            print(exc['stacktrace'][:2000])
        print('---')
PYEOF
```

### Поиск в ротированных логах (.gz)

```bash
# mc логи ротируются (java.log.1.gz .. java.log.6.gz)
for f in ~/copied_logs_processing/*/java.log.*.gz; do
  echo "=== $f ==="
  gunzip -c "$f" | grep "Unexpected error" | grep -v "metrics" | head -3
done
```

## Типичные ошибки и их причины

| Симптом | Где искать | Возможная причина |
|---|---|---|
| mdb-data 502 BAD_GATEWAY | mdb-processing logs | processing вернул 5xx, смотри `Unexpected error` |
| `Missing temporal workflow config for type: X` | mdb-processing `Unexpected error` | В `deploy/mdb-processing/templates/etc/application.yaml.j2` нет ключа `X` в `workflow-options`. Этот шаблон переопределяет packaged `application.yaml` на проде. |
| NPE `Cannot invoke "X.hostname()" because "Y" is null` | mdb-processing workflow task failure | Внутри workflow, обычно данные кластера не пришли из activity |
| mdb-data 500 INTERNAL_SERVER_ERROR | mdb-data logs, stacktrace внутри | NPE в самом mdb-data (diff detector, mapper, validator) |

## Ловушка external config override

`deploy/mdb-processing/templates/etc/application.yaml.j2` рендерится при деплое и кладётся как `/etc/application.yaml`, **полностью заменяя** packaged `application.yaml`. Поэтому:

- Локально (`./gradlew bootRun`) — используется packaged yaml, все workflow-ключи есть.
- На проде — используется j2-шаблон, каких-то ключей может не быть.

Если локально работает, а на проде `Missing temporal workflow config` — сравнить ключи в `src/main/resources/application.yaml` и `deploy/mdb-processing/templates/etc/application.yaml.j2`:

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
print("В template, НЕТ в packaged:", sorted(template - packaged))
PYEOF
```

## Очистка

```bash
rm -rf ~/copied_logs_data ~/copied_logs_processing
```
