---
name: prod-log-investigator
description: Используй этот скилл, когда нужно скачать и проанализировать прод-логи mdb-data или mdb-processing для разбора ошибок (502/500/NPE и т.п.). Скачивает логи со всех прод-ДЦ через скилл mcc-host-access, фильтрует шум, ищет stacktrace-ы.
allowed-tools: [bash, read_file, write_file, edit_file]
---

# Скилл для сбора и анализа прод-логов MDB Data и MDB Processing

Ты работаешь в режиме расследования инцидентов: пользователь дал ошибку (обычно 502/500 из UI), нужно найти её причину в прод-логах.

> Доступ к хостам и копирование файлов — через скилл [`mcc-host-access`](../mcc-host-access/SKILL.md).
> Ниже — только специфика прод-логов mdb-data / mdb-processing.

## Прод-ДЦ

На проде **4 ДЦ**: `hc`, `pc`, `uc`, `kc`. ДЦ `rc` — не продовый, его логи для прода не релевантны.

Хосты: `{1,2}.mdb-data.mdb-data.{hc,pc,uc,kc}.one-infra.ru` и `{1,2}.mdb-processing.java.{hc,pc,uc,kc}.one-infra.ru`.

## one-cloud-ops

`one-cloud-ops` — оператор кластеров, дёргает mdb-data через `KafkaSyncApi` / sync-таски (`KafkaSyncMdbStateTask` и т.п.). Если для кластера не идёт sync (не обновляются users/topics в mdb-data) — чаще всего кластер просто не зарегистрирован в one-cloud-ops, а не баг в коде.

### Namespace и ДЦ

`one-cloud-ops` деплоится в нескольких namespace, каждый со своим набором ДЦ. **Namespace и ДЦ между собой не связаны** — оператор может стоять в любой комбинации, нужно проверять все варианты.

Известные namespace (из CI-деплоев):

| Namespace | ДЦ, где есть оператор |
|---|---|
| `vkontakte` | `nc`, `ic`, `zc` (по деплою), реально отвечает в `kc`, `dc`, `nc`, `ic`, `zc` |
| `dzen` | `dc`, `ec`, `kc`, `pc`, `sc` (по деплою), отвечает во всех + `hc` |
| `infra` | используется для некоторых mdb-кластеров |

Service name на всех — `cdb.cloud-ops.batch` (queue `cloud-ops.batch`), URL API — `https://cdb.cloud-ops.clouds.vkcl.ru/...`.

⚠️ С бекстейджа/локальной машины DNS `cdb.cloud-ops.clouds.vkcl.ru` может не резолвиться для ДЦ namespace `vkontakte` → `dial tcp: i/o timeout`. Если нужен этот namespace — запускать проверку с хоста, у которого есть доступ, либо через UI one-cloud-ops.

### Проверка регистрации кластера

Проверка делается через скилл [`mcc-host-access`](../mcc-host-access/SKILL.md) (команда `ops` с флагами `-n <namespace> -c <dc>`):
- `cluster-id` — UUID, `cluster-name` — например `test-43version-4-mdbdev-kafka`.

Маркеры ответа:
- `EntityNotFoundException: Partition <cluster> is not managed by both one-cloud-ops and ops-temporal` — кластер не зарегистрирован в этом (ДЦ, namespace).
- `Not found ops by namespace <ns>` — в этом ДЦ оператор с таким namespace вообще не установлен.
- `Failed to create one-cloud client: Namespace cannot be resolved from <ns>` — namespace не зарегистрирован в PMS для этого ДЦ.
- `dial tcp: lookup cdb.cloud-ops.clouds.vkcl.ru: i/o timeout` — оператор есть, но с этой машины до него не достучаться (DNS/сеть).
- Без `-n <namespace>` падает `NamespaceMissingException`.

Так как namespace и ДЦ независимы — проверять нужно декартово: все namespace × все ДЦ, пока не найдётся тот, где кластер зарегистрирован.

### Если sync не идёт

1. Проверить регистрацию через скилл [`mcc-host-access`](../mcc-host-access/SKILL.md) (команда `ops`) по всем комбинациям namespace × ДЦ (см. выше).
2. Если нигде не зарегистрирован — проблема не в коде one-cloud-ops, деплой новой версии не поможет. Кластер нужно засабмитить (manifest типа kafka в нужный namespace) или уточнить namespace у команды оператора.
3. Если зарегистрирован — смотреть логи one-cloud-ops и sync-таски (`KafkaSyncMdbStateTask`, task name="sync", critical=true) на хосте оператора.

## Где лежат логи

| Сервис | Путь на хосте | Что искать |
|---|---|---|
| mdb-data | `/mnt/logs/mdb-data.err.log` | stacktrace с `at one.cloud.mdb.data...` |
| mdb-processing | `/one/logs/` (директория) | `java.log` (текущий) + `java.log.{1..6}.gz` (ротация) |

⚠️ `/mnt/logs/` на mdb-processing пустой — логи в `/one/logs/`.

## Скачивание

Скачать логи mdb-data (`/mnt/logs/mdb-data.err.log` с хостов `{1,2}.mdb-data.mdb-data.{hc,pc,uc,kc}.one-infra.ru`) и mdb-processing (директорию `/one/logs/` с хостов `{1,2}.mdb-processing.java.{hc,pc,uc,kc}.one-infra.ru`) — через скилл [`mcc-host-access`](../mcc-host-access/SKILL.md) (команда `scp`, см. `commands/scp.md` для шаблона массового скачивания по списку хостов × ДЦ).

**Только `scp`** — `ssh` не принимает аргументы с пробелами/пайпами, не используй его.
Подробнее про доступ к хостам и грабли — скилл [`mcc-host-access`](../mcc-host-access/SKILL.md).

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
