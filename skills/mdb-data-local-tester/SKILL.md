---
name: mdb-data-local-tester
description: Используй этот скилл, когда нужно локально протестировать API или workflow в MDB Data. Для запуска инфраструктуры используй команду /setup-local-mdb-data.
allowed-tools: [bash, read_file, edit_file, write_file]
---

# Скилл для тестирования локального MDB Data

Ты работаешь в режиме QA для тестирования интеграций mdb-data с другими сервисами.

## Запуск

1. **Инфраструктура mdb-data** — команда `/setup-local-mdb-data` (postgres, redis, сам mdb-data).
2. **mdb-processing + temporal** — обязателен для тестов, затрагивающих workflow (modify/resize/create кластеров). Команда `/setup-local-temporal` поднимает docker-compose (temporal, vault, kafka, wiremock) в `mdb-processing/localrun/`, затем запускает сам mdb-processing через `bootRun --args='--spring.profiles.active=local'`.

## Порты (важно!)

| Сервис | Порт | Как запустить |
|---|---|---|
| mdb-data | **8081** | `bootRun --args='--spring.profiles.active=local --server.port=8081'` |
| mdb-processing | **8080** | дефолт в `application.yaml` mdb-processing |
| temporal UI | 8233 | docker-compose mdb-processing |
| postgres (mdb-data) | 6434 | docker-compose mdb-data, контейнер `pg_backstage_plugin_mdb` |
| wiremock (processing) | 8088 | docker-compose mdb-processing |

mdb-data и mdb-processing оба по дефолту на 8080 — конфликт. Поэтому mdb-data запускать с `--server.port=8081`, а 8080 оставить под processing. В `application-local.yml` mdb-data уже есть `mdb-processing.base-url: http://localhost:8080` — это указывает на processing, не на сам mdb-data.

## История тестов

Перед написанием новых запросов проверяй готовые сценарии:
```bash
ls ~/.claude/skills/mdb-data-local-tester/history/
```

Каждый файл в `history/` содержит: кластер (cluster_id), SQL для seed БД, modify request JSON, ожидаемый workflow в temporal, найденные проблемы. Используй их как образец.

## Проверка в temporal

```bash
# Список workflow с статусами
curl -s "http://localhost:8233/api/v1/namespaces/default/workflows?pageSize=20" | \
  jq -r '.executions[] | "\(.startTime) \(.status) \(.type.name) \(.execution.workflowId)"'

# Декодировать input workflow
WORKFLOW_ID="..."
curl -s "http://localhost:8233/api/v1/namespaces/default/workflows/${WORKFLOW_ID}/history" | \
  jq -r '.history.events[0].workflowExecutionStartedEventAttributes.input.payloads[0].data' | \
  base64 -d | jq

# Ошибка упавшего workflow
curl -s "http://localhost:8233/api/v1/namespaces/default/workflows/${WORKFLOW_ID}/history" | \
  jq -r '.history.events[] | select(.eventType=="EVENT_TYPE_WORKFLOW_EXECUTION_FAILED") | .workflowExecutionFailedEventAttributes.failure'
```

Temporal UI: http://localhost:8233

## База данных

Контейнер: `pg_backstage_plugin_mdb`, БД `backstage_plugin_mdb`, пользователь `dev`.

```bash
# Список кластеров по типу
docker exec pg_backstage_plugin_mdb psql -U dev -d backstage_plugin_mdb -c \
  "SELECT id, name, type FROM db_cluster WHERE type = 'kafka';"

# Текущая версия кластера
docker exec pg_backstage_plugin_mdb psql -U dev -d backstage_plugin_mdb -c \
  "SELECT id, status, hardware_preset_id, cluster_params->'kafkaParams'->'brokerConfig' AS bc FROM db_cluster_version WHERE cluster_id = '...' ORDER BY create_ts DESC LIMIT 3;"

# Хосты
docker exec pg_backstage_plugin_mdb psql -U dev -d backstage_plugin_mdb -c \
  "SELECT id, host, params->>'dc' AS dc FROM host_state WHERE cluster_id = '...' ORDER BY id;"

# Операции
docker exec pg_backstage_plugin_mdb psql -U dev -d backstage_plugin_mdb -c \
  "SELECT id, status, type FROM operations WHERE cluster_id = '...';"
```

## Получение реальных данных кластера

Используй `/db-seed`: сгенерируй SELECT-запросы для удалённой БД, пользователь выполнит их через `mcc ssh/psql`, результат вставляется в локальную БД. Выдуманные хосты не работают — one-cloud master вернёт `404 EntityNotFoundException`.

## Правила

1. **PSQL через `-f`** — `docker exec ... <<'SQL'` (heredoc в stdin) тихо не применяет UPDATE. Копируй файл через `docker cp` и запускай `psql -f /tmp/file.sql`.
2. **enum values** в БД всегда lowercase (`kafka`, `in_progress`, `done`, `draft`).
3. **Логи**: mdb-data — `/tmp/mdb-data.log`, mdb-processing — `/tmp/mdb-processing.log`.
4. **Health**: mdb-data на 8081 возвращает `DOWN` на агрегированный `/actuator/health`, но `liveness`/`readiness` — `UP`. Это нормально, можно работать.
5. **Сохраняй историю** — каждый успешный сценарий сохраняй в `history/` с SQL, JSON и описанием workflow.

## Остановка (teardown)

Чтобы полностью остановить локальную инфраструктуру:

```bash
# 1. Java-процессы mdb-data (8081) и mdb-processing (8080)
lsof -ti:8080,8081 | xargs -r kill -9

# 2. Docker-инфраструктура mdb-data (pg + redis)
docker compose -f /Users/vl.ershov/Documents/Git/mdb-data/docker-compose.yml down

# 3. Docker-инфраструктура mdb-processing (temporal + vault + kafka + wiremock)
cd /Users/vl.ershov/Documents/Git/mdb-processing/localrun && docker compose down
```

Проверка:
```bash
docker ps --format '{{.Names}} {{.Status}}'   # должно быть пусто
lsof -iTCP:8080,8081,8233,6434,26379 -sTCP:LISTEN -P   # должно быть пусто
```

⚠️ У `pg_backstage_plugin_mdb` нет volume — после `down` данные стираются. При следующем запуске нужно заново:
- применять миграции V2–V4 (`shedlock`, `in_processing`) вручную, либо перезапустить mdb-data (flyway применит сам);
- применять seed SQL.
