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
3. **Backstage НЕ нужен** для базовых тестов modify-флоу. mdb-data сам стартует temporal workflow через processing. Backstage (`/setup-local-backstage`) поднимай только если тестируешь именно Backstage-слой (POST /version/, task chain generators, Redis-кеш проектов).

## Порты (важно!)

| Сервис | Порт | Как запустить |
|---|---|---|
| mdb-data | **8081** | `bootRun --args='--spring.profiles.active=local --server.port=8081'` |
| mdb-processing | **8080** | дефолт в `application.yaml` mdb-processing |
| temporal UI | 8233 | docker-compose mdb-processing |
| postgres (mdb-data) | 6434 | docker-compose mdb-data, контейнер `pg_backstage_plugin_mdb` |
| wiremock (processing) | 8088 | docker-compose mdb-processing |

mdb-data и mdb-processing оба по дефолту на 8080 — конфликт. Поэтому mdb-data запускать с `--server.port=8081`, а 8080 оставить под processing. В `application-local.yml` mdb-data уже есть `mdb-processing.base-url: http://localhost:8080` — это указывает на processing, не на сам mdb-data.

## Auth отключён в local-профиле

В `application-local.yml:59` стоит `mdb.auth.enabled: false`. **JWT/токен для запросов к mdb-data НЕ нужен** — шли прямые curl без `Authorization` заголовка. Проверено: `PATCH /api/v2/mdb/kafka/clusters/{id}/modify` без токена возвращает 202 и стартует temporal workflow.

Таблица `services_auth` нужна только если включить auth (или для тестов Backstage, где JWT签ится с `serviceName` из этой таблицы). Для прямых запросов к mdb-data — не требуется.

## Структура request body для PATCH /modify

Эндпоинт: `PATCH http://localhost:8081/api/v2/mdb/kafka/clusters/{id}/modify`

Тело — НЕ плоский `ModifyKafkaClusterParams`, а обёртка `ModifyKafkaClusterRequest`:
```json
{
  "params": {
    "acl": {}, "name": "...", "isWan": false,
    "lanIn": 10, "lanOut": 15, "diskGb": 8, "diskType": "nvme",
    "projectId": 160, "rootQueue": "prod",
    "needLanIpv6": true, "needWanIpv4": false, "needWanIpv6": false,
    "kafkaParams": {
      "controller": { "controllerDcs": ["dc","hc","kc"], "controllerConfig": {"config": {}}, ... },
      "brokerConfig": {"config": {}},
      "jvmHeapSizeMb": 1024,
      "cruiseControl": {"cruiseControlDc": "hc", "cruiseUserPassword": ""},
      "tosAgent": true,
      "socLogger": {"enabled": true}
    }
  },
  "hardwarePresetId": 100,
  "isNeedShards": false,
  "hosts": [{"dc": "dc"}, {"dc": "hc"}, {"dc": "kc"}],
  "type": "update_instances",
  "attempts": 3
}
```

Без `params`/`hardwarePresetId`/`attempts`/`hosts`/`type` на верхнем уровне → 400 "не должно равняться null".

**Важно про baseline `cluster_params`**: перед modify в baseline `db_cluster_version` должны быть `kafkaParams.brokerConfig.config={}` и `kafkaParams.controller.controllerConfig.config={}` (пусть пустые). Иначе `KafkaClusterDiffDetector` падает с NPE на `currentParams.brokerConfig().config()`.

## Mapping: request → temporal workflow input

После 202 mdb-data стартует temporal workflow `modifyKafkaCluster`. Его input (декодируется через `history.events[0].workflowExecutionStartedEventAttributes.input.payloads[0].data` | base64 -d | jq) содержит секции, которые **могут быть null** — это нормально, если в запросе не было изменений:

| Request field | Temporal input field | Когда null |
|---|---|---|
| `kafkaParams.tosAgent` | `updateBrokerConfigData.tosAgentEnabled` | поле не пришло в request |
| `kafkaParams.socLogger` | `socLoggerData` | поле не пришло в request |
| `kafkaParams.cruiseControl.autoRebalanceEnabled` | `cruiseUpdateConfigData.cruiseControl.autoRebalanceEnabled` | поле не пришло в request |
| `kafkaParams.cruiseControl.autoRebalanceOnBrokerFailEnabled` | `cruiseUpdateConfigData.cruiseControl.autoRebalanceOnBrokerFailEnabled` | поле не пришло в request |
| `kafkaParams.jvmHeapSizeMb` | `updateBrokerConfigData.heapSizeMB` | передаётся всегда (или когда brokerConfigDiff=true) |
| `kafkaParams.controller.controllerJvmHeapSizeMb` | `updateControllerConfigData.heapSizeMB` | controllerHeap не изменился → processing-side mapper опускает **весь** `updateControllerConfigData` |
| `kafkaParams.controller.controllerConfig` | `updateControllerConfigData.parameters` | controllerConfigDiff=false → весь блок null |
| `kafkaParams.brokerConfig` | `updateBrokerConfigData.parameters` | brokerConfigDiff=false → параметры пустые, но блок остаётся |

**Следствие**: `updateControllerConfigData: null` целиком — норма, если controller heap и controllerConfig не поменялись. `socLoggerData: null` — норма, если в запросе не было socLogger. Не путать с "propagation сломалось".

**Toggle-фичи (mdb-data `KafkaClusterModificationValidator`)**:
- `tosAgent=true` → требует docker ≥ 2.4.0
- `socLogger.enabled=true` → требует docker ≥ 2.3.3
- `cruiseControl.autoRebalanceEnabled` / `autoRebalanceOnBrokerFailEnabled` — без docker-чеков, можно свободно toggling
- Выключение (`false`) — без проверок

Для проверки каждой toggle-фичи нужен **отдельный** modify-запрос, меняющий только нужное поле. Комбинировать можно, но тогда в temporal input приедут все сразу.

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

Используй `/db-seed`: сгенерируй SELECT-запросы для удалённой БД, пользователь выполнит их на удалённом хосте (через скилл [`mcc-host-access`](../mcc-host-access/SKILL.md), `mcc ssh` + `psql`), результат вставляется в локальную БД. Выдуманные хосты не работают — one-cloud master вернёт `404 EntityNotFoundException`.

## Проверка PMS-переменных (modify-флоу в mdb-processing)

После modify-операции проверить, что флоу реально записал PMS-переменные (`kafka.soc.audit.*`, `kafka.sysconfig`, `kafka.cruisecontrol.*` и т.д.) — используй скилл **`kafka-config-inspector`**. Там же — сверка PMS с отрендеренными конфиг-файлами на хостах.

⚠️ **ВНИМАНИЕ: local-профиль mdb-processing пишет в РЕАЛЬНЫЙ `pms.cloud.vk.team`, не в
wiremock!** Bean `pmsRestClient` (`PmsAutoConfiguration.java:36`) берёт `baseUrl` из
`backend.mdb.baseUrl=https://pms.cloud.vk.team`, а не из `external.api.namespaces.infra.pms.base-url`.
mTLS-сертификат из `~/.mccloud/` работает — modify-флоу реально модифицирует прод-PMS.

**Следствие**: тестируй только на dev-кластерах (`test-resize`, `test-update-resize1`,
`test-sel-1` и т.п. — project 160, mdbdev). Никогда не запускай modify-флоу локально
против прода. Снапшот PMS до modify помогает отличить изменения от нашего флоу vs. фоновых прод-операций.

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
