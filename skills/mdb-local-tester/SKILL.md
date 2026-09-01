---
name: mdb-local-tester
description: Используй этот скилл, когда нужно локально протестировать стек MDB — API/workflow mdb-data + mdb-processing, или UI (репозиторий mdb) против локального бэкенда. Для запуска инфраструктуры используй команду /setup-local-mdb-data.
allowed-tools: [bash, read_file, edit_file, write_file]
---

# Скилл для локального тестирования стека MDB (mdb-data + mdb-processing + UI)

Ты работаешь в режиме QA для тестирования интеграций mdb-data с другими сервисами.

## Запуск

1. **Инфраструктура mdb-data** — команда `/setup-local-mdb-data` (postgres, redis, сам mdb-data).
2. **mdb-processing + temporal** — обязателен для тестов, затрагивающих workflow (modify/resize/create кластеров). Команда `/setup-local-temporal` поднимает docker-compose (temporal, vault, kafka, wiremock) в `mdb-processing/localrun/`, затем запускает сам mdb-processing через `bootRun --args='--spring.profiles.active=local'`.
3. **Backstage НЕ нужен** для базовых тестов modify-флоу (мимо UI). mdb-data сам стартует temporal workflow через processing.
4. **UI (репозиторий mdb)** — Vite dev-сервер на порту **3012**: `pnpm run dev` в `/Users/vl.ershov/Documents/Git/mdb` (Node ^22, см. `/ui-developer`).

## Полная локальная связка с UI (5 сервисов)

UI — это отдельный фронт (репозиторий mdb), его API `/api/mdb/*` отдаёт **Backstage mdb-backend**, а НЕ mdb-data. Полная цепочка:

```
UI (3012, vite) ──vite-proxy──▶ Backstage (7007) ──▶ mdb-data (8081) ──▶ mdb-processing (8080) ──▶ temporal (8233)
        └──▶ vkone-stub (8090)
```

`.env` репозитория mdb (в `.gitignore`, правки безопасны):
```
MDB_API_URL=http://localhost:7007
VKONE_API_URL=http://localhost:8090
MDB_DATA_LOCAL_URL=http://localhost:8081
PROXY_API_PREFIX=/proxy
```
С `PROXY_API_PREFIX` запросы идут через vite-proxy — обходит CORS. Важно: прод-балансер `api.mdb.one-infra.ru` рутил `/api/mdb/*` → Backstage (7007), а **`/api/v2/*` (products, кластерные v2-ручки) → mdb-data (8081)** — в `vite.config.cts` добавлен opt-in прокси `/proxy/_mdb/api/v2/*` → `MDB_DATA_LOCAL_URL` (без переменной — no-op).

### Запуск Backstage (локально)

1. Инфраструктура: `docker compose -f backstage/stubs/docker-compose.yml up -d` (postgres:6432, redis:6379, clickhouse, sentinel:26379).
   - **pg_boss**: `docker exec postgres psql -U dev -d postgres -c "CREATE DATABASE pg_boss;"` — иначе backend падает на старте.
   - **Sentinel**: контейнер `stubs-sentinel-1` не слушает с хоста без `bind 0.0.0.0` + `protected-mode no` в `stubs/sentinel.conf` (уже поправлено в репо). Старый контейнер `redis_sentinel` из docker-compose mdb-data держит 26379 и это обычный redis, не sentinel — удалить (`docker rm -f redis_sentinel`), иначе «Project cache initialization failed Command timed out» (ioredis commandTimeout=1000).
2. `app-config.mdb.local.yaml` — нужны `backend.mdb.abc.baseUrl` (http://localhost:8088 wiremock) и `backend.mdb.abc.ca` (любая строка, обязателен `getString`) — иначе `Missing required config value at 'backend.mdb.abc.ca'`. `backend.mdb.auth.enabled: false` уже стоит (локальная сессия `k.boblak` из ADMIN_LOGINS).
3. Запуск: `yarn mdb-start-backend` в `backstage/` (лог `/tmp/backstage.log`), ждать «Project cache successfully initialized» + «Listening on :7007».
4. **Устаревшая схема 6432**: если `Undefined column(s): [name]` на projects — снести `backstage_plugin_mdb` (`DROP DATABASE` + `CREATE DATABASE`) и рестартнуть backend (Flyway/knex пересоздаст).

### Сидирование из прода (для UI-данных)

Прод-БД через port-forward `localhost:53480` (см. `db-worker`). Что копировать для живого UI (обе БД — 6432 Backstage И 6434 mdb-data, данные должны совпадать):

- `projects` (все, ~2k), `namespaces` (все, 4), `hardware_presets` (все, ~91)
- по кластерам проекта (по умолчанию **mdbdev = project_id 160**, ~800 кластеров): `db_cluster`, `db_cluster_version`, `host_state`, `one_cloud_meta`, `db_shards`
- **`operations` (последние ~5 на кластер) — обязательно**: статус кластера в UI считается из последней операции (`ClusterManager.calculateClusterStatus` → `mapOperationEntityToModel` без null-check → 500 `Cannot read properties of undefined (reading 'id')` при отсутствии).

Способ: `\copy (SELECT …) to '/dev/stdout' csv header` через туннель → отчистить хвостовой тэг `COPY N` (grep -v) → `docker cp` → `\copy … from csv header`. Грабли:
- **Экспортировать по явному списку колонок локальной таблицы** — схемы прода и локали дрейфуют (6434 не знает `fake_id` в one_cloud_meta, другой порядок колонок в projects).
- **Enum'ы прода шире** — добавлять значения перед импортом: `version_type` (+add_shard/add_hosts/delete_hosts), `db_type` (6434: +newsql/cassandra/temporal), `operation_type` (почти весь список прода). Каждый `ALTER TYPE … ADD VALUE IF NOT EXISTS` — отдельным `docker exec` (новая psql-сессия): значения, добавленные в той же сессии, COPY иногда не видит.
- `host_state.grafana_dashboard_link`/`onecloud_ui_link` и `operations.error_message` — `ALTER COLUMN … TYPE text` (varchar(255) мало для прод-значений).
- Порядок вставки: projects/namespaces/hardware_presets → db_cluster → db_shards → db_cluster_version → host_state → one_cloud_meta → operations. После — `setval` для serial-pk (projects, hardware_presets, db_shards) и рестарт Backstage (кэш проектов в redis строится на старте).

### Auth UI на localhost: стаб vkone (порт 8090)

mdb-data локально открыт (`mdb.auth.enabled: false` → сессия `i.mishechkin`, админ, кука `mdb_session_id` не нужна). Но `GetCurrentUser` (`/api/v1/user/info`) и `AllFeatureFlags` (`/api/v2/allflags`) — это **vkone** (`one.vk.team/_vkone/`), без них UI показывает «Необходимо авторизоваться».

⚠️ Копировать прод-куки `vk-one-*` в DevTools бесполезно: в значениях из инструкций подписи замаскированы (`__SECRET_N__`) — vkone парсит юзера, но падает на `failed to get one-cloud user roles`.

Рабочий способ — локальный стаб:

1. Запустить: `node ~/.claude/skills/mdb-local-tester/scripts/vkone-stub.mjs` (порт 8090, лог `/tmp/vkone-stub.log`).
2. В `.env` репозитория mdb: `VKONE_API_URL=http://localhost:8090` (запросы пойдут `localhost:3012/proxy/_vkone/*` → vite proxy → стаб).
3. Перезапустить `pnpm run dev`, перегрузить страницу.

Стаб отдаёт фиксированного юзера (vl.ershov), пустые фиче-флаги, `/api/v1/dcs → []`; отсутствующие роуты — 404 с логом в `/tmp/vkone-stub.log` (по нему видно, чего ещё не хватает — дописать в стаб по контракту из `src/shared/api/vkone/__generated__/data-contracts.ts`).

## Порты (важно!)

| Сервис | Порт | Как запустить |
|---|---|---|
| mdb-data | **8081** | `bootRun --args='--spring.profiles.active=local --server.port=8081'` |
| mdb-processing | **8080** | дефолт в `application.yaml` mdb-processing |
| mdb UI (vite dev) | **3012** | `pnpm run dev` в `/Users/vl.ershov/Documents/Git/mdb` |
| Backstage (API для UI) | **7007** | `yarn mdb-start-backend` в `backstage/` + stubs compose |
| vkone-stub (auth UI) | **8090** | `node ~/.claude/skills/mdb-local-tester/scripts/vkone-stub.mjs` |
| temporal UI | 8233 | docker-compose mdb-processing |
| postgres (mdb-data) | 6434 | docker-compose mdb-data, контейнер `pg_backstage_plugin_mdb` |
| postgres (backstage) | 6432 | `backstage/stubs/docker-compose.yml`, контейнер `postgres` |
| redis (backstage) | 6379 | `backstage/stubs/docker-compose.yml`, контейнер `redis` |
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

## Идемпотентность рестарта операции (must-check)

Для любого флоу, который бутит/перезапускает хосты (scale/modify/cruise-create), после проверки
happy path **обязательно** проверяй сценарий рестарта:

1. Дай операции упасть **после** того, как хотя бы один child-workflow успешно завершился
   (например, урони деплой в одном ДЦ после reconcile/pms-шага).
2. Перезапусти операцию тем же workflowId (новый run, как это делает retry из mdb-data).
3. Ожидание: новый run **не** должен падать на старте уже завершённого child'а с
   `WorkflowExecutionAlreadyStarted` (`RETRY_STATE_NON_RETRYABLE_FAILURE`).

Причина: child-workflowId у нас детерминированные (`<parentId>_суффикс`), reuse policy —
`ALLOW_DUPLICATE_FAILED_ONLY`, т.е. после **успешного** завершения child'а повторный старт тем же
workflowId запрещён. Поэтому каждый синхронный/асинхронный старт child'а в workflow-коде должен
быть обёрнут в `ChildWorkflowUtils.runIgnoringAlreadyStarted` / `ignoreAlreadyStarted` — иначе
один упавший run операции блокирует все её рестарты навсегда (кейс MDBDEV-3245: reconcile-хелпер
без обёртки). ВTemporal-истории это видно как `START_CHILD_WORKFLOW_EXECUTION_FAILED` сразу после
старта run'а.

## Смежные скиллы

- **`upscale-kafka-controller-tester`** — тестирование upscale Kafka-контроллеров (MDBDEV-3180): seed test-modify3, симуляция падений из прод-Temporal, планы T1–T7.

## История тестов

Перед написанием новых запросов проверяй готовые сценарии:
```bash
ls ~/.claude/skills/mdb-local-tester/history/
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

Используй `/db-seed`: сгенерируй SELECT-запросы для удалённой БД, пользователь выполнит их на удалённом хосте (через скилл [`mcc-host-worker`](../mcc-host-worker/SKILL.md), `mcc ssh` + `psql`), результат вставляется в локальную БД. Выдуманные хосты не работают — one-cloud master вернёт `404 EntityNotFoundException`.

### Обязательный шаблон: один SQL через `jsonb_build_object`

Данные кластера тянем **одним SQL-запросом** через `jsonb_build_object` — пользователь получает один JSON, не несколько выводов. Это касается и cruise-creation, и modify-тестов, и любых других сценариев, где нужны полные данные кластера.

Шаблон (подставь свой `cluster_id`):

```sql
SELECT jsonb_build_object(
  'db_cluster', (SELECT json_agg(t) FROM (SELECT * FROM db_cluster WHERE id='<CLUSTER_ID>') t),
  'db_cluster_version', (SELECT json_agg(t ORDER BY create_ts DESC) FROM (SELECT * FROM db_cluster_version WHERE cluster_id='<CLUSTER_ID>' ORDER BY create_ts DESC LIMIT 3) t),
  'host_state', (SELECT json_agg(t) FROM (SELECT * FROM host_state WHERE cluster_id='<CLUSTER_ID>') t),
  'one_cloud_meta', (SELECT jsonb_agg(to_jsonb(t) ORDER BY t.params_type) FROM (SELECT * FROM one_cloud_meta WHERE cluster_id='<CLUSTER_ID>') t),
  'projects', (SELECT json_agg(t) FROM (SELECT p.* FROM projects p JOIN db_cluster c ON c.project_id=p.id WHERE c.id='<CLUSTER_ID>') t),
  'namespaces', (SELECT json_agg(t) FROM (SELECT n.* FROM namespaces n JOIN db_cluster c ON c.namespace_id=n.id WHERE c.id='<CLUSTER_ID>') t),
  'hardware_presets', (SELECT json_agg(t) FROM (SELECT hp.* FROM hardware_presets hp WHERE hp.id IN (SELECT DISTINCT hardware_preset_id FROM db_cluster_version WHERE cluster_id='<CLUSTER_ID>')) t)
);
```

⚠️ **`one_cloud_meta` обязательна для cruise-creation** — без записи `params_type='cruise-control-service'` workflow `createKafkaCruise` падает с `404` на `MdbDataKafkaHostsActivityImpl.savedCreatedKafkaCruiseInfo`. У таблицы UNIQUE-индекс по `(cluster_id, params_type)` — ВСЕГДА `jsonb_agg`, не скалярный `to_jsonb`.

Правила из `/db-seed` (важно):
- `ORDER BY` — только **внутри** `jsonb_agg(... ORDER BY col)`, не снаружи подзапроса.
- Для таблиц с unique-индексом по `(cluster_id, <другая колонка>)` (например `one_cloud_meta` по `(cluster_id, params_type)`) — ВСЕГДА `jsonb_agg`, не скалярный `to_jsonb`, иначе `more than one row returned`.
- `operations.created_ts` (с `d`), `db_cluster_version.create_ts` (без `d`) — имена различаются, проверяй через `\d <table>` на удалённой БД.

### Cruise-creation: что достаём из полученного JSON

Из засеянных данных собираешь `CreateCruiseControlRequest` (см. `history/MDBDEV-2882-create-cruise-control-*.md`). Соответствие полей:

| Поле request | Источник в БД |
|---|---|
| `clusterId` | `db_cluster.id` |
| `namespace` | `namespaces.name` → **uppercase** (`"INFRA"`, не `"infra"`) |
| `queue` | `<db_cluster.name>-<project.name>-kafka` |
| `fullQueue` | `<queue>.<project.name>.db.<environment>.mdb.prod` |
| `rootQueue` | `cluster_params.rootQueue` |
| `projectName` | `projects.name` |
| `pmsHostName` | `<queue>.clouds` |
| `certsHostName` | `cruise.<queue>.clouds` |
| `serviceName` | `cruise.<queue>` |
| `cruiseControlDc` | из задачи пользователя (например `rc`) — это DC, где будет поднят cruise |
| `namespaceDomain` | константа `"mdb"` (часть PMS-пути, не из БД) |
| `isWan` | `cluster_params.isWan` |
| `cruiseControl.jvmHeapSizeMb` | `cluster_params.kafkaParams.cruiseControl.jvmHeapSizeMb` или дефолт `2048` |
| `cruiseControl.autoRebalanceEnabled` | из задачи (обычно `true`) |
| `brokerDcs` | `host_state` — список уникальных `params->>'dc'` для хостов с FQDN вида `*.broker.*` |
| `brokerParameters` | `cluster_params.kafkaParams.brokerConfig.config` (например `{"num.io.threads":"8"}`) |
| `brokerDiskGb` / `brokerLanInMb` / `brokerLanOutMb` | `cluster_params.diskGb` / `lanIn` / `lanOut` |
| `dockerName` / `dockerTag` | docker-образ cruise-control (НЕ kafka-брокера!). Обычно `ubuntu20-mdb-cruisecontrol-2.5.147` / `1.0.2` — уточнять в PMS или через последний стабильный тест |
| `cruiseUserPassword` | из задачи пользователя |
| `workflowTtl` | константа `"PT1H"` (ISO-8601, **не** `3600`) |

⚠️ **Cruise-creation workflow запускается напрямую через `tctl`**, не через mdb-data modify-эндпоинт (кодогенерации API пока нет). См. `history/MDBDEV-2882-create-cruise-control-2026-08-06.md`.

## Проверка PMS-переменных (modify-флоу в mdb-processing)

После modify-операции проверить, что флоу реально записал PMS-переменные (`kafka.soc.audit.*`, `kafka.sysconfig`, `kafka.cruisecontrol.*` и т.д.) — чтение PMS через скилл **[`pms-worker`](../pms-worker/SKILL.md)** (скрипт `pms-read.sh`), сверка PMS с отрендеренными конфиг-файлами на хостах — скилл **`kafka-config-inspector`**.

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

# 2. Vite dev-сервер UI (3012), Backstage (7007), vkone-stub (8090)
lsof -ti:3012,7007,8090 | xargs -r kill -9

# 3. Docker-инфраструктура mdb-data (pg + redis)
docker compose -f /Users/vl.ershov/Documents/Git/mdb-data/docker-compose.yml down

# 4. Docker-инфраструктура mdb-processing (temporal + vault + kafka + wiremock)
cd /Users/vl.ershov/Documents/Git/mdb-processing/localrun && docker compose down

# 5. Docker-инфраструктура Backstage (postgres 6432 + redis 6379 + clickhouse + sentinel)
cd /Users/vl.ershov/Documents/Git/backstage/stubs && docker compose down
```

Проверка:
```bash
docker ps --format '{{.Names}} {{.Status}}'   # должно быть пусто
lsof -iTCP:8080,8081,8233,6434,26379,3012 -sTCP:LISTEN -P   # должно быть пусто
```

⚠️ У `pg_backstage_plugin_mdb` нет volume — после `down` данные стираются. При следующем запуске нужно заново:
- применять миграции V2–V4 (`shedlock`, `in_processing`) вручную, либо перезапустить mdb-data (flyway применит сам);
- применять seed SQL.
