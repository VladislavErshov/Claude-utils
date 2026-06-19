---
name: backstage-local-tester
description: Используй этот скилл, когда нужно локально протестировать API или workflow в Backstage MDB.
allowed-tools: [bash, read_file]
---

# Скилл для локального тестирования Backstage MDB

## Запуск инфраструктуры и сервисов

Все три команды обязательны для теста сценария Backstage → mdb-data → Temporal:

1. `/setup-local-backstage` — postgres :6432, redis :6379, sentinel :26379, clickhouse + `pg_boss` БД + сам Backstage backend :7007 (`yarn mdb-dev`)
2. `/setup-local-mdb-data` — postgres :6434 + mdb-data приложение :8081
3. `/setup-local-temporal` — temporal :7233 + vault + kafka + wiremock в `mdb-processing/localrun/` + mdb-processing приложение :8080, плюс переключение `app-config.mdb.local.yaml → mdb-processing.host: 'http://127.0.0.1:8080'`

**Порядок запуска**: `/setup-local-backstage` → `/setup-local-mdb-data` → `/setup-local-temporal`. Backstage сидируется до старта своих воркеров (Flyway применяет миграции в БД при первом boot), `mdb-data` поднимается на 8081 чтобы не конфликтовать с processing на 8080.

**Конфликт sentinel'ов:** `redis_sentinel` из `mdb-data` не поднимется (порт 26379 уже держит sentinel из `backstage/stubs`). Это нормально — mdb-data приложение подключается к sentinel из stubs.

## Настройка app-config.mdb.local.yaml

```yaml
mdb:
  mdb-processing:
    host: 'http://127.0.0.1:8080'   # НЕ localhost (IPv6 → Node.js → ECONNREFUSED)
  mdb-data:
    host: 'http://localhost:8081'
```

Секрет для service auth: `backend.mdb.auth.services.secret: 2210c0a2-fb9b-461f-9f21-a25acebb2559` (уже стоит в локальном конфиге).

## Базы данных

| Сервис | Контейнер | Порт | БД |
|--------|-----------|------|-----|
| Backstage | postgres | 6432 | backstage_plugin_mdb, pg_boss |
| mdb-data | pg_backstage_plugin_mdb | 6434 | backstage_plugin_mdb |

Backstage создаёт `backstage_plugin_mdb` при первом запуске через Flyway. **Seed после старта Backstage** — кеш проектов/namespaces в Redis инициализируется при boot. Если сидируешь после — рестарт backend обязателен, иначе все cluster auth и pms-таски падают.

## Сидирование (порядок важен)

1. Запустить инфраструктуру (stubs + mdb-data + temporal)
2. Запустить Backstage `yarn mdb-dev` → подождать Flyway, увидеть `Listening on :7007`
3. Применить seed в обе БД одновременно
4. **Перезапустить Backstage backend** (без этого Redis-кеш проектов и PMS-конфиг не обновятся)

Сидирование sql-файлом:
```bash
docker cp /tmp/seed.sql postgres:/tmp/seed.sql && \
  docker exec postgres psql -U dev -d backstage_plugin_mdb -f /tmp/seed.sql
docker cp /tmp/seed.sql pg_backstage_plugin_mdb:/tmp/seed.sql && \
  docker exec pg_backstage_plugin_mdb psql -U dev -d backstage_plugin_mdb -f /tmp/seed.sql
```

В seed-файл обязательно включить:
- **`hardware_presets.database_preset.mongodbPreset`** для **всех** пресетов кластера (даже Kafka) — иначе `POST /version/` падает с `Cannot read properties of null (reading 'mongodbPreset')` в DbParamsValidator.
- **`services_auth`** для curl-тестов (см. ниже про авторизацию).
- **`db_cluster_version` baseline со `status='done'`** — повторный сброс операции **не должен** трогать done-версии, иначе следующий POST упадёт на «нет базовой версии».

## Авторизация для curl

Backstage middleware `clusterIdAuthMiddleware` (router.ts:462) защищает все `/cluster/:clusterId/*` endpoint'ы. Для curl-тестов проще service auth (JWT).

```bash
# 1. Зарегистрировать service в БД для нужного проекта
docker exec postgres psql -U dev -d backstage_plugin_mdb -c \
  "INSERT INTO services_auth (name, project_id, access_type) VALUES ('local-tester', 160, 'w') \
   ON CONFLICT (name, project_id) DO UPDATE SET access_type='w';"
# Важно: access_type='w', НЕ 'write' — AccessType.WRITE = "w" (plugins/common/src/authorization/dto.ts)

# 2. Сгенерировать JWT
TOKEN=$(node -e "
  const jwt=require('/Users/vl.ershov/Documents/Git/backstage/node_modules/jsonwebtoken');
  console.log(jwt.sign(
    {serviceName:'local-tester', projectId:160, accessType:'w'},
    '2210c0a2-fb9b-461f-9f21-a25acebb2559',
    {expiresIn:'365d'}
  ));")

# 3. Использовать в curl
curl -X POST "http://localhost:7007/api/mdb/cluster/$CLUSTER_ID/version/" \
  -H "Authorization: ${TOKEN}" \
  -H "Content-Type: application/json" \
  -d @/tmp/request.json
```

## Триггер modify (update_instances)

Endpoint: `POST /api/mdb/cluster/:clusterId/version/`
Body: `ClusterVersionCreateDto = { params: ClusterParams, hardwarePresetId, isNeedShards, hosts: [{dc}], type: "update_instances", attempts, dbVersionId? }`

### Обязательные поля `params` для Kafka update_instances

DTO **не нормализует** поля из baseline — нужно передавать полный набор, иначе валидаторы либо вернут 422, либо mdb-data упадёт с NPE:

- `acl`, `name`, `isWan`, `lanIn`, `lanOut`, `diskGb`, `diskType`, `projectId`, `rootQueue`, `needLanIpv6`, `needWanIpv4`, `needWanIpv6`
- `kafkaParams.controller = {controllerDcs, controllerConfig, controllerLanIn, controllerLanOut, controllerVcores, controllerMemGb, controllerDiskGb, controllerDiskType, controllerJvmHeapSizeMb}` — **обязательно**, иначе `KafkaClusterModificationValidator.java:57` NPE на `ModifyKafkaParams.controller()`. Корневые `brokerConfig`/`controllerConfig` Jackson игнорирует — нужно вкладывать в `kafkaParams.brokerConfig` и `kafkaParams.controller.controllerConfig`.
- `kafkaParams.brokerConfig`, `kafkaParams.cruiseControl`, `kafkaParams.jvmHeapSizeMb`
- **`diskType` совпадает с baseline** — `validateDbParamsForUpdateRequest` отвергает изменение типа диска: 422 `Нельзя изменить тип диска при изменении кластера`.

### Шаблон `/tmp/request.json`

```json
{
  "params": {
    "acl": {},
    "name": "<cluster-name>",
    "isWan": false,
    "lanIn": 15, "lanOut": 25, "diskGb": 8, "diskType": "nvme",
    "projectId": 160, "rootQueue": "prod",
    "needLanIpv6": true, "needWanIpv4": false, "needWanIpv6": false,
    "kafkaParams": {
      "controller": {
        "controllerDcs": ["dc", "kc", "zc"],
        "controllerLanIn": 15, "controllerLanOut": 50,
        "controllerVcores": 4, "controllerMemGb": 4,
        "controllerDiskGb": 10, "controllerDiskType": "nvme",
        "controllerJvmHeapSizeMb": "2048",
        "controllerConfig": { "config": { "log.flush.interval.messages": 10000 } }
      },
      "brokerConfig": {
        "config": {
          "auto.create.topics.enable": "true",
          "compression.type": "uncompressed"
        }
      },
      "cruiseControl": { "cruiseControlDc": "dc", "cruiseUserPassword": "" },
      "jvmHeapSizeMb": 1024
    }
  },
  "hardwarePresetId": 169,
  "isNeedShards": false,
  "hosts": [{"dc":"dc"},{"dc":"kc"},{"dc":"zc"}],
  "type": "update_instances",
  "attempts": 3
}
```

Ответ 200 → создаётся `db_cluster_version` со статусом `scheduled` и запускается task chain.

## Обход PMS-таски (`update_kafka_pms_settings`)

PMS-таск пытается ходить в реальный prod-PMS (`stg.one-conf-web.devdc.odkl.ru` по конфигу для namespace=infra), который возвращает 400 — задача падает в `failed`/`need_retry` и блокирует следующие. Обход:

```sql
-- Найти PMS task для активной операции (id меняется между прогонами — НЕ хардкодить)
SELECT t.id, t.type, t.status FROM tasks t
JOIN operations o ON o.id=t.operation_id
WHERE o.cluster_id='<cluster_id>'
  AND o.status<>'canceled'
  AND t.type='update_kafka_pms_settings';

-- Помечаем done
UPDATE tasks SET status='done',
    result='{"status":"done","result":{"data":"Generated 5 settings"}}'::jsonb,
    finished_ts=now(), updated_at=now()
WHERE id=<pms_task_id>;
```

После обхода Backstage подхватывает следующую задачу (`modify_kafka_cluster` и т.п.) в течение ~10 секунд (cadence воркера PT10S).

## Сброс операции для повторного теста

```sql
-- Включаем need_retry — operation останавливается между попытками с этим статусом
UPDATE operations SET status='canceled'
WHERE cluster_id='<cluster_id>'
  AND status IN ('in_progress','failed','scheduled','need_retry');

-- baseline (done) НЕ трогаем, иначе следующий POST упадёт на «нет базовой версии»
DELETE FROM db_cluster_version
WHERE cluster_id='<cluster_id>'
  AND status IN ('draft','scheduled','failed','need_retry');
```

## Проверка прогресса теста

Полезные источники сигнала (в порядке информативности):

```bash
# 1. Backstage tasks — главный источник правды
docker exec postgres psql -U dev -d backstage_plugin_mdb -c "
  SELECT t.id, t.type, t.status, substring(t.result::text,1,100)
  FROM tasks t JOIN operations o ON o.id=t.operation_id
  WHERE o.cluster_id='<cluster_id>' AND o.status<>'canceled'
  ORDER BY t.id;"
# Ожидание: update_kafka_pms_settings=done, modify_kafka_cluster=done, finish_task=done

# 2. mdb-data логи — что Backstage отдал в PATCH, как mdb-data передал в processing
tail -100 /tmp/mdb-data.log | grep -iE "kafka|modify"

# 3. mdb-processing логи — старт workflow
tail -100 /tmp/mdb-processing.log | grep -iE "modifyKafkaCluster|workflow"

# 4. Temporal workflows
curl -s "http://localhost:8233/api/v1/namespaces/default/workflows?pageSize=20" | \
  jq -r '.executions[] | "\(.startTime[:19]) \(.status) \(.type.name) \(.execution.workflowId)"'

# По workflowId-префиксу (operationId)
curl -s "http://localhost:8233/api/v1/namespaces/default/workflows?query=WorkflowId%20BETWEEN%20%22<prefix>%22%20AND%20%22<prefix+1>%22" | \
  jq -r '.executions[] | "\(.startTime[:19]) \(.status) \(.type.name) \(.execution.workflowId)"'
```

**Важно**: `grep modify_kafka /tmp/backstage.log` бесполезен — `ModifyKafkaClusterTaskProcessor` логирует **только при ошибке** (`this.logger.error`). На успех — тишина. Смотри в tasks/mdb-data/mdb-processing.

**Workflow в Temporal RUNNING ≠ тест провалился.** `modifyKafkaCluster` — long-running: `updateControllerConfig` и др. дёргают реальный one-cloud (`*.one-infra.ru`), которого локально нет → OCI runtime errors, workflow висит. Это ожидаемо. Тест Backstage считается успешным, как только:
1. `modify_kafka_cluster` task в Backstage перешла в `done` (PATCH к mdb-data прошёл с 2xx)
2. В Temporal появился main workflow `modifyKafkaCluster` со статусом RUNNING/COMPLETED + ожидаемые дочерние

## Откат

```bash
# Сервисы — pkill не всегда убивает Backstage backend; верификация через lsof
pkill -f "MdbProcessingApplication"
pkill -f "MdbDataApplication"
pkill -f "mdb-app start\|mdb-backend start\|backstage-cli.*mdb"
sleep 2
# Backstage всё ещё на :7007? Убиваем по PID
PID=$(lsof -nP -iTCP:7007 -sTCP:LISTEN -t)
[ -n "$PID" ] && kill -9 $PID

# Инфраструктура
cd /Users/vl.ershov/Documents/Git/backstage/stubs && docker compose down
cd /Users/vl.ershov/Documents/Git/mdb-data && docker compose down
cd /Users/vl.ershov/Documents/Git/mdb-processing/localrun && docker compose down

# Конфиг
cd /Users/vl.ershov/Documents/Git/backstage && git checkout app-config.mdb.local.yaml
```

## Правила

1. **Enum lowercase** — `kafka`, `in_progress`, `done`, `update_instances`. `AccessType.WRITE='w'` (не `write`).
2. **IPv4 в Processing host** — `http://127.0.0.1:8080`, не `localhost` (IPv6 проблема Node.js на macOS).
3. **mdb-data порт** — `--server.port=8081` (8080 занят processing).
4. **Yarn команда** — `yarn mdb-dev`, НЕ `yarn dev`.
5. **Seed → рестарт Backstage** — Redis-кеш проектов и PMS-конфиг строятся при старте, после seed их нужно перечитать.
6. **PMS обходим вручную** — реальный prod-PMS возвращает 400, чинить SQL'ем после падения первой попытки.
7. **Дублирование данных** — для интеграции с mdb-data сидируй обе БД (postgres:6432 + pg_backstage_plugin_mdb:6434).
8. **Workflow RUNNING — норма** — Backstage-часть теста = PATCH прошёл и стартанул workflow. Cloud API локально недоступен.
9. **diskType неизменяемый** — в `update_instances` всегда передавай baseline-значение, иначе 422.
10. **`kafkaParams.controller` обязательное** — без него NPE на стороне mdb-data; brokerConfig/controllerConfig тоже только внутри `kafkaParams`.
