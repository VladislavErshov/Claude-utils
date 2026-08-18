# Kafka controller upscale/downscale в одном ДЦ — test-create (zc) — 2026-07-28

Тест add/remove kafka-контроллера через `POST/DELETE /api/v2/mdb/kafka/clusters/{id}/hosts/controllers?dc={dc}`. Проверена корректность изменения PMS-переменных `kafka.layout` и `kafka.controller.quorum` на реальном `pms.cloud.vk.team`.

## Инфраструктура

- mdb-data на :8081, mdb-processing на :8080, temporal UI :8233, postgres `pg_backstage_plugin_mdb` :6434
- Local-профиль mdb-processing ходит в **реальный PMS** и **реальный Cloud API** (mTLS из `~/.mccloud/`)
- Vault: реальный `vault.cloud.vk.team` — здесь лежит затык для нового ДЦ (см. ниже)

## Кластер

| Поле | Значение |
|---|---|
| cluster_id | `f1a8a446-a692-4871-a5ee-ed296e3c2230` |
| name | test-create |
| type | kafka |
| project | 160 (mdbdev) |
| namespace | 2 (infra), PMS-ключ `test-create-mdbdev-kafka.clouds` |
| environment | production |
| db_version | 3.8 (docker `ubuntu20-kafka-3.8.0:2.4.2`, cruise `1.0.8`) |
| hardware_preset | 168 (m.femto) |
| baseline version | 152506, status=scheduled |
| controllerDcs (исходно) | `["hc","kc","pc"]` |
| hosts (исходно) | 3 broker (hc/kc/pc) + 1 cruise (hc) + 3 controller (hc/kc/pc) |

В baseline добавлены `kafkaParams.brokerConfig.config={}` и `kafkaParams.controller.controllerConfig.config={}` — без них `KafkaClusterDiffDetector` падает с NPE.

## Эндпоинты (mdb-data)

- `POST /api/v2/mdb/kafka/clusters/{id}/hosts/controllers?dc={dc}` → `KafkaHostsController.upscaleKafkaController`
- `DELETE /api/v2/mdb/kafka/clusters/{id}/hosts/controllers?dc={dc}` → `KafkaHostsController.downscaleKafkaController`

Тело запроса не нужно. Auth отключён (`mdb.auth.enabled: false` в local).

## Сценарий A: upscale controller в ДЦ `dc`

`POST .../hosts/controllers?dc=dc` → 202, operation `add_hosts` создана, temporal workflow `upscaleKafkaController` (97d2331a) запущен.

Workflow прошёл активности:
1. `cloud_getInfoForInstances` ✓
2. `cloud_submitQueueIfNeeded` ✓
3. `cloud_getQueueState` ✓
4. `upsertKafkaLayout` ✓ → `kafka.layout` = `hc,kc,pc,dc` (добавлен dc)
5. `upsertControllerInstancePendingQuorum` ✓
6. `cloud_submitService` ❌ — `Error in cloud call: Could not execute request` (нет квоты в ДЦ dc)

PMS после сценария A:
- `kafka.layout` = `hc,kc,pc,dc` ✓ (добавлен dc)
- `kafka.controller.quorum` = исходные 3 voter (workflow упал до `upsertControllerQuorum`)

**Вывод**: в ДЦ `dc` нет квоты → Cloud API не создаёт сервис → workflow падает. Нужно использовать ДЦ с квотой.

## Сценарий B: upscale controller в ДЦ `zc` (есть квота)

`POST .../hosts/controllers?dc=zc` → 202, operation `add_hosts`, workflow `upscaleKafkaController` (75663630) запущен.

Прошёл:
1. `upsertKafkaLayout` ✓ → `kafka.layout` = `hc,kc,pc,dc,zc` (добавлен zc; dc остался от упавшего сценария A)
2. `upsertControllerQuorum` ✓ → `kafka.controller.quorum` = `10001@...hc,11001@...kc,12001@...pc,14001@...zc` (добавлен 4-й voter)
3. ❌ Упал на `vault` 404: `no handler for route "zkv/data/mdb/mdbdev/kafka/test-create-mdbdev-kafka.mdbdev.db.production.mdb.prod/super"`

PMS после сценария B:
- `kafka.layout` = `hc,kc,pc,dc,zc` ✓
- `kafka.controller.quorum` = 4 voter, добавлен `14001@1.controller.test-create-mdbdev-kafka.zc.one-infra.ru:9093` ✓

В `host_state` controller в zc **не появился** — workflow упал до `saveUpscaledKafkaControllersInfo`.

## Сценарий C: downscale controller в ДЦ `zc`

Подготовка:
- `DELETE FROM operations WHERE id='75663630-...'` (сброс failed operation, иначе 409)
- `INSERT INTO host_state (id=100058, host='1.controller.test-create-mdbdev-kafka.zc.one-infra.ru', params='{"dc":"zc"}')` — simулировали наличие controller в БД (иначе валидатор `min value of controllers is 3` не пропустит downscale)

`DELETE .../hosts/controllers?dc=zc` → 202, operation `delete_hosts`, workflow `downscaleKafkaController` (80d518d6) запущен.

Прошёл:
1. `pmsActivity.getVariable(kafka.controller.quorum)` ✓
2. `kafkaPmsActivity.removeControllerFromQuorum(namespace, "test-create-mdbdev-kafka.clouds", targetController=zc)` ✓ → `kafka.controller.quorum` = 3 voter (hc, kc, pc) — controller из zc удалён
3. ❌ Упал на `vault` 404 (тот же путь, что и в сценарии B)

PMS после сценария C:
- `kafka.controller.quorum` = `10001@...hc,11001@...kc,12001@...pc` ✓ (controller из zc удалён, вернулись к 3 voter)
- `kafka.layout` = `hc,kc,pc,dc,zc` — НЕ обновился

В `host_state` controller в zc **остался** — workflow упал до `saveDownscaledKafkaControllersInfo`.

## Подтверждённые цепочки PMS-изменений

| Операция | Activity | PMS-переменная | Изменение | Статус |
|---|---|---|---|---|
| Upscale | `upsertKafkaLayout` | `kafka.layout` | добавлен dc | ✅ |
| Upscale | `upsertKafkaLayout` | `kafka.layout` | добавлен zc | ✅ |
| Upscale | `upsertControllerInstancePendingQuorum` | (per-instance quorum) | временный quorum для нового controller | ✅ (успешно выполнено до падения) |
| Upscale | `upsertControllerQuorum` | `kafka.controller.quorum` | добавлен `14001@...zc...` voter | ✅ (только в zc, не в dc — упало раньше) |
| Downscale | `removeControllerFromQuorum` | `kafka.controller.quorum` | удалён `14001@...zc...` voter | ✅ |
| Downscale | (нет activity) | `kafka.layout` | — | ⚠️ не обновляется (известное поведение: downscale не трогает layout) |

## Находки

### 1. min value of controllers is 3 — валидатор downscale

`KafkaHostsServiceImpl.downscaleKafkaController` → `validateIfItIsAllowedToDownscale` возвращает 400 `"Not allowed to downscale: min value of controllers is 3"`, если в `host_state` меньше 4 controller-ов. То есть чтобы удалить одного controller-а, нужно иметь минимум 4. Это блокирует downscale, если до этого upscale не завершился полностью (controller не попал в `host_state`).

**Workaround для local-теста**: simулировать controller в `host_state` INSERT-ом вручную перед downscale.

### 2. Vault 404 для нового ДЦ

`Query for vault returned non-200. Status: 404. Error: {"errors":["no handler for route \"zkv/data/mdb/mdbdev/kafka/test-create-mdbdev-kafka.mdbdev.db.production.mdb.prod/super\". route entry not found."]}`

Vault-путь `zkv/data/mdb/mdbdev/kafka/<fullQueue>/super` не существует для кластера `test-create` — нет pre-provisioned секретов. Это блокирует и upscale (при создании сервиса в новом ДЦ), и downscale (при остановке сервиса). Local-тест упирается в отсутствие vault-секретов, а не в PMS/Cloud API.

**Важно**: это не баг workflow, а окружение. PMS-изменения уже применились до vault-запроса — для проверки PMS это не помеха.

### 2a. Vault-секрет для downscale-controller — что нужно (подтверждено 2026-08-13 на test-modify3)

Downscale-controller workflow на activity `kafka_host_getLeaderId` (после `removeControllerFromQuorum`) читает superuser-пароль Kafka из vault. В local-профиле vault-клиент mdb-processing настроен на **локальный** контейнер `mdb-processing-vault` (`http://localhost:8200`, token `root`, namespace `infra`), а не на реальный `vault.cloud.vk.team`. В локальном vault секрета для кластера нет — 404 с пустым телом `{"errors":[]}` (не путать с прод-404 `no handler for route`).

**Параметры секрета**:
- mount: `zkv` (KV v2)
- путь: `zkv/mdb/mdbdev/kafka/<fullQueue>/super`
  - для test-modify3: `zkv/mdb/mdbdev/kafka/test-modify3-mdbdev-kafka.mdbdev.db.production.mdb.prod/super`
- ключ: `password` (DEFAULT_KEY в `CachedVaultPasswordService` / `BaseVaultPasswordService`)
- значение: реальный superuser-пароль Kafka (взять с прода: `vault kv get -field=password zkv/mdb/mdbdev/kafka/<fullQueue>/super`)

**Залить в локальный vault**:
```bash
docker exec mdb-processing-vault sh -c \
  'VAULT_ADDR=http://localhost:8200 VAULT_TOKEN=root vault kv put \
  zkv/mdb/mdbdev/kafka/<fullQueue>/super password=<PASSWORD> permissions=[]'
```

Также по тому же пути в проде есть секреты: `cruise`, `kafka_exporter`, `keystore-password`, `super`, `truststore-password`. Для downscale-controller достаточно `super` (пароль superuser). Остальные могут понадобиться для других шагов workflow — если упадёт дальше, тащить по тому же шаблону.

**Важно**: после заливки секрета workflow ходит на **реальные** прод-kafka-brokers (`1.broker.<cluster>.<dc>.one-infra.ru:9092`) под этим паролем. Пароль должен быть актуальным — иначе упадём уже на `KafkaAdminClient.getLeaderId` с auth error, не на vault.

### 2b. SSL keystore/truststore пароли для `kafka_host_getLeaderId` (SASL_SSL)

После заливки `super` workflow проходит vault-шаг, но `KafkaHostActivityImpl.getLeaderId` всё равно падает на `KafkaAdminClientFactoryImpl.createClient` — клиент перебирает `[SASL_SSL, SASL_PLAINTEXT]` и оба падают:

- **SASL_SSL**: `Failed to create new KafkaAdminClient` → `Failed to load PEM SSL keystore` / `Is a directory`. Root cause — клиенту нужны пароли `keystore-password` и `truststore-password` из того же vault-пути.
- **SASL_PLAINTEXT**: `TimeoutException: Timed out waiting for a node assignment. Call: listNodes` — брокеры не слушают plaintext на 9092, этот протокол не работает.

**Секреты для заливки в локальный vault** (путь `zkv/mdb/mdbdev/kafka/<fullQueue>/`):
- `keystore-password` → key `password`
- `truststore-password` → key `password`

Взять с прода:
```bash
vault kv get -field=password zkv/mdb/mdbdev/kafka/<fullQueue>/keystore-password
vault kv get -field=password zkv/mdb/mdbdev/kafka/<fullQueue>/truststore-password
```

Залить в локальный vault:
```bash
for s in keystore-password truststore-password; do
  docker exec mdb-processing-vault sh -c \
    "VAULT_ADDR=http://localhost:8200 VAULT_TOKEN=root vault kv put \
    zkv/mdb/mdbdev/kafka/<fullQueue>/$s password=<VALUE>"
done
```

Если после заливки паролей `SASL_SSL` всё равно падает на `Failed to load PEM SSL keystore` — значит локально отсутствуют PEM-файлы truststore/keystore (путь берётся из `kafkaConnectionProperties.getNamespaces().get(...).sslTruststoreLocation()`, см. `KafkaConnectionPropertiesConverter.java:65`). Это уже отдельная проблема окружения — нужен SSL-контент из prod-vault (секреты `cruise`/`kafka_exporter` тоже могут содержать PEM).

### 3. `kafka.layout` не очищается при downscale

В `DownscaleKafkaControllerWorkflowImpl` нет activity `removeDcFromLayout` / `upsertKafkaLayout` — только `removeControllerFromQuorum`. После сценария C `kafka.layout` остался `hc,kc,pc,dc,zc`, хотя controller в zc удалён из quorum. Если это намеренно (layout описывает ДЦ кластера, а не controller-voter) — ОК. Если должно чиститься — баг. Стоит уточнить у команды mdb-processing.

### 4. Остаточный "dc" в kafka.layout

Сценарий A (upscale в dc) успел добавить "dc" в layout до падения на Cloud API. Сценарии B/C не очищали его (downscale в zc не трогает layout). Итоговый layout содержит dc и zc, хотя controllers там нет. Это артефакт local-теста — в проде без квоты сервис бы не создался и workflow бы упал, но `kafka.layout` уже изменён. PMS только читаем из local, почистить нельзя.

## Cleanup

```sql
-- Сброс failed operations
DELETE FROM operations WHERE cluster_id='f1a8a446-a692-4871-a5ee-ed296e3c2230';

-- Удалить simулированного controller в zc
DELETE FROM host_state WHERE id=100058;

-- Удалить draft-версии, если появились
DELETE FROM db_cluster_version WHERE cluster_id='f1a8a446-a692-4871-a5ee-ed296e3c2230' AND status='draft';
```

Temporal workflows 75663630 и 80d518d6 остались в FAILED — не влияют на следующие тесты, при желании можно terminate через UI :8233.

## Файлы

- Seed SQL: `/tmp/seed_test_create.sql`
- PMS snapshot после upscale: `/tmp/kafka-inspect/test-create/pms-after-upscale.txt`
- PMS controller snapshot: `/tmp/kafka-inspect/test-create/pms-controller-after-upscale.txt`
- Final PMS layout+quorum: `/tmp/kafka-inspect/test-create/pms-layout-final.txt`, `pms-quorum-final.txt`
