---
name: db-worker
description: Используй этот скилл для работы с БД MDB — локальной backstage_plugin_mdb, продовой backstage_plugin_mdb (mdb-etp-pgsql) и продовой mdb-health (health). Четыре сценария. 1) Локальный сидинг: наполнить локальную БД данными из прода для тестирования API. 2) Прямые SELECT к прод-БД через port-forward (read-only по умолчанию) — операции, кластеры, хосты, миграции. 3) DML на проде — ТОЛЬКО с явного разрешения пользователя (закрытие зависших операций MDBSUP и т.п.). 4) Прод-БД mdb-health — tier/warnings. Если туннель не поднят — генерирует SQL для удалённой БД, пользователь выполняет и отдаёт результат.
allowed-tools: [bash]
---

# Скилл работы с БД MDB (db-worker)

Ты работаешь в режиме DBA-ассистента. Четыре сценария:
1. **Локальный сидинг** — наполнить локальную БД реальными данными из удалённой, чтобы API-запросы проходили успешно.
2. **Прямые SELECT к прод-БД** (backstage_plugin_mdb) — read-only по умолчанию, разрешение не нужно.
3. **DML на проде** — только с явного разрешения пользователя, показывать SQL до выполнения.
4. **Прод mdb-health** — tier/warnings для UI mdb-data.

## Обязательный шаг: сканирование истории

**Перед началом** — проверь папку `history/` на наличие готовых seed-файлов и разборов для текущей задачи.

⚠️ Также смотри `history/gotchas-remote-schema.md` — каталог известных расхождений схемы удалённой БД (например, `db_versions.version` не существует).

```bash
ls ~/.claude/skills/db-worker/history/
```

Если есть готовые INSERT-запросы — используй их вместо генерации новых.

## Подключения (сводная таблица)

| БД | Как подключиться | Имя БД | Пользователь |
|---|---|---|---|
| Локальная | docker-контейнер `postgres`, порт 6432 | `backstage_plugin_mdb` | `dev` |
| Прод backstage_plugin_mdb | туннель `localhost:53480` | `backstage_plugin_mdb` | `backstage` |
| Testing backstage_plugin_mdb | туннель `localhost:53481` | `backstage_plugin_mdb` | `backstage` |
| Прод mdb-health | туннель `localhost:53482` | `health` | `admin` |

Туннели поднимаются через Teleport (скилл [`mcc-host-worker`](../../mcc-host-worker/SKILL.md)):

```bash
mcc tp-port-forward 1.db.mdb-etp-pgsql.pc.wan.idzn.ru:7432 --local-port 53480     # прод backstage
mcc tp-port-forward 1.db.mdb-testing-etp-pgsql.kc.wan.idzn.ru:7432 --local-port 53481  # testing
mcc tp-port-forward 1.db.mdb-health-mdb-pgsql.hc.one-infra.ru:7432 --local-port 53482  # прод mdb-health
```

Перед использованием проверяй, что туннель жив: `lsof -iTCP:53480 -sTCP:LISTEN`.

## Локальное подключение

```
Docker: postgres (postgres:14-alpine, порт 6432:5432)
БД: backstage_plugin_mdb
Пользователь: dev
Команда: docker exec postgres psql -U dev -d backstage_plugin_mdb -c "<SQL>"
```

Контейнер запускается через `cd stubs && docker-compose up -d`.

Локального `psql` на macOS нет — используй psql из postgres-контейнера (`pg_backstage_plugin_mdb`), подключаясь к localhost-порту через `host.docker.internal`.

## Прод backstage_plugin_mdb: SELECT (read-only по умолчанию)

Подключайся к продовой БД напрямую сам — **разрешение пользователя не нужно, но только SELECT (read-only)**. Никаких INSERT/UPDATE/DELETE без явного разрешения.

- Туннель: `localhost:53480`, БД `backstage_plugin_mdb`, пользователь `backstage`, пароль `HDX!cpw5yxf0ypd5tgd`
- Клиент — из контейнера `pg_backstage_plugin_mdb`, хост внутри docker — `host.docker.internal`:

```bash
docker exec -e PGPASSWORD='HDX!cpw5yxf0ypd5tgd' pg_backstage_plugin_mdb \
  psql -h host.docker.internal -p 53480 -U backstage -d backstage_plugin_mdb -At -c "<SELECT ...>"
```

На проде работает вся схема `public` (db_cluster, one_cloud_meta, db_cluster_version и т.д.).

### Правило одного запроса (jsonb_build_object)

Собирай данные с удалённой БД **одним SQL-запросом** через `jsonb_build_object` (или `jsonb_agg`) — на выходе один JSON. Это касается сбора схемы (`information_schema.columns`), исследовательских SELECT-ов, аналитики, seed-данных. Разнородные данные — отдельные ключи в `jsonb_build_object`.

⚠️ Грабли:
- **`ORDER BY` в `jsonb_agg` — только внутри скобок агрегата.** ❌ `SELECT jsonb_agg(to_jsonb(v)) FROM ... ORDER BY v.create_ts DESC` — конфликт с агрегацией. ✅ `SELECT jsonb_agg(to_jsonb(v) ORDER BY v.create_ts DESC) FROM ...` или подзапрос с `ORDER BY ... LIMIT N` во `FROM`.
- **Скалярный подзапрос `to_jsonb(t)` падает с `more than one row returned`**, если у таблицы уникальность по `(cluster_id, X)` (пример — `one_cloud_meta`, UNIQUE по `(cluster_id, params_type)`). Для таких таблиц всегда `jsonb_agg(to_jsonb(t) ORDER BY t.<колонка>)`. Перед подзапросом проверяй индексы через `\d <table>`.

Ручной запасной путь (туннель не поднят): сгенерировать SQL, пользователь выполняет на удалённом хосте через `mcc ssh` + `psql` и отдаёт результат.

### Шаблон: один запрос на все данные кластера

```sql
SELECT jsonb_build_object(
  'db_cluster', (SELECT json_agg(t) FROM (SELECT * FROM db_cluster WHERE id='<CLUSTER_ID>') t),
  'db_cluster_version', (SELECT json_agg(t ORDER BY create_ts DESC) FROM (SELECT * FROM db_cluster_version WHERE cluster_id='<CLUSTER_ID>' ORDER BY create_ts DESC LIMIT 5) t),
  'host_state', (SELECT json_agg(t) FROM (SELECT * FROM host_state WHERE cluster_id='<CLUSTER_ID>') t),
  'one_cloud_meta', (SELECT json_agg(t) FROM (SELECT * FROM one_cloud_meta WHERE cluster_id='<CLUSTER_ID>') t),
  'operations', (SELECT json_agg(t ORDER BY created_ts DESC) FROM (SELECT * FROM operations WHERE cluster_id='<CLUSTER_ID>' ORDER BY created_ts DESC LIMIT 10) t),
  'projects', (SELECT json_agg(t) FROM (SELECT p.* FROM projects p JOIN db_cluster c ON c.project_id=p.id WHERE c.id='<CLUSTER_ID>') t),
  'namespaces', (SELECT json_agg(t) FROM (SELECT n.* FROM namespaces n JOIN db_cluster c ON c.namespace_id=n.id WHERE c.id='<CLUSTER_ID>') t),
  'hardware_presets', (SELECT json_agg(t) FROM (SELECT hp.* FROM hardware_presets hp WHERE hp.id IN (SELECT DISTINCT hardware_preset_id FROM db_cluster_version WHERE cluster_id='<CLUSTER_ID>')) t),
  'settings', (SELECT json_agg(t) FROM (SELECT * FROM settings WHERE type IN ('kafkaResizeProcessingEnabledProjects','kafkaModifyProcessingEnabledProjects')) t)
);
```

## Прод backstage_plugin_mdb: DML (только с разрешения пользователя)

⚠️ Это прод: любые INSERT/UPDATE/DELETE — показать пользователю и получить явное разрешение до выполнения.

**Выполнение DML на проде:**
- psql через `docker cp` файла + `psql -f` — **stdin-heredoc молча не применяет DML!**
- Оборачивать в `BEGIN/COMMIT`, после — верификационный SELECT обеих таблиц.

**Комментарии в Jira по MDBSUP:** текст писать через скилл **`text-writer`** — кратко: результат + следующий шаг, без перечня действий. Детали (withdraw, SQL) остаются в чате/history.

### Диагностика зависших MDBSUP-операций (operations / host_state)

Диагностические SELECT'ы по кластеру:

```sql
SELECT id, status, type, attempts_left, in_processing, created_ts, finished_ts, left(error_message, 400) AS err
FROM operations WHERE cluster_id='<uuid>' ORDER BY created_ts DESC LIMIT 10;
SELECT host, shard_id, params->>'dc' AS dc FROM host_state WHERE cluster_id='<uuid>' ORDER BY host;
```

Шаблоны правок:

```sql
-- закрыть зависшую операцию
UPDATE operations SET status='done', in_processing=false, finished_ts=now(), error_message=NULL
WHERE id='<op_id>' AND status='failed';

-- добавить хост
INSERT INTO host_state (cluster_id, host, update_ts, onecloud_ui_link, grafana_dashboard_link, params, shard_id)
VALUES ('<cluster_id>', '<fqdn>', now(),
  'https://cloud.vk.team/cloud/<DC_UPPER>/ns/<ns>/service/<service_name>',
  'https://goc.vk.team/d/deahz1a8c50xsb/kafka-cluster?orgId=1&var-cluster=<cluster_name>&var-instance=<fqdn>&var-vm_datasource=P1D7AE08E5B4F8828',
  '{"dc": "<dc>"}'::jsonb, NULL);
```
(onecloud/grafana ссылки лучше копировать из строк соседних хостов кластера, заменяя dc/instance.)

**delete_hosts (Kafka):** помимо `DELETE FROM host_state` — поправить актуальную строку
`db_cluster_version` (последняя по `create_ts`): в `cluster_params.kafkaParams.controller.controllerDcs`
(jsonb-массив) убрать ДЦ удаляемого контроллера — там записываются ДЦ контроллер-хостов,
и без правки UI/API считают контроллер существующим.

**Грабли схемы:**
- У таблицы `operations` нет `updated_ts` — есть `created_ts/started_ts/finished_ts`.
- `host_state` не имеет колонки `status` — состояние хоста только в облаке/mcc.
- Порт продовой БД в port-forward: 53480 (7432 на хосте; в истории встречаются разные варианты — проверять `lsof -iTCP:53480`).

## Прод mdb-health (БД health)

Источник tier/warnings для UI mdb-data и для локального mdb-health (см. скилл `mdb-local-tester`). Туннель `localhost:53482`, креды в таблице выше.

```bash
docker exec -e PGPASSWORD='<пароль admin>' pg_backstage_plugin_mdb \
  psql -h host.docker.internal -p 53482 -U admin -d health -c "<SQL>"
```

Ключевые таблицы: `tier.tier_state`, `tier.tier_history` (после вставки — `setval` id_seq!), `warnings.cluster_warnings`; кластеры — `mirror.db_cluster` (фильтр `project_id=160` — mdbdev). Все таблицы mirror.*, tier.*, warnings.* лежат в этой же БД.

## Схема работы (сидинг)

1. **Получение задачи** — пользователь говорит: «скопируй таблицу X с удалённого хоста» или «для теста эндпоинта /api/y нужны данные».
2. **Анализ зависимостей** — по графу связей определить, какие родительские таблицы тоже нужно заполнить (FK-ограничения).
3. **Генерация SQL для удалённой БД** — один запрос через `jsonb_build_object`. Если туннель доступен — выполнить сам (read-only); иначе пользователь выполнит через `mcc ssh` + `psql`.
4. **Получение данных** — из прямого подключения или от пользователя.
5. **Вставка в локальную БД** — INSERT через `docker exec`, порядок: сначала родительские таблицы, потом дочерние.
6. **Верификация** — SELECT, чтобы подтвердить, что данные на месте.
7. **Подготовка данных для конкретного теста** — при необходимости обновить статусы версий, убрать/добавить поля в cluster_params для триггера нужной ветки кода.

## Граф связей таблиц (FK, backstage_plugin_mdb)

```
projects ──▶ db_cluster ◀── namespaces
                 │
                 ├──▶ cluster_to_template
                 ├──▶ db_cluster_version ──▶ hardware_presets
                 ├──▶ host_state ◀── db_shards
                 ├──▶ one_cloud_meta
                 ├──▶ operations ──▶ tasks
                 ├──▶ cluster_links
                 ├──▶ users ──▶ permissions ◀── databases
                 ├──▶ databases
                 ├──▶ cluster_alerts ──▶ alert_templates
                 ├──▶ cluster_notifications
                 ├──▶ extensions_state ──▶ extensions_info ──▶ db_versions
                 └──▶ db_shards

db_versions ──▶ db_version_dockers
             ──▶ extensions_info

cluster_alert_group ──▶ cluster_alert_rule
backup_repositories ──▶ backups
projects ──▶ services_auth
roles ──▶ role_mappings
```

**Изолированные таблицы** (без FK-связей, можно заполнять независимо):
kafka_config, db_params, criticality_level, datacenters, hardware_presets, log_templates, settings, services_api_auth, knex_migrations, knex_migrations_lock

## Названия timestamp-колонок (частая ошибка!)

В разных таблицах разные имена — не путай:

| Таблица | Колонка создания | Колонка обновления |
|---|---|---|
| `db_cluster` | `create_ts` | `update_ts` |
| `db_cluster_version` | `create_ts` | `update_ts` |
| `host_state` | — | `update_ts` |
| `operations` | **`created_ts`** | — |
| `tasks` | `created_ts` | — |
| `projects` | — | — |
| `one_cloud_meta` | — | — |

⚠️ `operations.created_ts` (с `d`), `db_cluster_version.create_ts` (без `d`). Перед `ORDER BY` проверяй имя колонки через `\d <table>` на удалённой БД.

## Правила

1. **Прод по умолчанию read-only.** DML — только с явного разрешения пользователя, SQL показывать до выполнения.
2. **Порядок вставки** — сначала родительские, потом дочерние. При удалении — наоборот.
3. **Конфликты PK** — перед вставкой проверяй, нет ли уже данных. Используй `ON CONFLICT DO UPDATE` для обновления.
4. **Минимальный набор** — копируй только те строки, которые реально нужны для теста. Не тащи всю таблицу без необходимости.
5. **Единая команда для удалённой БД** — один SELECT через `jsonb_build_object` (см. раздел выше и грабли ORDER BY / to_jsonb).
6. **Экранирование** — при вставке данных экранируй одинарные кавычки в строках (`'` → `''`).
7. **Sequences** — после вставки с явными id сбрасывай sequence: `SELECT setval('<table>_id_seq', (SELECT MAX(id) FROM <table>));`
8. **Русский язык** — все пояснения на русском, лаконично.
