---
name: db-seed
description: Используй этот скилл, когда нужно заполнить локальные таблицы данными из удалённой базы для тестирования API. Работает по схеме: пользователь указывает что и откуда копировать → скилл генерирует SQL-запросы для удалённой БД → пользователь выполняет их и отдаёт результат → скилл вставляет данные в локальную БД.
allowed-tools: [bash]
---

# Скилл заполнения локальной БД тестовыми данными (db-seed)

Ты работаешь в режиме DBA-ассистента для тестирования. Цель — быстро наполнять локальную БД реальными данными из удалённой, чтобы API-запросы проходили успешно.

## Обязательный шаг: сканирование истории

**Перед началом** — проверь папку `history/` на наличие готовых seed-файлов для текущей задачи.

```bash
ls .claude/skills/db-seed/history/
```

Если есть — используй готовые INSERT-запросы вместо генерации новых.

## Локальное подключение

```
Docker: postgres (postgres:14-alpine, порт 6432:5432)
БД: backstage_plugin_mdb
Пользователь: dev
Команда: docker exec postgres psql -U dev -d backstage_plugin_mdb -c "<SQL>"
```

Контейнер запускается через `cd stubs && docker-compose up -d`.

## Схема работы

1. **Получение задачи** — пользователь говорит: «скопируй таблицу X с удалённого хоста» или «для теста эндпоинта /api/y нужны данные».
2. **Анализ зависимостей** — по графу связей определить, какие родительские таблицы тоже нужно заполнить (FK-ограничения).
3. **Генерация SQL для удалённой БД** — написать SELECT-запросы, которые пользователь выполнит на удалённом хосте (через скилл [`mcc-host-access`](../mcc-host-access/SKILL.md), `mcc ssh` + `psql`). **Всегда одним запросом** через `jsonb_build_object` (см. шаблон ниже), чтобы пользователь получил единый JSON — не несколько копий вывода.
4. **Получение данных** — пользователь передаёт результат запросов.
5. **Вставка в локальную БД** — сгенерировать и выполнить INSERT-запросы через `docker exec`, соблюдая порядок (сначала родительские таблицы, потом дочерние).
6. **Верификация** — выполнить SELECT, чтобы подтвердить что данные на месте.
7. **Подготовка данных для конкретного теста** — при необходимости обновить статусы версий, убрать/добавить поля в cluster_params для триггера нужной ветки кода.

## Граф связей таблиц (FK)

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

## Правила

1. **Порядок вставки** — сначала родительские, потом дочерние. При удалении — наоборот.
2. **Конфликты PK** — перед вставкой проверяй, нет ли уже данных. Используй `ON CONFLICT DO UPDATE` для обновления.
3. **Минимальный набор** — копируй только те строки, которые реально нужны для теста. Не тащи всю таблицу без необходимости.
4. **Единая команда для удалённой БД** — ВСЕГДА собирай данные с удалённого хоста одним SQL-запросом через `jsonb_build_object` (или `jsonb_agg`), чтобы пользователь получил один JSON, а не несколько выводов. Это касается:
   - сбора схемы таблиц (`information_schema.columns`) — одним запросом;
   - исследовательских SELECT-ов (distinct values, sample rows) — одним запросом;
   - аналитических запросов (средние, перцентили, сравнение до/после) — одним запросом;
   - сбора seed-данных по кластеру — одним запросом по шаблону ниже.
   Никаких нескольких `;`-разделённых запросов в одном блоке — только один `SELECT jsonb_build_object(...)`. Если нужно собрать разнородные данные, вкладывай каждый подзапрос как отдельный ключ в `jsonb_build_object`.
   - ⚠️ **`ORDER BY` в `jsonb_agg` — только внутри скобок агрегата**, не снаружи подзапроса. Иначе Postgres падает с `column "v.create_ts" must appear in the GROUP BY clause or be used in an aggregate function`.
     - ❌ `SELECT jsonb_agg(to_jsonb(v)) FROM ... ORDER BY v.create_ts DESC` — ORDER BY относится ко всему SELECT, конфликтует с агрегацией.
     - ✅ `SELECT jsonb_agg(to_jsonb(v) ORDER BY v.create_ts DESC) FROM ...` — ORDER BY внутри агрегата, корректно.
   - Альтернатива (как в шаблоне ниже): подзапрос с `ORDER BY ... LIMIT N` во `FROM`, и `jsonb_agg(t)` без ORDER BY снаружи — тоже рабочий вариант.
   - ⚠️ **Скалярный подзапрос `to_jsonb(t) FROM ... WHERE cluster_id=...` падает с `more than one row returned by a subquery used as an expression`**, если у таблицы уникальность по `(cluster_id, <другая колонка>)`, а не по одному `cluster_id`. Типичный пример — `one_cloud_meta` (UNIQUE по `(cluster_id, params_type)`, у кластера есть строки `params_type='db-service'`, `'kafka'`, …). Для таких таблиц ВСЕГДА используй `jsonb_agg(to_jsonb(t) ORDER BY t.<колонка>)`, не скалярный `to_jsonb`. Перед написанием подзапроса проверяй индексы через `\d <table>` на удалённой БД — если видим multi-column unique index по `cluster_id + X`, агрегируй.
5. **Экранирование** — при вставке данных экранируй одинарные кавычки в строках (`'` → `''`).
6. **Sequences** — после вставки с явными id сбрасывай sequence: `SELECT setval('<table>_id_seq', (SELECT MAX(id) FROM <table>));`
7. **Русский язык** — все пояснения на русском, лаконично.

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

⚠️ `operations.created_ts` (с `d`), `db_cluster_version.create_ts` (без `d`). Перед написанием `ORDER BY` проверяй имя колонки через `\d <table>` или `\d+ <table>` на удалённой БД.

## Шаблон: один запрос на все данные

Всегда запрашивай данные одним SQL через `jsonb_build_object` — пользователь получает один JSON, не несколько выводов:

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
