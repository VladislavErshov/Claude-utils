# MDBDEV-3301 — топики: otherProperties + собственная persisted-модель (local, 2026-09-15)

Ветка mdb-data: `ershov/MDBDEV-3301-topic-other-properties` (коммит 91a1c56e — реверт на own KafkaTopicConfig).
Кластер: **test-modify3** `9fc47c1b-011d-4aaa-b411-de5345a0204e`. mdb-data 8081 (local), pg 6434.

## Auth для internal-эндпоинтов

`Authorization: <raw JWT HS256>` (без Bearer), claim `serviceName=mdb-processing` (sync: `one-cloud-ops`),
секрет `my-very-long-secret-key-that-is-at-least-32-chars-long` (application-local.yml).
Генератор: `python3 /tmp/mdb-jwt.py [service]`.

## Seed

```sql
INSERT INTO databases (cluster_id, name, created_by, created_ts, settings, is_deleted, is_system)
VALUES ('9fc47c1b-011d-4aaa-b411-de5345a0204e'::uuid, 'test-topic-3301', 'service.mdb-processing', NOW(),
'{"kafkaSettings":{"partitions":3,"replicationFactor":3,"config":{"retentionMs":604800000,"maxCompactionLagMs":9223372036854775807,"minInsyncReplicas":2}}}'::jsonb, false, false);
```

## Результаты

| # | Сценарий | Статус | Факт |
|---|---|---|---|
| TP1 | internal PUT полный configs + otherProperties, pc=6 | PASS | partitions=6, otherProperties в jsonb, maxCompactionLagMs **строкой**, created_by сохранён |
| TP2 | internal PUT configs=null, pc=9 | PASS | конфиг сохранён байт-в-байт (patch-контракт), partitions=9 |
| TP3 | internal PUT частичный configs (retentionMs) | PASS | replace: otherProperties/minInsync из старого конфига ушли (ожидаемо) |
| TP4 | POST /databases/multi ×2 | PASS | обе строки, created_by=service.mdb-processing |
| TP5 | sync без изменений (otherProperties не в payload) | PASS | строка не тронута (маркер updated_by выжил) — лишнего upsert нет |
| TP6 | sync retentionMs=333333 без otherProperties | PASS | retentionMs новый + **otherProperties смержены из сохранённого** |
| TP7 | user API retentionMs=-5 | PASS | 400 `errors[{field:"retentionMs"}]`, операция не создана |
| TP8 | legacy-строка (maxCompactionLagMs числом) | PASS | прочитана без потерь, после TP1 перезаписана строкой |

Бонус-проверка: otherProperties-валидация на own-модели — `remote.log.copy.disable` на не-4.3 кластере → 400
`errors[{field:"otherProperties.remote.log.copy.disable"}]`.

Вывод: реверт на собственную persisted-модель (KafkaTopicConfig + otherProperties, Dto только на границе)
работает end-to-end на реальном HTTP+jsonb пути; формат `databases.settings` совместим со старыми строками.

## Финализация MR (тот же день)

- Ветка перебейзена на master (в !502 checkers/iswan уже вмержены), конфликт импортов в
  DatabaseDaoImplTest разрешён, потерянный автомерджем `java.util.Map` в KafkaSyncServiceImpl возвращён.
- Реверт собственной модели закоммичен: `MDBDEV-3301: restore own KafkaTopicConfig persisted model,
  keep KafkaTopicConfigDto only at processing boundary` (91a1c56e) — commit-месседж на английском по
  требованию пользователя.
- Описание MR !493 переписано под финальное состояние: persisted-модель собственная, Dto на границе,
  контракт patchSettings (компактный конструктор — термин полностью, не «compact-ctor»), добавлена секция
  «Как проверялось» с результатами TP1–TP8 на test-modify3.
- Хвост: заголовок MR всё ещё «drop duplicate KafkaTopicConfig model» — предложить переименование.
