# 2026-09-03 — MDBDEV-3305 / MDBSUP-5068: операция 14:51 не получила новый ACL (oneme, dwh_rt_antifraud_reader)

⚠️ Времена в Temporal API — **UTC**, в итогах всегда пересчитывать в МСК (+3).
Первая версия этой заметки путала UTC/MSK — таблица ниже исправлена.

Кластер: oneme Kafka `14aa4059-4d9c-4cd3-a852-35d3e06629f9`, юзер `dwh_rt_antifraud_reader`,
ACL: READ+DESCRIBE на GROUP `oneme_dwh_realtime_metrics_dev_group`.

## Операции (createKafkaUser, все COMPLETED, инпуты из WorkflowExecutionStarted)

| МСК | UTC | operationId | perms | dwh_realtime_metrics |
|---|---|---|---|---|
| 02.09 14:38 | 11:38 | 543e56e8-... (antispam-internal) | 151 | — |
| 02.09 14:51:45 | 11:51 | 441224de-c37d-4fac-8aa9-b0278f1b71ec | 36 | **НЕТ** (первая операция по юзеру за день) |
| 02.09 16:32 | 13:32 | 914e5927-8fab-4e82-a013-b63c3e15f032 | 34 | **есть** (2) |
| 02.09 17:48 | 14:48 | 5cd87258-f901-4c72-aef9-a02bf3ac0cb0 | 38 | **есть** (2) |
| 02.09 20:42 | 17:42 | f27c37e8-... (oneme_yt_reader) | 89 | — |

## Выводы

- Первая операция по юзеру в день инцидента (14:51:45 МСК) уже вышла БЕЗ нового ACL —Grant
  в 14:51 прошёл мимо неё, никакого последующего затирания операциями нет (поздние операции
  ACL несут). Processing применил ровно то, что получил; проблема не на его стороне.
- ACL появился в операциях с 16:32 → между 14:51 и 16:32 он попал в БД. Единственный писатель
  permissions в БД — обратный синк: `KafkaSyncServiceImpl.permissionsChanged` →
  `KafkaServiceImpl.updateExistingUser` → `permissionService.updateGlobalPermissions`.
  Код реконсилации permissions добавлен коммитом `d5dceb54` от 31.08 (mdb-data, замаскирован
  под MDBDEV-3245).
- ОПРОВЕРГНУТО позднее: «ACL выдали напрямую в Kafka, синк подтянул» — синк за 02.09 по юзеру
  не писал ни разу; групповые ACL появились в 16:32 только ручным вводом в диалоге. См.
  «Итоговую цепочку» ниже.
- MDBDEV-3305 закрыт как неактуальный (проблема не в отсутствии реконсилации ACL в синке).

## Подтверждение логами mdb-data (03.09)

- Оп 441224de создана в `14:51:45.483 МСК` на `1.mdb-data.mdb-data.kc` через
  `KafkaController.updateKafkaUser`, автор `a.perevalov` (строка OperationServiceImpl
  «Created operation ... with model KafkaUserModel(...)»). Тело запроса mdb-data НЕ логирует
  (ControllerLogAspect — только STARTED/COMPLETED; toString модели без permissions).
- Синк 02.09: `[syncKafkaUsers]` ни разу не упоминает dwh_rt_antifraud_reader (все 6 хостов,
  оба гза + текущий app.log) → для синка Kafka-состояние юзера == БД, дрейф он не видит.
- UI: edit-модалка юзера (feature flag mdbKafkaCreate) сабмитит полный список
  `form.permissions` (из `currentUser.permissions.kafkaUserPermissions` ← GET ← БД),
  KafkaFieldsV2 FieldArray name='permissions' — путь консистентен; других флоу, меняющих
  ACL юзера, в UI нет.

## Итоговая цепочка (уточнено после прочтения MDBSUP-5068)

Сап от a.perevalov: выдавались права на топик `oneme_antispam_pr_idsSpamAction` И группу
`oneme_dwh_realtime_metrics_dev_group`, «применились и пропали через час».

Фактические инпуты операций:

| МСК | оп | топик ACL | группа ACL | эффект |
|---|---|---|---|---|
| 14:38 | 543e56e8 (antispam-internal, 151) | — | — | соседнее действие |
| 14:51:45 | 441224de (36) | **+4 ЕСТЬ** | **0 НЕТ** | топик применился; группа — не применялась вовсе |
| 16:32 | 914e5927 (34) | **0 — СНЯЛ** | +2 | группа применилась; full-upsert снял топик → ошибка datastream 17:15 |
| 17:48 | 5cd87258 (38) | +4 | +2 | финальное состояние ок |

1. Сейв 14:51: групповые ACL потеряны ДО бэка — их нет в запросе (маппер pass-through,
   упавших запросов нет: единственный FAILED updateKafkaUser за день — чужой кластер
   7ebfc7c1, 409, 18:17). Либо форма потеряла строки, либо сейв с группой не был отправлен.
2. Сейв 16:32: диалог = БД-стейт (без топичных ACL, т.к. синк не подтянул их из Kafka:
   ни одной `updating dwh_rt_antifraud_reader` за 02.09) + добавленная группа →
   full-upsert молча снял топик. «Пропали через 60 минут» = вот это.
3. БД для юзера между сейвами не менялась (синк не писал, операций между нет) —
   топичные ACL в 16:32 появились в форме только ручным вводом.

Баги: (1) full-list submit модалки юзера из стейл-снапшота БД молча снимает Kafka-only ACL;
(2) синк не видит реальный срез ACL Kafka (дрейф не детектится и не лечится);
(3) потеря групповых строк в первом сейве — требует репро/слов a.perevalov.

## Статус (03.09, конец разбора) — ОТЛОЖЕНО до повторения

Финальные установленные факты:
- `updateKafkaUser` по юзеру за 02.09 — ровно 3 запроса: 14:51:45 (kc-1, `441224de`),
  16:32:30 (kc-1, `914e5927`), 17:48:52 (pc-1, `5cd87258`). Второго «потерянного» запроса
  с группой НЕ было (FAILED за день один — чужой кластер `7ebfc7c1`, 409, 18:17).
- Первый сейв (14:51): топичные ACL (+4) в запросе есть, групповые (которые он, по сапу,
  вносил) — 0. Потеря строго на клиенте, до mdb-data.
- UI-код edit-модалки kafka-юзера не менялся с мая 2026 (`739cb16` ONEUI-3247) — текущий
  мастер репрезентативен для прода. Статический анализ явной точки потери не нашёл
  (KafkaFieldsV2 FieldArray name='permissions' консистентен с initialValues и сабмитом).

Решение: баг отложен до повторения. При повторении — план репро на локальном стенде
(mdb-local-tester, стенд поднимается целиком за минуты):
1. В vkone-stub (`~/.claude/skills/mdb-local-tester/scripts/vkone-stub.mjs`) включить
   флаг: `flags: { mdb_kafka_create: { value: 'true' } }` (прод-путь v2 `KafkaController.updateKafkaUser`;
   пустой стаб = flag OFF = legacy v1-путь). Рестарт стаба (порт 8090).
2. Засидить kafka-юзера: таблицы `users` + `permissions` в 6434 (pg_backstage_plugin_mdb)
   — на 03.09 там пусто; кластер взять из сидированных kafka/mdbdev (project 160).
3. Автоматизация: puppeteer-core + системный Chrome (`/Applications/Google Chrome.app`),
   без скачивания браузера; `page.on('request')` — перехватить PATCH `updateKafkaUser`
   и сверить body с введёнными строками.
4. Сценарии: (а) happy path — добавить ACL, сабмит, ACL в body?; (б) гонка — добавить
   строки ДО загрузки currentUser / триггернуть refetch при открытой форме (react-final-form
   переинициализирует по смене initialValues → строки могут стираться); (в) два сейва
   подряд из одной модалки.
5. Спросить a.perevalov про флоу 14:51 (одна модалка или две; жал ли «Сохранить» после
   добавления группы) — дёшево и может закрыть вопрос без репро.

Кандидаты в тикеты (не заведены): (1) full-list submit модалки юзера молча снимает
Kafka-only ACL; (2) синк не видит все ACL Kafka (дрейф не детектится).

## Грабли

- Workflow type `createKafkaUser` используется и для update юзера (upsert-семантика).
- В инпуте workflow dto обёрнут: `.dto.permissions`, в activity input — `.permissions` напрямую.
- UI-save юзера НЕ пишет permissions в БД напрямую — БД обновляется только через синк.
- mcc: хосты mdb-data видны только с `-c <dc>` (мастера по ДЦ), `instances %mdb-data%` без
  `-c` у базового ДЦ ничего не находит; mdb-data в hc сейчас называется mdb-data-testing.
- Тела HTTP-запросов в логах mdb-data нет; фактический инпут операции — в Temporal.
