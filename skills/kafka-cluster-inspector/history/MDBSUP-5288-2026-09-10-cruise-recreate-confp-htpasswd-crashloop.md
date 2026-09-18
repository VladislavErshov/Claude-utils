# MDBSUP-5288 — Cruise Control не создаётся на communal (crash-loop confp/htpasswd)

- Дата: 2026-09-10
- communal `167c2774-1c41-494e-a427-00e11e946ba3` (prod, project integr-platform, ns infra)
- Операция `create_additional_service` `a16aa6cd-0b0a-4206-9ef3-d8e521b01f06`, workflow
  `CreateKafkaCruiseWorkflow` (3 фейл-рана: 09.09 16:11/16:50, 10.09 08:43 МСК)
- Cruise-инстанс: `1.cruise.communal-integr-platform-kafka.pc.one-infra.ru` (пересоздание в pc
  после вывода из hc в MDBSUP-5263)

## Причина (цепочка)

1. UI-форма «Добавить Cruise» → `CreateKafkaCruiseRequest.password` (валидация только
   `@NotEmpty`) → `KafkaHostMapper` → Temporal `cruiseUserPassword` → activity
   `createCruiseUserSecret` пишет в vault `zkv/mdb/integr-platform/kafka/<fullQueue>/cruise`,
   ключ `password`.
2. В поле пароля попал **текст ошибки UI** от первой неудачной попытки («Не удалось создать
   Cruise Control … Conflict … Already has active or failed operation for cluster
   86a382dc…» — guard по stg-kafka). В коде UI пути ошибки в поле нет — вставка из буфера
   в замаскированный `PasswordField` (проверить невозможно, но единственное реалистичное
   объяснение).
3. confp хоста рендерит `.htpasswd` = `cruise:<пароль>` → `/opt/fix_htpasswd.py`
   (`fileData.split(":")`, строго 2 части) → `ValueError: too many values to unpack` →
   `confp-init.service` fail → OnFailure → контейнер гасится за ~5 c → crash-loop →
   waiter 30 мин → `ServiceNotRunning`.

## Диагностика (что где смотрели)

- Temporal: вход workflow (`cruiseUserPassword` = текст ошибки), вход
  `createCruiseUserSecret`, `operations.operation_model` (пароля нет) — реального пароля
  в операции не существовало никогда.
- PMS app=mdb: `kafka.cruisecontrol.{sysconfig,jaas.conf,capacity.json,…}` — на кластерном
  ключе `<queue>.clouds`, заполнены и валидны (НЕ на `cruise.<queue>.clouds`).
- Хост: `VAULT_ADDR=https://pc.vault.<dc>.one-infra.ru` из PID1 env, токена в env/файлах
  нет — конфп логинится через `vault-login.service` (JWT). Прямой curl с пустым токеном —
  403 (не диагностично).
- `fix_htpasswd.py`/`htpasswd.j2` в docker-images старые, не менялись — виноваты данные.

## Что сделано

- Пользователь перезаписал vault-секрет `.../cruise` (ключ `password`) на валидный пароль
  (v6) — **недостаточно**: crash сохранился, host читает не то (см. «Открытое»).
- Пользователь поднимал инстанс с флагом «не гасить контейнер» для живой отладки;
  после `ATTEMPTS_LIMIT` автологина нет — поднимать руками
  `mcc --local -n infra -c PC start cruise.<queue>`.
- mdb-data (ветка `ershov/MDBDEV-2900-downscale-brokers-save-map`, MR !496):
  - `6316abd3` — UUID-валидация пароля в `KafkaHostsValidator.validateCreateCruiseControl`
    (сигнатура + `request.password()` на call-site, тесты);
  - `a248fb18` — канонический UUID (сравнение `toString()` с исходной строкой, отсекает
    `1-1-1-1-1`) + `@Schema` на поле и описание 400 в `KafkaHostsDataApi`; ревью-треды
    #3/#6 закрыты.
  - ⚠️ Фикс разворачивается только с деплоем mdb-data.

## Открытое

- **ok-pyvault — ГИПОТЕЗА СНЯТА (10.09)**: `mdb/*`-свойства в PMS-приложении ok-pyvault
  для communal оказались в порядке — причина crash-loop'а в нём НЕ лежит. Копать дальше:
  почему confp рендерит `.htpasswd` с лишними `:` при исправленном секрете v6
  (кандидаты: фактический vault-путь/ключ, который читает pyvault на хосте; версия
  секрета на конкретном VAULT_ADDR хоста; состояние `vault-login`/токена на хосте).
- Известный шум: `mcc instances` принимает только полное имя FQDN (не сабстринг/сервис);
  `mcc logs` — follow-режим (фонить и убивать); plait-`reported_progress` не диагностичен.
