# MDBSUP-5505 — 2026-09-17 — CH stat (media-infra): add_hosts падает — образ из замороженного инпута; за 404 вскрылась квота облака

- **Кластер:** `stat` (media-infra), `e02f53e3-aa8d-4fd3-b8e9-8c32be706097`, CH 24.3, sharded, hosts: shard1/shard2 × {hc,kc,pc} + keeper × {hc,pc,sc}; операция добавляет +1 реплику в sc на shard1 и shard2
- **Операция:** add_hosts `1b3d9986-45d2-4b1c-a5f1-8cf71feb2828` (создана 12:17), workflow `addChReplicas`, task queue `clickhouse-activities-queue`
- **Статус:** ОТКРЫТ — ждём квоту облака у продукта

## Цепочка проблем (по мере вскрытия)

1. **404 на образе из снапшота** (первый фейл 12:17): `Image manifest dzen-external-registry.odkl.ru/ubuntu20-clickhouse-24.3.2.23:2.0.2 not found`. Тот же паттерн, что MDBSUP-5226: операция при создании снимает снапшот `db_cluster_version.db_version->'dockers'`, тот устарел относительно эталона `db_version_dockers` (CH 24.3: service `3.0.8`, keeper `2.0.4`).
2. **Фикс снапшота не чинит РЕТРАЙ той же операции**: docker-тег заморожен в **инпуте workflow** (`serviceConfig.dockerTag`, base64-payload в `WORKFLOW_EXECUTION_STARTED`). Ретрай 15:56 с тем же operationId переиграл старый инпут → тот же 404. Снапшот-фикс действует только на НОВЫЕ операции.
3. **Ручной перезапуск через Temporal API с патчем инпута** — рабочий обход (см. ниже). С тегом 3.0.8 createShardService прошёл валидацию registry → 3.0.8 в dzen-registry ЕСТЬ.
4. **Следующий слой — квота облака** (фейл 16:18): `product 1465 (media-infra): LAN out=19.41Gbit > quota 19.28Gbit, unsatisfied 131Mbit`. Каждая реплика просит `lanOut=1023M`, нужно 2 реплики ≈ +2 Gbit. Чинится только продуктом (ресурс-менеджер/освобождение хостов).

## Ручной запуск workflow через Temporal HTTP API (работает!)

UI-прокси `mdb-processing-temporal.common.mdb.one-infra.ru` умеет POST start, но:

- **CSRF**: сначала GET любой страницы UI с `-c jar` (кука csrf), затем POST с заголовком `X-Csrf-Token: <значение из куки>`.
- **Формат инпута**: `input.payloads[0] = {metadata: {encoding: "anNvbi9wbGFpbg=="}, data: <base64 json>}` — не плоский payload.
- **Тот же workflowId обязателен** (sync результата в mdb-data идёт по `workflowId = operationId`); т.к. прошлые run'ы упали — `workflowIdReusePolicy: WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE_FAILED_ONLY`.
- Тип/очередь/инпут берутся из истории предыдущего рана: `workflowType.name`, `taskQueue.name`, `input.payloads[0].data` (декодировать base64 → править поля → энкодить обратно).
- POST `/api/v1/namespaces/default/workflows/<workflowId>` → `{"runId":..., "started":true, "status":"...RUNNING"}`.

## Грабли

- 404-на-образе vs квота легко перепутать по «операция упала»: текст только в Temporal failure-цепочке (activity `ch_createShardService` → `CloudClientException: Master responded with status ...`), в operations.error_message — только «Could not execute request».
- Эмпирическая проверка наличия тега в dzen-registry с хоста невозможна (докера/кредов на CH-хостах нет, анонимный token-flow registry закрыт 401). Косвенная проверка — запуск самой операции или свежие успешные операции других кластеров с этим тегом.
- Квота проверяется облаком при submit сервиса — фейл быстрый (секунды), данных в облаке не создаёт.
- Блокировка «Already has unapplied operation» для новых операций снимается закрытием failed-операции в БД (шаблон в db-worker) — на 2026-09-17 НЕ делали, т.к. ждём квоту и хотим доделать ту же операцию перезапуском.
