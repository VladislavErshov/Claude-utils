# 2026-09-08: B-сценарии downscale брокеров (MDBDEV-2900) — партия 1

Кластеры: modify3 `9fc47c1b`, modify4 `3fb46c41-fec6`, downgrade7 `23f108ac-1907`.
Стартовое состояние: по 1 брокеру в 4 ДЦ + ic:1 из prep 07.09 (все COMPLETED, data на ic-брокерах).
Ветка processing `ershov/MDBDEV-2900-downscale-kafka-brokers` (5c68c972), mdb-data на 8081.

## Инфра-находки сессии

1. **Stale-рантайм**: processing был поднят (11:07) ДО финальных amend-коммитов ветки (19:23–19:30
   07.09) — `DELETE /hosts/brokers` отдавал 405/500. Лечение: `clean bootRun` после чекаута.
2. **Снова потерялся redirect mdb-data**: `application-local.yaml`
   `external.api.namespaces.infra.mdb-data.base-url` был `:8088` (wiremock) → save-фаза upscale
   падала 404 `save/upscaled-brokers`. Вернул `:8081`. **Правка незакоммичена** — при чекауте
   веток теряется (уже третий раз).
3. On-host kafka CLI (mcc sshexec + kafka-topics/metadata-quorum) сегодня стабильно падает
   (AdminClient timeout `listNodes` + «Connection closed by remote host») — живость кластера
   верифицировалась локальным AdminClient (drain/describe/unregister проходили).

## НАЙДЕННЫЕ БАГИ (фиксы в этой сессии, незакоммичены)

1. **TOPIC_NOT_EXISTS на internal-топиках** (downgrade7 B3, прод-паттерн любого кластера с
   consumer-группами): `KafkaHostActivityImpl.validateTopicsExist` сверял со
   `getTopicNames()` = `listTopics(listInternal(false))`, а downscale шлёт в reassign-child
   `topics=null` → `listAllTopicNames()` (С internal) → `__consumer_offsets` «missing» →
   non-retryable падение reassign-child. На modify3 не воспроизводилось (нет
   __consumer_offsets — консьюмеров не было). ФИКС: `validateTopicsExist` →
   `client.listAllTopicNames()`.
2. **Инверсия цикла drain-поллинга** (критический): `waitBrokersDrained` —
   `while (noneMatch(predicate))` = «пока ВСЕ дренированы — спать». Флоу висел в поллинге,
   когда всё уже дренировано (поллы `true`), и вышел бы БЕЗ ожидания при not-drained; по TTL
   кидал бы `BROKER_NOT_DRAINED` в полностью дренированном состоянии. ФИКС: `anyMatch`.
3. **Ретрай тем же operationId падал на reassign-child**: `{opId}_reassign-partitions`
   COMPLETED в прошлом ране → `WORKFLOW_ALREADY_EXISTS` (reuse policy
   ALLOW_DUPLICATE_FAILED_ONLY). reconcile-child обёрнут `runIgnoringAlreadyStarted`,
   reassign — НЕТ. ФИКС: `runIgnoringAlreadyStarted` вокруг старта reassign-child.

## Прогоны

| # | Сценарий | Кластер | opId/wfId | Результат |
|---|---|---|---|---|
| B9 | Guard увеличения ic:1→2 | downgrade7 | fccb2997 | **PASS**: ic-child `DOWNSCALE_NOT_ALLOWED` (Current: 1, target: 2), parent `PARTIAL_DOWNSCALE_FAILURE`; pc/kc/hc no-op; zero side-effects |
| B1 | Happy path rescale ic 2→1 | modify3 | 1f3e2c71 | **PASS** после багфиксов (2 рана на инвертированном цикле терминированы, 3-й ран COMPLETED): reassign все 76 партиций (вкл. CC-топики) → drain → unregister 23002 → rescale. Партиции `test` ушли с 23002, ISR полный |
| B4 | Повтор после успеха (новый opId, та же цель) | modify3 | f734d186 | **PASS**: removedHosts пуст → все фазы skip, COMPLETED без side-effects |
| B3 | target=0 withdraw ic | downgrade7 | ad9430ce | **PASS**: stop→withdrawService→withdrawStorage по эталону D2; ретрай тем же opId после багфикса #1 довёл (ребёнок ic: 5×getServiceInfo, stop, withdraw, storage) |
| B10 | RF-guard (survivors 2 < RF 3) | downgrade7 | 5b0a1881 | **PASS**: `REASSIGN_INVALID_TARGET_BROKERS` «Target brokers count 2 < required RF 3» в reassign-child ДО изменений; в истории только listTopicNames+describePartitions |
| B2 | Пачка 2 ДЦ (2.pc+2.ic из 2×2) | modify4 | 62c713bf | **PASS**: ОДИН reassign-план (survivors [25001,21001,22001,23001] — сортировка по host, topics=null), параллельные дети pc/ic rescale, COMPLETED |
| B5 | Рестарт после unregister до rescale | modify3 | f44bc768 | **PASS**: ран 1 — terminate в момент ic-child (после unregister; drain оказался мгновенным) → ран 2 тем же opId: describeBrokerIds не нашёл 23002 → reassign+unregister SKIP → дети довели rescale → COMPLETED |
| B12 | Контракт input | — | — | **PASS**: parent `brokersPerDc` абсолютные, `connectionParams` (bootstrap+vault-path), `isWan`, TTL=21600s=6ч; reassign-child `topics=null`, `targetReplicationFactor=null`, survivors детерминированы |
| B13 | Десинк host_state | все | — | **Подтверждён**: после COMPLETED downscale удалённые брокеры остаются в host_state (save не реализован); повторный запуск с той же целью — no-op с корректной целью. Ручная чистка хвостов выполнена на всех 3 кластерах перед rollback-upscale |
| B6 | Terminate строго В drain-поллинге | modify3 | f44bc768 (ран1) | **Частично**: дренаж 3 партиций занял <30с — окно не поймано, terminate попал после unregister (см. B5). Семантика «retrey тот же opId» покрыта B1-retry/B5 |
| B7 | Битый vault-секрет | modify3 | c873b90b | **PASS**: секрет `super` перезатёрт → AdminClient ретраи в describeBrokerIds → секрет восстановлен → ТЕМ ЖЕ раном сошёлся (ретраи activity переживают восстановление) → COMPLETED |
| B8 | Partial failure (terminate одного ребёнка в пачке) | modify4 | 91aaf192 | **PASS**: terminate pc-child посреди rescale → ic-child дошёл → parent FAILED `PARTIAL_DOWNSCALE_FAILURE` «failed in 1 DC(s): [pc]» → ретрай тем же opId → COMPLETED (pc доведён) |
| B14 | Серия terminate на одном opId | modify4 | c59e59f6 | **PASS**: 2×2 → terminate#1 (drain-фаза) → ретрай → terminate#2 (дети pc/ic) → ретрай → ран3 COMPLETED, дети COMPLETED; состояние не деградировало |
| B11 | Drain timeout до TTL | — | — | **Не воспроизводим локально**: нет рычага замедления репликации (throttle недоступен, on-host CLI сломан); отложен на стенд |

## Полный журнал запусков (хронология, все раны)

| # | Действие | Кластер | opId/wfId | Итог |
|---|---|---|---|---|
| 1 | POST upscale ic:2 (prep B1) | modify3 | 0ec7579d (mdb-data) | ран1 FAILED: save → wiremock 404 `save/upscaled-brokers` (base-url :8088). Операция закрыта руками |
| 2 | — | — | `18759ca4` | Ретрай upscale (converge): дети skip, save → :8081 OK → COMPLETED, host_state +2.broker.ic |
| 3 | B9 попытка 1 | downgrade7 | 3684efc2 | HTTP 500: stale-рантайм без DELETE /brokers → рестарт processing №1 (clean bootRun) |
| 4 | B9 | downgrade7 | fccb2997 | PASS (PARTIAL_DOWNSCALE_FAILURE ← DOWNSCALE_NOT_ALLOWED ic) |
| 5 | fill reassign test→5 id | modify3 | b04a289c | COMPLETED |
| 6 | B1 ран 1 | modify3 | 1f3e2c71 | reassign-child OK, drain-поллинг ВЕЧНЫЙ (баг #2, ещё не известен) |
| 7 | B3 ран 1 | downgrade7 | ad9430ce | FAILED: TOPIC_NOT_EXISTS `__consumer_offsets` → багфикс №1 + рестарт №2 |
| 8 | B3 ран 2 (same opId) | downgrade7 | ad9430ce | FAILED: WORKFLOW_ALREADY_EXISTS на reassign-child → багфикс №3 + рестарт №3 |
| 9 | B3 ран 3 (same opId) | downgrade7 | ad9430ce | COMPLETED (withdraw) — **B3 PASS** |
| 10 | B1: расшифровка поллов (все true) → найдена инверсия цикла → багфикс №2 (anyMatch), рестарт №4, terminate застрявших ранов 1f3e2c71/ad9430ce |
| 11 | B1 ран 3 (same opId) | modify3 | 1f3e2c71 | COMPLETED — **B1 PASS** (rescale ic 2→1) |
| 12 | B4 (новый opId, та же цель) | modify3 | f734d186 | COMPLETED no-op — **B4 PASS** |
| 13 | POST upscale ic:2 (prep) | modify4 | 328a6784 | COMPLETED |
| 14 | B10 (hc:0 при 3 брокерах) | downgrade7 | 5b0a1881 | PASS: REASSIGN_INVALID_TARGET_BROKERS 2<3 |
| 15 | fill test→6 id | modify4 | 33968717 | COMPLETED |
| 16 | B2 пачка (2.pc+2.ic) | modify4 | 62c713bf | COMPLETED — **B2 PASS** |
| 17 | POST upscale ic:2 (prep B5/B6) | modify3 | a293f9ab | COMPLETED; fill test→5 id (opId не залогирован) |
| 18 | B5/B6 ран 1 → terminate в ic-child (после unregister) | modify3 | f44bc768 | TERMINATED |
| 19 | B5 ран 2 (same opId) | modify3 | f44bc768 | COMPLETED — **B5 PASS** |
| 20 | POST upscale ×3 параллельно: modify3(B7-prep) / modify4(B8-prep pc:2) / downgrade7(rollback B3 ic:1) | 4bbe59ee, 763dbe8c, 6631a2d8 | все COMPLETED |
| 21 | B7 (битый vault super) | modify3 | c873b90b | COMPLETED — **B7 PASS** (restore → тем же раном) |
| 22 | fill test→6 id (B8 prep) | modify4 | (не залогирован) | COMPLETED |
| 23 | B8 ран 1 → terminate pc-child | modify4 | 91aaf192 | FAILED: PARTIAL_DOWNSCALE_FAILURE [pc] |
| 24 | B8 ран 2 (same opId) | modify4 | 91aaf192 | COMPLETED — **B8 PASS** |
| 25 | POST upscale pc:2,ic:2 (B14 prep) | modify4 | 6ceb8664 | COMPLETED; fill test→6 id COMPLETED |
| 26 | B14 ран 1 → terminate#1 (drain) | modify4 | c59e59f6 | TERMINATED |
| 27 | B14 ран 2 (same opId) → terminate#2 (дети pc/ic) | modify4 | c59e59f6 | TERMINATED |
| 28 | B14 ран 3 (same opId) | modify4 | c59e59f6 | COMPLETED — **B14 PASS** |
| 29 | POST upscale ic:2 (B6-proper prep) | modify3 | — | HTTP 500: processing убит для checkstyle → рестарт №5 → повтор 202 COMPLETED |
| 30 | B3b попытка 1 | downgrade7 | 1e5c208c | HTTP 000 (processing down, workflow не создан) |
| 31 | B15/B3b ран 1 → terminate mid-withdraw (после stopService) | downgrade7 | 5d1827fc | TERMINATED (частичное состояние) |
| 32 | B15 ран 2 (same opId) | downgrade7 | 5d1827fc | COMPLETED — **B15 PASS** |
| 33 | fill-ALL (test+3 CC → 5 id, 76 партиций) | modify3 | (не залогирован) | COMPLETED |
| 34 | B16 ран 1 → terminate при drain=false | modify3 | 8d6fc604 | TERMINATED |
| 35 | B16 ран 2 (same opId) | modify3 | 8d6fc604 | COMPLETED — **B16 PASS** (поллинг дождался переливки) |
| 36 | POST upscale ic:2 (B17 prep) | modify3 | (не залогирован) | COMPLETED |
| 37 | B17 ран 1 → terminate в reassign-child (alter не применён) | modify3 | b7ed5e01 | TERMINATED |
| 38 | B17 ран 2 (same opId) | modify3 | b7ed5e01 | COMPLETED — **B17 PASS** (ребёнок пересоздан, alter повторён) |

Рестарты processing: №1 clean bootRun (stale-рантайм) → №2 (фикс TOPIC_NOT_EXISTS) →
№3 (после правки mdb-data base-url) → №4 (фикс anyMatch + runIgnoringAlreadyStarted) →
№5 (после checkstyle `clean`). Между ранами одного opId workflow-история копится:
TERMINATED/FAILED раны остаются в UI, финальный ран COMPLETED.

## Финальное состояние (все 3 кластера = стартовое 4×1 + 3 контроллера + cruise)

- modify3/modify4/downgrade7: брокеры pc/kc/hc/ic по 1, :9092 живые (nc), host_state без хвостов,
  операций active/failed — 0. Хвосты 2.broker.* почищены (3 шт., шаг B13).
- Код: checkstyle PASS (clean + --rerun-tasks), kafka-тесты PASS (KafkaHostActivityImplTest мок
  обновлён на `listAllTopicNames` — семантика фикса #1).

## Изменённые файлы (незакоммичены, к MR MDBDEV-2900)

1. `KafkaHostActivityImpl.validateTopicsExist` — `getTopicNames()` → `listAllTopicNames()` (баг #1)
2. `DownscaleKafkaBrokerInClusterWorkflowImpl.waitBrokersDrained` — `noneMatch` → `anyMatch` (баг #2)
3. `DownscaleKafkaBrokerInClusterWorkflowImpl.startPartitionReassignment` — обёртка
   `ChildWorkflowUtils.runIgnoringAlreadyStarted` вокруг старта reassign-child (баг #3)
4. `KafkaHostActivityImplTest` — мок под новую семантику
5. `application-local.yaml` (некоммитимое) — mdb-data base-url → :8081

## Грабли сессии

- Fill-reassign: `topics:["test"]` двигает ТОЛЬКО юзерский топик — CC-топики остаются на старом
  составе (окно drain схлопывается). Полный дренаж в B1 двигал 76 партиций ~2-3 мин.
- Конверге-save upscale возвращает «хвост» host_state (2.broker.*) — DELETE хвоста повторять
  перед каждым rollback-upscale.
- Отдельной очереди `kafka-activities-worker` НЕ существует — workflow+activity таски идут через
  `kafka-activities-queue` (пустые pollers по имени `-worker` — не поломка).
- Terminate ребёнка не трогает siblings — ic-child дошёл конца → parent собрал failedDcs →
  PARTIAL_DOWNSCALE_FAILURE (ровно контракт B8).
- nodeId жертвы: `KafkaNodeIdCalculator` (base 20000 + dcId*1000 + instanceId, dcId по PMS
  `kafka.layout`): modify3 ic=23001/23002, modify4 pc=22001/22002, ic=25001/25002.

## Дополнение (вечер 08.09): закрытие пробелов покрытия обрывов

Матрица «точка обрыва → рестарт» сверена со всеми фазами флоу. Три непокрытых места —
новые сценарии B15/B16/B17 (описание в SKILL.md), все три прогнаны PASS:

| # | Точка обрыва | Кластер | wfId | Ловля окна | Результат ретрая |
|---|---|---|---|---|---|
| B15 | ic-child: stopService выполнен, withdrawService/storage нет | downgrade7 | `5d1827fc` | поллинг истории ic-child до появления `cloud_stopService` (t+10с) → terminate родителя | ребёнок повторил isServiceExists→stopService (идемпотентно на остановленном) → withdrawService → storage → COMPLETED |
| B16 | parent в drain-поллинге при drain=false (fill-ALL 76 партиций прямо перед запуском) | modify3 | `8d6fc604` | поллинг истории родителя до `isBrokerDrained` + последний результат poll=false → terminate | ретрай: reassign-child скипнут (COMPLETED, runIgnoringAlreadyStarted), поллинг возобновился, дождался фоновой переливки → unregister → дети → COMPLETED |
| B17 | reassign-child исполняется, alter НЕ применён (list done, describe в полёте) | modify3 | `b7ed5e01` | поллинг списка workflow каждые 3с до `_reassign-partitions=RUNNING` → terminate родителя (ребёнок убит PARENT_CLOSE_POLICY) | ребёнок TERMINATED реюзабелен (ALLOW_DUPLICATE_FAILED_ONLY) → пересоздан → полный план list+describe+alter повторно → COMPLETED |

Итог карты покрытия: reconcile ✓ / discovery ✓ / reassign-child до alter ✓(B17) /
drain false ✓(B16) / drain true ✓(B14#1) / unregister ✓(B5) / rescale-дети ✓(B8,B14#2) /
withdraw-частичное ✓(B15). Полное покрытие обрывов + рестартов; B11 (drain timeout) остаётся
единственным непрогнанным (нет локального рычага throttle, отложен на стенд).

Финал: все 3 кластера в стартовом состоянии 4×1, host_state чист, операций active/failed нет.
⚠️ Механическая заметка для повторения: после `clean`/checkstyle воркер надо поднимать заново —
bootRun-процесс был убит и первый запуск B-сценариев получил 500/000 (processing down).
