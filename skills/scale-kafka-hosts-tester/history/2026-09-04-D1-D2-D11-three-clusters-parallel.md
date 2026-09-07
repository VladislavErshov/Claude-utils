# 2026-09-04 — D-сценарии на трёх кластерах параллельно: D2 modify3 + D11-подтверждение, D1+миграция лидера modify4, D2-withdraw downgrade7

Ветка processing `ershov/MDBDEV-3180-Kafka-downscale-controllers-per-DC-with-idempotent-withdraw-restart`
(da26d471, включает фикс MDBSUP-4939). Первый полноценный прогон D-сценариев нового
cluster/dc-флоу — все три dev-кластера, по сценарию на кластер параллельно.

## Инфраструктурные фиксы, сделанные в этой сессии (до сценариев)

1. **mdb-processing перезапущен** — до рестарта процесс (15:01) работал на классах без
   4939-фикса (классы пересобраны в 16:51). После рестарта: кворум читается ДО чистки и
   передаётся параметром в `reloadBrokersAndControllers` ✓.
2. **mdb-data переключён на ветку** `ershov/MDBDEV-3180-Kafka]-rework-controller-scale-workflows-to-cluster-dc-scheme-with-QueueInfo`
   (был master → старый DTO → processing отвечал 400 «queueInfo null, controllersPerDc empty»,
   mdb-data оборачивал в 502). Адаптация downscale под новый контракт (по образцу upscale ff615663):
   - `processing-api:3.52.0` → `processing-api:ershov-MDBDEV-3180-Kafka-downscale-controllers-per-DC-with-idempotent-withdraw-restart-SNAPSHOT`
     (api-модуль ветки processing опубликован в mavenLocal: `./gradlew :api:publishToMavenLocal`;
      в опубликованных 3.53.0–3.57.0 нового DTO ещё НЕТ — только branch-snapshot);
   - `KafkaHostMapper.toDto` downscale: новая сигнатура `(operationId, controllersPerDc, queueInfo, metaParams, brokerHosts, vaultPathToSecret)`,
     `isWan` из `metaParams.getIsWan()`;
   - `KafkaHostsServiceImpl.downscaleKafkaController`: absolute `controllersPerDc` из host_state
     + `computeIfPresent(targetDc, -1)` (= старая семантика replicas=count-1).
   - ⚠️ Правки mdb-data НЕ закоммичены. Грабли stash-pop: ветка удалила `rtconfig/local.hjson`
     (использует `local.json`) — конфликт DU решён `git rm`.
3. **application-local.yaml processing**: `external.api.namespaces.infra.mdb-data.base-url`
   `8088 (wiremock)` → `8081 (реальный mdb-data)`. Иначе save-фаза падает: wiremock не имеет
   маппингов `/internal/api/v2/.../save/*-controllers` → 404 «Request was not matched»
   (T15 подтверждает: ранее save ходил на 8081; правка была локальной и потерялась).
   ⚠️ Не коммитить (как и truststore-блок).
4. mdb-processing перезапущен после п.3. Грабли рестарта: `kill` по PID из `lsof -i :8080 -P -t`
   при пустом выводе убивает НЕ ТОТ процесс (переиспользованный PID) — bootRun-JVM пережил
   kill wrapper'а; убивать конкретный PID JVM (`ps -o pid,lstart -p $(lsof -i :PORT -P -t | head -1)`).

## modify3 — D2 {ic:0}: happy path + D11-десинк

Контекст: 4 контроллера dc/hc/ic/kc (PMS=облако=БД), живой кворум при этом 3 вотера
(13001@ic — observer со старым конфигом), лидер 11001@hc. Призрак 2.controller.dc в облаке
(битый инстанс квота-фейла) — в БД его не было.

**Run 1** (op `9d14e5c3`, input controllersPerDc {dc:1, hc:1, kc:1, ic:0}, TTL 3ч):
- discovery из облака поймал И призрака 2.controller.dc → removedHosts = [1.controller.ic,
  2.controller.dc] — флоу самочистит фантомы, которых нет в БД (discovery-based) ✓;
- removeControllerFromQuorum ×2 (13001@ic реальная, 2.controller.dc no-op) ✓;
- getLeaderId: лидер 11001@hc вне удаляемых → миграция не нужна ✓;
- update-broker-config (брокеры hc/kc/pc) ✓, reload оставшихся dc/hc/kc ✓;
- children: ic=stop+withdraw, dc=rescale (фантом снят), hc/kc=skip — все COMPLETED ✓;
- **save FAILED**: 404 от wiremock (см. фикс 3) — инфра-баг роутинга, НЕ кода флоу.
  Итог облака: ic и фантом удалены, PMS 3 вотера — цель достигнута, save не доехал.

**Run 2 — ретрай = живое подтверждение D11** (op `298aaa4f`, COMPLETED, операция done):
- discovery → removedHosts = **пусто** → removeControllerFromQuorum нет;
- НО: getLeaderId + reload брокеров (update-broker-config) + reload всех контроллеров
  **выполняются заново** (лишний churn — идемпотентно, но не no-op);
- children все skip; `saveDownscaledKafkaControllersInfo(clusterId, [])` — save с пустым
  списком **не удаляет ic из host_state** → операция «успешна», десинк БД↔облако остаётся.
  Из БД цель недостижима никогда (icc остаётся → повторный DELETE dc=ic →
  host_state ic:1-1=0... на самом деле повтор даст ту же пустую removedHosts).
- Ранний выход в текущем коде отсутствует вообще — всегда полный прогон с churn.

**Ручная починка (D11 step 5)**: DELETE из host_state `1.controller.ic` (+ призрак
`1.broker.dc`, удалённого пользователем из облака руками). Финал modify3: 3 контроллера
dc/hc/kc, KRaft voters {10001,11001,12001}, лидер 11001@hc, lag=0, PMS=БД=облако ✓.

Критерий будущего фикса (не реализован): ранний выход/пустой removedHosts обязан
сохранять факт удаления — save по хвостам host_state, идемпотентный save в child после
withdraw, или expected-removed в request.

## modify4 — converge {kc:2} → D1 {kc:2→1} с миграцией лидера

Исходно: облако/PMS 4 контроллера (2.controller.kc жив в кворуме после вчерашнего
07a21cb7), host_state 3 → **внимание: даунскейл при таком десинке опасен** — mdb-data
строит цель из host_state (kc:1-1=0) и флоу снял бы ОБОИХ kc-контроллеров → потеря кворума.
Сначала converge-ретрай (T14-паттерн): POST ?dc=kc → child kc skip (2=2), save дописал
2.controller.kc → БД=облаку. (409 от failed `07a21cb7` — закрыт UPDATE status='done'.)

**PREFAIL-диагностика (до converge)**: kc/rc брокеры PREFAIL «Has 33/17 partitions with
min in-sync replicas» — НЕ plait-шум. kc-брокер был в crash-loop (4 ротации лога за час,
`Kafka Server started` = 0, fetchMetadata/register-таймауты до контроллеров; TCP 9093 при
этом OK). Кворум контроллеров здоров (4 вотера, лидер 10001@hc, lag=0). kc **сам выбрался**
в 23:18 после стабилизации кворума: inter-broker acceptor поднялся, replica-fetchers
догнали лидеров 22001/23001, catchup LEAVE через 30s in-sync. После этого availability
всех трёх брокеров = RESERVED (rank 1.015), PREFAIL снялся. rc-шум был collateral от kc.
Вывод: PREFAIL «at min ISR» лечится восстановлением ISR (рестартом застрявшего брокера),
ручной чистки не потребовалось.

**D1** (op `d69b1f3e`, DELETE ?dc=kc, target kc:1): getLeaderId → **лидер стоял на
удаляемом 2.controller.kc** (переизбрался туда после converge-рестартов) → отработала ветка
миграции (D9-семантика): restart самого лидера-удалённика → waitLeaderMigrated
(getLeaderId ×2) → reload оставшихся pc/hc (+restartAndRestore) → child kc rescale →
save → COMPLETED. Финал: 3 контроллера, PMS=KRaft={10001,11001,12001}, лидер 10001@hc, lag=0,
host_state=облаку ✓.

## downgrade7 — converge {hc:2} → D2 {pc:1→0} withdraw-путь

Аналогичный десинк (2.controller.hc в облаке/PMS, нет в БД) → converge-ретрай
(op `46449759`, ~4 мин, child hc skip, save дописал) → затем **D2** (op `9f75061d`,
DELETE ?dc=pc, target pc:0):
- removeControllerFromQuorum(12001@pc) 4→3; лидер 11001@kc вне удаляемых → без миграции;
- child pc: stopService + withdrawService + withdrawStorage — **1.controller.pc удалён из
  облака полностью** (инстанс+сервис+сторадж), pc-БРОКЕР остался жив (RUNNING) ✓;
- save удалил pc-контроллер из host_state ✓. Финал: voters {10001,10002,11001},
  лидер 11001@kc, lag=0 ✓.

## Грабли сессии

- `kafka-metadata-quorum.sh` на контроллере: только :9093 и UnsupportedVersionException —
  запускать через БРОКЕРА (:9092). `--command-config`: рендеренный клиентский конфиг с
  паролем собрать на хосте из vault (`zkv/mdb/mdbdev/kafka/<fullQueue>/super`, vaultRoot из
  PMS `zen.kafka.vaultRoot`); bootstrap по FQDN (localhost ломает SSL hostname verification);
  admin.properties класть в /tmp хоста.
- `ss -tlnp` на хосте НЕ видит порты контейнера (Porto) — «9092 не слушается» был false
  negative; живость брокера проверять AdminClient-запросом.
- mdb-data 409: блокирует ЛЮБАЯ свежая failed-операция (не только последняя) — чистить
  `UPDATE operations SET status='done', finished_ts=now(), error_message=NULL, in_processing=false`.
- mcc instances лагает (пусто на живой инстанс) — перепроверять повторным вызовом.
- Публикация api: `./gradlew :api:publishToMavenLocal` в processing (git-versioning даёт
  `ershov-<branch>-SNAPSHOT`); в корпоративных релизах нового DTO нет.

## Статус D-сценариев после сессии

| # | Статус | Примечание |
|---|---|---|
| D1 rescale-путь (kc 2→1) | PASS 04.09 | modify4 `d69b1f3e`, вкл. ветку миграции лидера |
| D2 target=0 withdraw (pc→0) | PASS 04.09 | downgrade7 `9f75061d`, сервис+сторадж, брокер жив |
| D2 target=0 (modify3 ic→0) | PASS* | run 1 `9d14e5c3` — всё кроме save (wiremock-роутинг) |
| D11 прерывание до save | ПОДТВЁРЖДЁН | ретрай `298aaa4f`: «успех» без save → вечный десинк; починка руками |
| D5 рестарт посреди applyNewQuorum | не гонялся | |
| D6 AdminClient недоступен | не гонялся | естественное состояние наблюдалось на kc (fetchMetadata-таймауты) |
| D7 partial failure | не гонялся | |
| D8 шаг >1 (INVALID_REPLICAS_COUNT) | не гонялся | mdb-data API шлёт только шаг −1 — контрактный тест потребует прямого запуска |
| D9 лидер среди удаляемых | ПОКРЫТ D1 modify4 | лидер 11002@2.controller.kc → миграция ok |
| D10 серия ретраев | частично | converge-ретраи обоих кластеров сошлись; 3+ подряд не гонялось |

---

# Партия 2 (04.09 ночь) — D5, D10, D8, D6

Предусловия: все три кластера апскейлом до 4 контроллеров (kc:2 у каждого, ops
`01aaf391`/`7655e8de`/`c51a6570`, все COMPLETED ~00:26, sync сам закрыл операции).

## Результаты

| # | Кластер | Сценарий | Результат |
|---|---|---|---|
| D5 | modify4 | terminate после removeControllerFromQuorum (`be6c5705`), ретрай тем же workflowId | PASS по флоу: чистка кворума идемпотентна (2-й прогон — no-op), reload+дети+… всё прошло; **save упал** — mdb-data умер (exit 143) между child и save → десинк host_state, закрыт ручным DELETE 2.controller.kc |
| D10 | downgrade7 | тот же паттерн (`94bfc3ce`): terminate 1 → ретрай тем же id | Аналогично D5: флоу идемпотентен, состояние не деградировало, save-фейл из-за mdb-data |
| D8 | modify3 | direct-start цель ВЫШЕ текущей {kc:3} при kc:2 (`d8-direct-guard-9fc47c1b`) | PASS: child kc FAILED — `DOWNSCALE_NOT_ALLOWED` nonRetryable («Current: 2, target: 3»), parent — `PARTIAL_DOWNSCALE_FAILURE` [kc], дети dc/hc skip. Side-effects: только reload-churn (update-broker-config + reload контроллеров ПРОШЛИ до guard'а — guard живёт в child, валидация не ранняя) |
| D6 | modify3 | битый vault-секрет super → DELETE dc=kc (`fbab747a`) | PASS: getLeaderId исчерпал ретраи → `KafkaAdminClientException: Failed connection check to :9092` — точный прод-паттерн `1a776d19`; ~9 мин ретраев → FAILED. Восстановление секрета → ретрай тем же id → COMPLETED, save доведён (kc:2→1 в БД) |

## Ключевые находки партии 2

1. **Direct-start рецепт (важно!)**:
   - task queue = **`kafka-activities-queue`** (НЕ `kafka-activities-worker` — это имя воркера
     из `@WorkflowImpl(workers=...)`, workflow на ней висит вечно без поллеров);
   - payload encoding = **`application/json`** (base64 `anNvbi9wbGFpbg==`), НЕ `json` — кастомный
     конвертер проекта не знает голый json → `DataConverterException: No PayloadConverter is
     registered for an encoding: json` → WorkflowTaskFailed;
   - POST `http://localhost:8233/api/v1/namespaces/default/workflows/{id}` c csrf-cookie+header.
2. **D11-нюанс (уточнение процедуры починки)**: ретрай после прерывания спасает host_state
   ТОЛЬКО если к моменту его discovery облако ещё содержит удаляемый хост (removed непустой).
   Если дети уже успели удалить инстанс, а save упал — ретрай даёт removed=[] и хвост остаётся
   в БД навсегда → только ручной DELETE из host_state.
3. **Воркер-поллеры умирают после query-replay nondeterminism на terminated-ране**
   (`InternalWorkflowTaskException ... EVENT_TYPE_WORKFLOW_EXECUTION_TERMINATED during replay`
   — прилетает, когда mdb-data sync дергает query по мёртвому рану) → очередь без поллеров,
   все workflow стоят. Лечение: рестарт mdb-processing. Наблюдать:
   `GET :8233/api/v1/namespaces/default/task-queues/kafka-activities-queue?taskQueueType=1` → pollers=[].
4. **mdb-data gradle-bootRun умирает молча** (~20–70 мин: exit 143 дважды). Митигация:
   `nohup ./gradlew bootRun … > log 2>&1 < /dev/null & disown` — после этого жил дольше всех.
5. Окно симуляции D5: terminate родителя ПОСЛЕ `removeControllerFromQuorum` COMPLETED и ДО
   старта `update-broker-config` — окно широкое (секунды), поллинг истории ловится легко.
6. Sync mdb-data закрывает операцию по завершении workflow (upscale-партӣ: все три закрылись
   сами в течение минуты), но failed-операция после успешного ретрая тем же id остаётся failed
   — закрывать руками.

## Финальное состояние кластеров (все = 3 контроллера, PMS=KRaft=БД=облако)

- modify3: dc/hc/kc, лидер 11001@hc, lag=0
- modify4: hc/kc/pc, лидер 10001@hc, lag=0
- downgrade7: hc:2/kc, лидер 11001@kc, lag=0

## Остаток по D-матрице

- D3 (перезапуск после успеха) — покрыт `298aaa4f` (партия 1) как частный случай.
- D7 (partial failure в 2 ДЦ ОДНОВРЕМЕННО через mdb-data API) — API шлёт работу только по
  одному ДЦ; multi-DC возможен лишь с фантомами облака (см. партию 1, modify3 run 1) —
  либо прямой запуск с составной целью (негатив/фолт-инжекция).

---

# Партия 3 (04.09 день) — SAVE-PER-STATE: переделка контракта и полная валидация D5/D10/D11

## Переделка save (remaining-контракт)

Контракт: processing шлёт **список остающихся** контроллеров (бывш. removed), mdb-data берёт
контроллеры из host_state и **по диффу удаляет отсутствующих в списке**; при удалении последнего
контроллера ДЦ — ДЦ уходит из controllerDcs/units db_cluster_version.

- mdb-data: `KafkaHostsInternalServiceImpl.saveDownscaledKafkaControllers` — дифф + `isNeedRemoveDc`
  + `removeControllerDcFromClusterVersion`; facade/param `remainingControllers`; API-сигнатура НЕ
  менялась (Set<String>, data-api 1.95.0) — semantic-only change.
- processing: активность `saveDownscaledKafkaControllersInfo(clusterId, Collection<String>
  remainingControllers)` (имя Temporal-активности сохранено!), воркфлоу шлёт `remainingHosts`
  (deteministic buildHosts 1..N ∩ discovery) — НЕ зависит от diff'а облака.
- Тесты: workflow-тесты — remaining-ассерты, DC-фильтрованные моки облака (getServiceInfo:
  dc = args[0]!, getInfosForServices: фильтр по `.{dc}.one-infra.ru`), quorumFrom — позиционные
  nodeId, requestWithRemoteDc (полный состав ДЦ как в проде). mdb-data: 13 keep-list тестов +
  интеграционные remaining-тела.
- Коммиты: processing `a1f6eaf5` (rebase на master), mdb-data `be484944` (rebase на master;
  version-bump processing-api НЕ в коммите — юзер пушит релиз сам). MR processing !440, mdb-data !481.
  ⚠️ После мерджа processing → выйдет processing-api с новым Downscale DTO → mdb-data build.gradle
  поставить релизную версию.

## Результаты партии 3 (все PASS)

| Сценарий | Кластер/Op | Ход | Итог |
|---|---|---|---|
| D5 | modify4 `55f31a35` | terminate после removeControllerFromQuorum → ретрай тем же id | COMPLETED, БД сошлась (kc:1) — ретрай доводит до конца ВКЛЮЧАЯ save |
| D10 | downgrade7 `c523d886` | terminate r1 → ретрай → terminate r2 → ретрай | COMPLETED, БД сошлась (kc:1), план детерминирован |
| **D11** | modify3 `c28ef4e0` | **детерминированно**: DELETE → kill mdb-data в окне save → дети успели удалить 2.controller.kc → save исчерпал ретраи → FAILED; ловушка зафиксирована (cloud чист, host_state kc=2). Поднятие mdb-data → ретрай тем же id → **COMPLETED за ~30с, remaining-дифф убрал хвост (kc:1)** | ✅ Главный фикс подтверждён: раньше хвост был вечным |

Финал: все три кластера по 3 контроллера, PMS=KRaft=host_state=облако, lag=0.

## Инфра-грабли партии 3

1. **Stale-классы gradle после ребейза**: bootRun поднимал СТАРЫЕ классы (инкрементальная
   компиляция ломается на git-перезаписи исходников) → «Failed to parse Kafka cluster params»
   при заведомо-парсящемся JSON (проверено scratch-тестом). Лечение: `clean compileJava` +
   рестарт; диагностический приём — временный System.err-дамп в catch.
2. **Версия processing-api теряется при смене веток** (незакоммиченный bump) → mdb-data шлёт
   старый DTO → processing 400 «queueInfo null». Держать локальный sed-снапшот:
   `ershov-MDBDEV-3180-Kafka-downscale-controllers-per-DC-with-idempotent-withdraw-restart-SNAPSHOT`
   (публиковать `:api:publishToMavenLocal` из processing ПОСЛЕ его ребейза — snapshot старой
   базы не собирается с master-кодом newsql).
3. Окно terminate D11 «после детей до save» — неуловимо поллингом (<1с, workflow успевает
   завершиться). Детерминированный способ: kill mdb-data в окне save (ретраи ~3 мин → FAILED),
   затем ретрай тем же id. ⚠️ Не применять при параллельных сценариях — save других кластеров
   тоже ляжет.
4. Зоопарк bootRun-процессов после нескольких nohup-рестартов: один держит 8080, другой «UP»,
   поллеры исчезают (stale-записи по старым PID). Чистить ВСЕ (ps + kill), затем один bootRun.
5. mdb-data bootRun продолжал умирать молча (~30-60 мин) даже с `</dev/null & disown` — при
   длинных сериях проверять health перед каждым запуском операции.

## Восстановление кластеров к прод-состоянию (финал сессии)

1. Дамп прода (туннель 53480 жив) → `/tmp/prod-snapshot-3clusters.json`: прод-таблица =
   modify3 dc/hc/kc, modify4 hc/kc/pc, downgrade7 hc/kc/pc (по 3 контроллера).
2. downgrade7 расхождение с продом (после тестов облако было hc:2/kc:1/pc:0) закрыто двумя
   операциями через mdb-data: upscale pc (deploy 1.controller.pc, op `c08d10b9`) →
   downscale hc (снятие 2.controller.hc, op `b29020c1`) — обе COMPLETED.
3. Ресид локальной БД из дампа: DELETE operations/host_state/one_cloud_meta/db_cluster_version
   по трём cluster_id + INSERT (db_cluster НЕ трогать — FK от users; one_cloud_meta без fake_id;
   namespaces/projects/hardware_presets ON CONFLICT DO NOTHING). `/tmp/reseed-3clusters.sql`.
4. Итог: БД=прод=облако=PMS (по 3 вотера), KRaht жив (lag=0), последние операции done (нет 409).

---

# СТОП-ТОЧКА (04.09 вечер) — ревью MR !440: что запушено, что в рабочей копии, что сломано

## Контекст

Ревью Gena выдал 8 замечаний (+3 от ai-ревьюера) на processing MR !440. Разбор и статусы:

| # | Замечание | Статус |
|---|---|---|
| #4 DTO-валидация | ✅ запушено `32a4a6e9`, ответ в MR отправлен |
| #5 stale-таймаут | ✅ запушено `32a4a6e9`, ответ отправлен |
| #8 semantic-change save | ✅ тест активности запушен, ответ отправлен (дизайн remaining отстояли) |
| #9 capture чистки кворума | ✅ запушено, ответ отправлен |
| #10 partial-failure тест | ✅ запушено (`partialFailure_oneDcFails_throwsNonRetryableAndSkipsSave`), ответ отправлен |
| #7 миграция лидера на ретрае | 🔧 В РАБОЧЕЙ КОПИИ, НЕ ЗАПУШЕНО, тест красный (см. ниже) |
| #11 docs breaking-change | 🔧 В РАБОЧЕЙ КОПИИ (downscale-controller.md), НЕ ЗАПУШЕНО |
| #6 replay in-flight старых execution | 📄 закрывается докой из #11 (деплой без активных downscale-операций); ответ НЕ отправлен |

## Что в рабочей копии mdb-processing (незакоммичено)

1. **#7 унифицированная миграция лидера** (`DownscaleKafkaControllerInClusterWorkflowImpl`):
   - `migrateLeaderIfNeeded(namespace, nodeIdToHost, params, deadlineMs, removedHosts, remainingHosts)`
     — единый путь: лидер на удаляемом хосте ИЛИ не разрешается в карте (кворум уже вычищен
     прошлым раном) → `restartAliveRemovedHosts` (все живые RUNNING удаляемые) →
     `waitLeaderMigrated` с картой, ОТФИЛЬТРОВАННОЙ по remainingHosts, и пустым oldLeader
     (= «лидер среди оставшихся»). Старая ветка «рестарт только лидера» удалена по решению юзера.
   - `restartAliveRemovedHosts` — getInfoForInstances + фильтр CloudState.RUNNING +
     `restartAndWaitControllerInstancesSsh` (с per-instance таймаутом из #5).
   - Реконструкция nodeId по kafka.layout УБРАТА (юзер: в layout бывают давно удалённые ДЦ —
     формула ненадёжна). Метод `KafkaPmsActivity.getKafkaLayout` (+Impl, +stub в
     ModifyKafkaClusterWorkflowImplTest) ОСТАВЛЕН в запушенном коде как полезный API.
2. **Тест** `retryAfterQuorumCleanup_leaderOnRemovedHost_restartsRemovedAndConverges`:
   фикстура — discovery видит [C1(hc),C2(hc),REMOTE(pc)], PMS-кворум уже без C2
   (`quorumControllers`-ручка добавлена в тест), лидер id=2 (unmapped) → миграция → сходится.
   **ТЕСТ КРАСНЫЙ**: hc-child падает → PARTIAL_DOWNSCALE_FAILURE. Причина НЕ найдена:
   в логах message обрезан (workflow metadata без cause). happyPath с rescale — зелёный.
   Гипотезы: поведение mock'а rescaleService/getServiceInfo при 2 ДЦ + изменённом
   currentControllers (mock мутирует список, сортировка lexicographic, limit(size-1) по всему
   списку). Дебажить: добавить печать cause в тест/логгер, или смотреть history child-workflow
   в testEnv.
3. **#11 дока**: секция «Совместимость и порядок релиза» в `docs/kafka/downscale-controller.md`
   (breaking REST, инверсия save-контракта, порядок деплоя mdb-data → processing, replay-оговорка).
4. Локальные некоммитимые: `application-local.yaml` (truststore + mdb-data→8081).

## Состояние кластеров (восстановлено, можно не трогать)

Все три = прод-таблица (по 3 контроллера), БД перезалита из прода (`/tmp/reseed-3clusters.sql`,
дамп `/tmp/prod-snapshot-3clusters.json`), downgrade7 облако докручено ops `c08d10b9` (upscale pc)
+ `b29020c1` (downscale hc). PMS=KRaft=БД=облако, lag=0.

## Как продолжать

1. Починить красный тест из п.2 → прогнать downscale-тесты + checkstyle (`clean checkstyleMain
   checkstyleTest checkstyleIntegrationTest --rerun-tasks`).
2. Закоммитить #7+#11 (БЕЗ application-local.yaml), запушить.
3. Ответить в MR на #7, #11, #6.
4. После мерджа processing: юзер публикует processing-api, mdb-data build.gradle — релизная
   версия (сейчас локально стоит snapshot в mdb-data).
