---
name: scale-kafka-hosts-tester
description: Локальное тестирование scale-операций с Kafka-хостами в mdb (upscale/downscale контроллеров и брокеров) — настройка инфраструктуры, seed dev-кластера из прода, симуляция падений по фазам workflow, проверка идемпотентности и контрактов mdb-data ↔ mdb-processing. Секции: upscale Kafka-контроллеров (MDBDEV-3180), downscale Kafka-контроллеров (сценарии D1–D11 по прод-паттернам из Temporal) и downscale Kafka-брокеров (MDBDEV-2900: reassign + drain + unregister + withdraw, сценарии B1–B14). Используй когда нужно локально прогнать scale-флоу Kafka и проверить сходимость после падений.
allowed-tools: [bash, read_file, edit_file, write_file, grep, glob]
---

# Скилл тестирования scale-операций Kafka-хостов

Общие разделы (инфраструктура, seed, прод-Temporal, верификация) — ниже.
Специфика операций — в секциях.

# Секция: Upscale Kafka-контроллеров (MDBDEV-3180)

## Ограничение масштаба тестов (обязательно)

Стандартный продовый сценарий — поднятие **1–2 контроллеров за раз**, итого не больше,
чем текущий размер кворума. В тестах НЕ задираем цель: для кластера с 3 контроллерами
(кворум 3) допустимые цели — `{dc:2}` (+1 контроллер) максимум `{dc:2,hc:2}` (+2).
Запуск `{dc:2,hc:2,kc:2}` (+3, удвоение кворума) НЕ гонять — не соответствует
реальному использованию и может уронить кластер (flexible quorum не настроен).
Тот же лимит для T8: ретрай с изменённой целью — шаг тоже +1.

## Что тестируем

- **mdb-processing** (этот репо): `UpscaleKafkaControllerInClusterWorkflowImpl` (parent) +
  `UpscaleKafkaControllerInDcWorkflowImpl` (child по ДЦ). Архитектура — `docs/kafka/upscale-controller.md` (ЧИТАТЬ ОБЯЗАТЕЛЬНО).
- **mdb-data**: адаптация к контракту `QueueInfo`/`controllersPerDc` (branch `ershov/MDBDEV-3180-Kafka]-rework-controller-scale-workflows-to-cluster-dc-scheme-with-QueueInfo`).
- Ключевые свойства, которые надо проверять: идемпотентность повторного запуска, union-merge кворума в PMS, скип при достигнутой цели, `PARTIAL_UPSCALE_FAILURE` / `DOWNSCALE_NOT_ALLOWED` как non-retryable.

Фазы parent-workflow (места для симуляции падений):
1. `discoverKafkaHosts` — discovery контроллеров;
2. `upsertPms` (kafka.layout + kafka.controller.quorum) — ДО деплоя;
3. `deployControllers` — параллельные child по ДЦ (`Async`, `ignoreAlreadyStarted`);
4. `reloadBrokersAndControllers` — reload с `parameters = null`;
5. `saveUpscaledControllers` — сохранение полного списка в mdb-data.

## Инфраструктура (порты) — общая для всех операций

| Сервис | Порт | Запуск |
|---|---|---|
| mdb-data | 8081 | `bootRun --args='--spring.profiles.active=local --server.port=8081'` (лог `/tmp/mdb-data.log`) |
| mdb-processing | 8080 | `bootRun --args='--spring.profiles.active=local'` (лог `/tmp/mdb-processing.log`) |
| temporal (local) | 8233 | docker-compose в `mdb-processing/localrun/` (`/setup-local-temporal`) |
| postgres (mdb-data) | 6434 | контейнер `pg_backstage_plugin_mdb`, БД `backstage_plugin_mdb`, юзер `dev` |
| wiremock | 8088 | docker-compose mdb-processing |
| **ABC-стаб квот** | 3000 | mdb-data local ходит за квотами в `http://localhost:3000/v1/quotas/one-cloud/product/{id}` (AbcClient). Без стаба — 500 «Connection refused»; стаб `/tmp/seed-2900/abc-stub.py` (python http.server): ответ — список по ДЦ (dc/hc/kc/pc/rc/ic/uc) с resourceType **VCPU/RAM/NVME/SSD/HDD/LAN_IN/LAN_OUT**, большими `quota` и `demand: 0`; `dc: null` в записях даёт NPE `DcProductQuota.dc() is null`; без NVME/LAN_* — «Quota is insufficient» 400 |

Auth в mdb-data local-профиле отключён — curl без токена.
⚠️ local-профиль mdb-processing пишет в РЕАЛЬНЫЙ `pms.cloud.vk.team` (bean `pmsRestClient`, `PmsAutoConfiguration.java:36`). Только dev-кластеры (project 160, mdbdev). Снапшот PMS до/после обязателен.

## Seed-кластер: test-modify3 — общая для всех операций

- cluster_id: `9fc47c1b-011d-4aaa-b411-de5345a0204e`, project 160, kafka, namespace INFRA.
- Хосты: brokers dc/hc/kc, controllers dc/hc/kc (по 1), cruise в ic. Цель upscale: `controllersPerDc {dc:2, hc:2, kc:2}`.
- `one_cloud_meta`: `cruise-control-service`, `db-service`, `kafka-controller-service` — все нужны.
- В draft-версиях `cluster_params` обязательно проставить пустые
  `kafkaParams.brokerConfig.config={}` и `kafkaParams.controller.controllerConfig.config={}`
  (иначе NPE в `KafkaClusterDiffDetector`):

```sql
UPDATE db_cluster_version
SET cluster_params = jsonb_set(jsonb_set(cluster_params, '{kafkaParams,brokerConfig,config}', '{}'), '{kafkaParams,controller,controllerConfig,config}', '{}')
WHERE cluster_id='9fc47c1b-011d-4aaa-b411-de5345a0204e' AND status='draft';
```

### Выгрузка из прода (read-only туннель localhost:53480)

**Перед серией тестов обязательна синхронизация трёх источников истины** (иначе тесты
упадут на 409/guard из-за рассинхрона, как в T8):
1. **Локальная БД ← прод**: выгрузить seed (JSON ниже) и вставить — `host_state`,
   `db_cluster_version`, `one_cloud_meta`, `operations` должны совпадать с продом.
2. **Облако (one-cloud) ↔ host_state**: после прямых temporal-запусков или упавших операций
   облако может опережать локальную БД (save не выполнился). Сверять фактические инстансы
   controller-сервиса (`cloud_getServiceInfo` в history или UI one-cloud) с `host_state`;
   при расхождении — вылечить «реconcile»-запуском через mdb-data с целью=факт, либо
   перезалить seed заново.
 3. **PMS ↔ host_state**: `kafka.controller.quorum` (через `pms-read.sh` из скилла
    [`pms-worker`](../pms-worker/SKILL.md), broker-ключ) должен
    содержать ровно хосты из `host_state` (+ их nodeId по `KafkaNodeIdCalculator`), без лишних
    и пропущенных voter'ов. `kafka.layout` — все ДЦ кластера.
4. **operations**: последняя операция кластера должна быть закрыта (не active/failed), иначе
   mdb-data вернёт 409 «Already has active or failed operation».

Готовый JSON с продовым снапшотом: `/tmp/test-modify3.json` (если жив). Обновить:

```bash
docker exec -e PGPASSWORD='HDX!cpw5yxf0ypd5tgd' pg_backstage_plugin_mdb \
  psql -h host.docker.internal -p 53480 -U backstage -d backstage_plugin_mdb -At -c "
SELECT jsonb_build_object(
  'db_cluster', (SELECT json_agg(t) FROM (SELECT * FROM db_cluster WHERE id='9fc47c1b-011d-4aaa-b411-de5345a0204e') t),
  'db_cluster_version', (SELECT json_agg(t ORDER BY create_ts DESC) FROM (SELECT * FROM db_cluster_version WHERE cluster_id='9fc47c1b-011d-4aaa-b411-de5345a0204e' ORDER BY create_ts DESC LIMIT 5) t),
  'host_state', (SELECT json_agg(t) FROM (SELECT * FROM host_state WHERE cluster_id='9fc47c1b-011d-4aaa-b411-de5345a0204e') t),
  'one_cloud_meta', (SELECT jsonb_agg(to_jsonb(t) ORDER BY t.params_type) FROM (SELECT * FROM one_cloud_meta WHERE cluster_id='9fc47c1b-011d-4aaa-b411-de5345a0204e') t),
  'operations', (SELECT json_agg(t ORDER BY created_ts DESC) FROM (SELECT * FROM operations WHERE cluster_id='9fc47c1b-011d-4aaa-b411-de5345a0204e' ORDER BY created_ts DESC LIMIT 10) t),
  'projects', (SELECT json_agg(t) FROM (SELECT p.* FROM projects p JOIN db_cluster c ON c.project_id=p.id WHERE c.id='9fc47c1b-011d-4aaa-b411-de5345a0204e') t),
  'namespaces', (SELECT json_agg(t) FROM (SELECT n.* FROM namespaces n JOIN db_cluster c ON c.namespace_id=n.id WHERE c.id='9fc47c1b-011d-4aaa-b411-de5345a0204e') t),
  'hardware_presets', (SELECT json_agg(t) FROM (SELECT hp.* FROM hardware_presets hp WHERE hp.id IN (SELECT DISTINCT hardware_preset_id FROM db_cluster_version WHERE cluster_id='9fc47c1b-011d-4aaa-b411-de5345a0204e')) t),
  'settings', (SELECT json_agg(t) FROM (SELECT * FROM settings WHERE type IN ('kafkaResizeProcessingEnabledProjects','kafkaModifyProcessingEnabledProjects')) t)
);" > /tmp/test-modify3.json
```

### Вставка локально

Грабли: в проде у `one_cloud_meta` есть колонка `fake_id`, в локальной схеме её НЕТ — дропать при INSERT.
Шаблон генерации SQL — python-скрипт из `history/2026-08-24-seed-test-modify3.md` (delete по cluster_id + INSERT ON CONFLICT DO NOTHING, порядок: namespaces → projects → hardware_presets → db_cluster → db_cluster_version → host_state → one_cloud_meta → operations → settings). Затем `docker cp` + `psql -f` (НЕ heredoc в stdin).

## Прод-Temporal: откуда брать типовые ошибки — общая для всех операций

UI: https://mdb-processing-temporal.common.mdb.one-infra.ru
API-база: `https://mdb-processing-temporal.common.mdb.one-infra.ru/api/v1/namespaces/default`

```bash
# Список выполнений upscale-флоу (broker — аналог controller по фазам)
curl -s "$API/workflows?query=%60WorkflowType%60%3D%22upscaleKafkaBroker%22&pageSize=100" \
 | jq -r '.executions[] | "\(.startTime) \(.status) \(.execution.workflowId) \(.execution.runId)"'

# Для controller: %22upscaleKafkaController%22 и %22upscaleKafkaControllerInDc%22

# Причина падения конкретного рана
curl -s "$API/workflows/$WID/history?runId=$RID&maximumPageSize=1000" | jq -r \
 '.history.events[] | select(.eventType=="EVENT_TYPE_WORKFLOW_EXECUTION_FAILED") | .workflowExecutionFailedEventAttributes.failure'
```

⚠️ Грабля: `?runId=` на этом инстансе часто игнорируется и возвращается последний ран —
если `length==0`, проверяй `startTime` первого события. Запасной путь — UI руками или
`SearchAttributes` (OperationId/ClusterId в indexedFields) для поиска нужного workflow.

Наблюдение (24.08.2026, upscaleKafkaBroker): 13 уникальных workflow с FAILED-ранами,
ВСЕ затем завершились COMPLETED после ретрая/перезапуска операции. Паттерн
«упал → перезапущен → сошёлся» — основной сценарий для симуляции.

## Декодирование input/output workflow (local temporal)

```bash
curl -s "http://localhost:8233/api/v1/namespaces/default/workflows/$WID/history" | \
  jq -r '.history.events[0].workflowExecutionStartedEventAttributes.input.payloads[0].data' | base64 -d | jq
```

Проверяем в input: `controllersPerDc` (абсолютные значения), `queueInfo`, `brokerDcs`, `hardwarePresetInputData`.

## План тестирования (upscale-контроллеров)

### T1. Happy path
1. Seed test-modify3, снапшот PMS (kafka.layout, kafka.controller.quorum).
2. Запустить upscale `{dc:2}` (+1 контроллер, в рамках лимита).
3. Проверить: порядок фаз (pms до deploy), child только по dc, reload с `parameters=null`, `saveUpscaledControllers` записал 2 хостов в dc.
4. Сверить PMS-кворум = union старых+новых хостов.

### T2. Идемпотентность повторного запуска (главный сценарий)
1. После успешного T1 запустить ту же операцию повторно (тот же operationId → тот же workflowId).
2. Ожидание: `Controller service in DC {} already has {} replicas, nothing to submit`, кворум не дублируется (union-merge), reload идемпотентен, workflow завершается COMPLETED без side-effects.

### T3. Симуляция падения в середине + рестарт
Способы симуляции (от простого к сложному):
- убить workflow через UI API (POST `…/workflows/{wid}/terminate`, csrf-cookie, см. ниже) на каждой фазе по очереди: после upsertPms, посреди deployControllers, после deploy до reload;
- ⚠️ wiremock НЕ перехватывает cloud-API — proxylib идёт в реальный one-cloud, маппинги бесполезны для симуляции cloud-падений; использовать terminate (parent или child);
- остановить mdb-processing (`kill`) во время reload-фазы → workflow остаётся RUNNING, рестарт воркера подхватывает.
После каждого падения — перезапустить операцию и проверить сходимость к T1-результату (те же хосты `buildHosts`, PMS не задублирован, хосты в mdb-data записаны ровно один раз).
⚠️ Окно «после upsertPms, до старта child» — секунды (фазы идут подряд); terminate по условию
«3-й upsert завершён» почти неуловим поллингом. T3a считается покрытым косвенно (T3c/T4).

### T4. Partial failure по одному ДЦ (цель `{dc:2,hc:2}`, +2 — максимум лимита)
1. Один child падает (см. T3), второй ДЦ задеплоен.
2. Ожидание: parent → non-retryable `PARTIAL_UPSCALE_FAILURE`; кворум в PMS уже включает оба ДЦ (upsert был до deploy) — это допустимо.
3. Перезапуск → упавший ДЦ доводится до цели, остальные скипаются (`nothing to submit`).

### T5. Downscale guard
1. Запрос с `controllersPerDc {dc:1}` при текущих 2.
2. Ожидание: child падает с non-retryable `DOWNSCALE_NOT_ALLOWED`, никаких изменений хостов/PMS.

### T6. Отсутствие очереди в целевом ДЦ
1. Убрать queue-переменные/очередь для одного ДЦ.
2. Ожидание: `submitQueueIfNeeded` + `waitQueueRunning` отрабатывают, деплой продолжается.

### T7. Контракт mdb-data ↔ processing
1. Modify-запрос из mdb-data (branch MDBDEV-3180) → декодировать temporal input.
2. Проверить маппинг: `controllersPerDc` абсолютные, `queueInfo` заполнен, TTL = `DEFAULT_TTL` (3ч) если не передан, `brokerDcs` из хостов.

### T8. Ретрай с изменённой целью
1. Первый запуск `{dc:2}` уронить посреди deployControllers.
2. Перезапуск с целью `{dc:2,hc:2}` (другая цель, шаг тоже в лимите).
3. Ожидание: сходится к новой цели; хосты от первой попытки входят в состав новой (buildHosts детерминирован `1..N`), PMS-кворум = union без дублей, мусорных хостов в mdb-data нет.

### T9. Конкурирующие запуски
1. Два workflow одновременно (разные operationId, та же цель) — второй должен упереться в `ignoreAlreadyStarted` по workflowId/childId.
2. Ожидание: деплой не задваивается, итоговый состав хостов корректен, оба workflow завершаются без конфликта.

| T1 Happy path `{dc:2}` | PASS 24.08 | history/2026-08-24-T1-happy-path.md |
| T2 Повторный запуск | PASS 24.08 | history/2026-08-24-T2-idempotent-rerun.md |
| T3a После upsertPms | покрыт косвенно | history/2026-08-25-T3a-T3b-T8.md |
| T3c Terminate в reload + рестарт | PASS 24.08 | history/2026-08-24-T3c-terminate-during-reload.md |
| T3b Terminate в multi-DC deploy | PASS 25.08 | history/2026-08-25-T3a-T3b-T8.md |
| T8 Ретрай с изменённой целью | PASS + находки | history/2026-08-25-T3a-T3b-T8.md |
| T9 Конкурирующие запуски | PASS + находки | history/2026-08-25-T9-concurrent.md |
| T4 Partial failure ДЦ | PASS 25.08 | history/2026-08-25-T4-partial-failure.md |
| T5 Downscale guard | PASS 25.08 | history/2026-08-25-T5-downscale-guard.md |
| T6 Новый ДЦ | PASS (частично) | history/2026-08-25-T6-queue-in-new-dc.md |
| Cleanup downscale ×3 | PASS 25.08 | history/2026-08-25-cleanup-downscale-all.md |
| T17/T18 Upscale в новый ДЦ ic (баг discovery) | PASS после фикса | history/2026-08-25-T17-new-dc-discovery-fix.md |
| T19 Повторный upscale в новый ДЦ | PASS 25.08 | history/2026-08-25-T19-repeat-new-dc.md |
| T10 Terminate child в waitInstance → PARTIAL_UPSCALE_FAILURE | PASS + БАГ 5 | history/2026-08-25-T10-partial-wait-instance.md |
| T11 Terminate в controller reload + рестарт | PASS 25.08 | history/2026-08-25-T11-terminate-in-controller-reload.md |
| T12 Частичный отказ broker reload (RELOAD_FAILED) | PASS 25.08 | history/2026-08-25-T12-partial-broker-reload.md |
| T13 Ретрай opId при существующих children | PASS 25.08 | history/2026-08-25-T13-child-already-exists.md |
| T14 Reconcile: облако опережает host_state | PASS 25.08 | history/2026-08-25-T14-reconcile-cloud-ahead.md |
| T7 Контракт mdb-data ↔ processing | PASS 25.08 | history/2026-08-25-T7-contract-mdb-data-processing.md |
| T15 Save-fallback (mdb-data недоступен при save) | PASS 25.08 | history/2026-08-25-T15-save-fallback.md |

## Найденные баги (upscale-контроллеров, сводка)

1. **БАГ 5 (T10, 25.08)**: `KafkaHostReloadHelper.executeReloadCycle` бесконечно ждёт
   non-RUNNING хост (instance в DEPLOYING → takeNext никогда его не отдаёт, выхода нет).
   Прод-паттерн `85169290`. **Фиксится в ветке** `ershov/MDBDEV-2375-skip-offline-Kafka-hosts-in-config-update-reload`
   (коммит f6a06aec): offline-хосты фильтруются один раз ДО цикла и скипаются (конфиг применится
   при следующем старте), workflow завершается COMPLETED. ⚠️ Остаточный пробел: хост, упавший
   в оффлайн ПОСЛЕ начального фильтра (середина reload-цикла), всё ещё даёт бесконечное ожидание —
   упомянуть в ревью MDBDEV-2375. См. T10 history.
2. **T10-симптом (T14-кейс)**: downscale при рассинхроне облако/host_state строит цель
   2→0 → INVALID_REPLICAS_COUNT. Лечить upscale-reconcile'ом, не downscale.
3. Minor (T11): terminated reload-child пробрасывается в parent без типизированного
   failure type (type=null) — сообщение в mdb-data неинформативно.

Конфиги и шаблоны запусков — `configs/` (README + direct-start шаблоны).

# Секция: Downscale Kafka-контроллеров (MDBDEV-3180)

## Прод-статистика (анализ 03.09.2026, из прод-Temporal)

- Старый per-DC флоу `downscaleKafkaController` (17–26.08): 72 запуска, 54 ok / 15 failed / 3 terminated.
- Новый `downscaleKafkaControllerInCluster` (с 26.08): 101 запуск, 70 ok / 31 failed.
  ВСЕ упавшие в итоге сходились перезапуском операции (включая 5–7 фейлов подряд:
  `bdf8bf6b`/`eba4c8ec` — 7 фейлов 31.08 → ok; `2655e340` — 5 фейлов → ok; `efabd6aa` — 5 → ok).
- Реальные причины фейлов (последние run'ы с доступной историей):
  1. `KafkaAdminClientException` на `kafka_host_getLeaderId` (`1a776d19`/stage-vk-support-kafka):
     AdminClient не подключается к брокерам :9092, `RETRY_STATE_MAXIMUM_ATTEMPTS_REACHED` —
     фаза applyNewQuorum (миграция лидера / поиск лидера).
  2. `CloudClientException` 404 «The entity you specified cannot be found / Service controller.*»
     (`24875e47`, `610ff371`): cloud-операция по уже удалённому сервису — рестарт флоу
     после выполненного stop+withdraw.
  3. `INVALID_REPLICAS_COUNT` «Current: 7, target: 1» (`34f6f10c`, старый флоу): mdb-data
     прислал цель с шагом >1 — non-retryable, zero side-effects.
- Прод-эталон фаз (успешный run `39099247`, target=0, последний контроллер в ДЦ):
  `getExistingServiceDcs`+`getInfosForServices` (discovery) → `reconcileKafkaCluster` →
  `getVariable`+`removeControllerFromQuorum` → `getLeaderId`×2 → `getInfoForInstances` →
  `updateConfigKafkaBroker` → `restartAndRestoreControllerInstanceSsh`+`pingReady` (лидер,
  remaining пуст — мигрировал) → child: `isServiceExists`+`stopService`+`getServiceInfo`×3+
  `withdrawService`+`isServiceExists`×3 → `isStorageExists`+`withdrawStorage` →
  `saveDownscaledKafkaControllersInfo`.

## Что тестируем

- **mdb-processing**: `DownscaleKafkaControllerInClusterWorkflowImpl` (parent) +
  `DownscaleKafkaControllerInDcWorkflowImpl` (child по ДЦ). Архитектура —
  `docs/kafka/downscale-controller.md` (ЧИТАТЬ ОБЯЗАТЕЛЬНО).
- Фазы parent (места падений): discovery → affectedDcTargets (+`INVALID_REPLICAS_COUNT`) →
  reconcileCluster → applyNewQuorum (кворум PMS → миграция лидера → reload брокеров →
  рестарт контроллеров+лидера) → downscaleControllers (child) → saveDownscaledControllers.
- Лимит как в upscale: шаг −1 контроллер на ДЦ за запуск; цели ниже — только через серию.

## План тестирования (downscale-контроллеров)

| # | Сценарий | Прод-паттерн |
|---|---|---|
| D1 | Happy path −1 контроллер (rescale-путь child): порядок фаз как в эталоне, PMS-кворум без удалённого, save ровно один раз | ~70% запусков |
| D2 | Happy path target=0: stop+withdraw сервиса, withdraw стораджа, лидер мигрирует в другой ДЦ, remaining пуст | эталон `39099247` |
| D3 | Идемпотентный перезапуск после успеха: `current <= target` → early return, ноль side-effects | — |
| D4 | Рестарт после stop+withdraw: кворум уже чист, сервиса уже нет → discovery не отдаёт ДЦ / child скипает, save не дублируется | 404 `24875e47`/`610ff371` — главный падающий прод-кейс |
| D5 | Рестарт посреди applyNewQuorum (после removeFromQuorum, до рестартов): PMS уже без хоста, хосты живы → `removeControllerFromQuorum` идемпотентен, сходится | — |
| D6 | AdminClient недоступен: `getLeaderId` исчерпывает ретраи → FAILED; после восстановления перезапуск сходится | `1a776d19`; локально — естественное состояние (9092 недоступен), для happy path наоборот нужны tp-port-forward + секреты (см. секцию vault ниже) |
| D7 | Partial failure: цель в 2 ДЦ, один child падает → `PARTIAL_DOWNSCALE_FAILURE` non-retryable; перезапуск доводит упавший ДЦ | — |
| D8 | Контракт шага >1: `INVALID_REPLICAS_COUNT` non-retryable, zero side-effects (валидация уезжает в mdb-data — тест держать как контракт до переезда) | `34f6f10c` |
| D9 | Лидер среди удаляемых: stop по SSH + waitLeaderMigrated + рестарт НОВОГО лидера, `LEADER_NOT_FOUND_IN_QUORUM` если миграция не сошлась | — |
| D10 | Серия ретраев (3+ падения на одном operationId): состояние не деградирует, каждый ретрай сходится к тому же плану (`buildHosts` детерминирован) | `bdf8bf6b`×7, `2655e340`×5 |
| D11 | Прерывание ПОСЛЕ удаления сервиса (до save) → перезапуск деградирует до ручной починки — «падает на рестартах» | `24875e47`/`610ff371` |

### D11. Прерывание после удаления сервиса (главная боль прода)

Прод-механизм (подтверждён 03.09.2026): оба кейса — единственная активность в упавшем ране
`cloud_getServiceInfo` с `CloudClientException` 404 «Service controller.* cannot be found»,
`RETRY_STATE_MAXIMUM_ATTEMPTS_REACHED`. Задеплоенная тогда версия шла в discovery через
`getServiceInfo` per-DC → сервис уже withdrawn → 404 → операция падала на каждом ретрае,
пока состояние не чинили руками.

Шаги:

1. Seed: 2 контроллера в dc (после T1-апскейла). Запустить downscale `{dc:1}` (или `{dc:0}`
   для withdraw-пути) через mdb-data API.
2. Дождаться завершения child (в history родителя появился `cloud_withdrawService` /
   `rescaleService` + `waitService*` прошёл), но terminate родителя ДО
   `saveDownscaledKafkaControllersInfo` (terminate через UI API по поллингу history;
   окно секунды — если не поймали, см. п.4: тот же эффект даёт kill mdb-processing).
3. Закрыть операцию в mdb-data (`UPDATE operations SET status='done'`), перезапустить
   downscale с той же целью через mdb-data API.
4. Зафиксировать фактическое поведение (текущий код):
   - discovery (`getExistingServiceDcs`) уже НЕ падает 404 — ДЦ без сервиса пропускается;
   - но `affectedDcTargets` пуст → ранний выход **БЕЗ `saveDownscaledControllers`**;
   - `host_state` в mdb-data всё ещё содержит удалённые хосты → десинк БД ↔ облако;
   - повторные запуски через mdb-data строят `controllersPerDc` из устаревшего host_state
     → «завершаются» без эффекта → операция не сходится никогда.
5. Ручная починка (процедура для дежурного, пока фикс не сделан): вырезать удалённые
   хосты из `host_state` (или перевести в removed) + убедиться, что PMS-кворум уже чист,
   операция закрыта. После этого новый запуск — ранний выход без side-effects (корректно).
6. Критерий будущего фикса (после него сценарий должен проходить без п.5): ранний выход
   при уже достигнутой цели обязан сохранять факт удаления в mdb-data — варианты:
   идемпотентный save по списку из host_state на стороне mdb-data при закрытии операции,
   передача ожидаемых удаляемых хостов в request, или save из child сразу после withdraw.
   Плюс regression-guard: discovery не должен падать 404 на отсутствующем сервисе
   (уже обеспечен `getExistingServiceDcs`, тест держит).

⭐ Сценарий прогонять в двух вариантах: target=0 (withdraw сервиса+стораджа) и target>0
(rescale: инстанс удалён, сервис жив) — во втором десинк тот же, но 404 не было никогда;
падение было только в deploy-версии с discovery через getServiceInfo.

Способы симуляции — те же, что в T3 (terminate через UI API, kill воркера, прямой запуск
только для негативных). Проверки после каждого сценария — общий чек-лист ниже
(host_state, PMS-кворум, operations, живость кластера).

⚠️ Статус на 03.09: D-сценарии ещё не пройдены — оба кластера заблокированы plait-PREFAIL
(reload-цикл висит вечно, живое подтверждение MDBSUP-4938); найдена и исправлена регрессия
MDBSUP-4939 (кворум для миграции лидера читался после чистки). Полный разбор и план
продолжения — `history/2026-09-03-downscale-incluster-first-runs-plait-prefail-block.md`.

✅ Статус на 04.09 (партия 3): SAVE ПЕРЕДЕЛАН на remaining-контракт (processing шлёт
  остающихся, mdb-data делает дифф с host_state; ДЦ без контроллеров уходит из
  controllerDcs/units) — D11 теперь САМОЗЖИВАЕТСЯ: ретрай из ловушки «cloud чист /
  host_state с хвостом» сошёлся за ~30с (det: kill mdb-data в окне save → FAILED → ретрай).
  D5/D10 — PASS (ретрай доводит до конца включая save). Коммиты: processing `a1f6eaf5` (MR !440),
  mdb-data `be484944` (MR !481, без version-bump). Все D-сценарии закрыты.

✅ Статус на 04.09 (партии 1-2): D-матрица закрыта на трёх кластерах параллельно —
D1+миграция лидера (modify4 `d69b1f3e`), D2 withdraw pc→0 (downgrade7 `9f75061d`),
D2 ic→0 + D11-десинк (modify3 `9d14e5c3`/`298aaa4f`), D5 terminate-после-кворума +
ретрай тем же id (`be6c5705`), D10 серия прерываний (`94bfc3ce`), D8 guard
увеличения → PARTIAL_DOWNSCALE_FAILURE (`d8-direct-guard-9fc47c1b`), D6 битый
vault-секрет → KafkaAdminClientException = прод `1a776d19` (`fbab747a`).
Инфра-фиксы: mdb-data переведён на контракт queueInfo/controllersPerDc
(branch-snapshot processing-api + mapper), save-фаза направлена на реальный
mdb-data:8081. Разбор — `history/2026-09-04-D1-D2-D11-three-clusters-parallel.md`.
⚠️ Даунскейл при десинке «host_state < облако» ОПАСЕН: цель строится из host_state и флоу
снимет ВСЕ контроллеры ДЦ (потеря кворума) — сначала converge-ретрай апскейла.
⚠️ Direct-start воркфлоу руками: task queue `kafka-activities-queue` (не `kafka-activities-worker`),
payload encoding `application/json` (не `json`) — иначе workflow висит/падает на первом таске.

⚠️ Версии processing-api: mavenLocal vs глобальный стор (находка 06.09). Если в mdb-data
запинена РЕЛИЗНАЯ версия `processing-api:3.x.y`, Gradle возьмёт её из nexus — а релиз,
опубликованный ДО переезда контракта, содержит старый DTO (`replicas` вместо
`controllersPerDc`/`queueInfo`). MapStruct при этом молча собирает DTO без новых полей
(unmapped target = WARNING, не ERROR) — сборка зелёная, а в Temporal улетают null'ы.
mavenLocal перекрывает nexus только при СОВПАДЕНИИ GAV. Рецепт: публиковать api ветки
под отдельной версией и пиннуть её в mdb-data:
1. В processing: init-script `gradle.projectsEvaluated { rootProject.findProject(':api')?.version = '3.57.local' }`
   + `./gradlew -I init.gradle :api:publishToMavenLocal`.
2. В mdb-data: `implementation 'one.cloud.mdb:processing-api:3.57.local'`.
3. Проверять ПОЧТИ сгенерённый MapStruct-имплементор
   (`build/generated/.../KafkaHostMapperImpl.java` — в toDto должны быть
   `controllersPerDc(...)` и `queueInfo(...)`), а не только EXIT код сборки.

⚠️ Downscale — операция отката для upscale-сценариев: после каждого upscale-теста
возвращать состав через downscale (не руками). Для самих D-сценариев наоборот —
восстановление upscale-ом.

# Секция: Downscale Kafka-брокеров (MDBDEV-2900)

## Что тестируем

- **mdb-processing** (ветка `ershov/MDBDEV-2900-downscale-kafka-brokers`):
  `DownscaleKafkaBrokerInClusterWorkflowImpl` (parent) + `DownscaleKafkaBrokerInDcWorkflowImpl`
  (child по ДЦ) + переиспользуемый `ReassignKafkaPartitionsWorkflow` (child реассигна,
  `topics=null` → все топики включая internal). Архитектура — `docs/kafka/downscale-broker.md`
  (ЧИТАТЬ ОБЯЗАТЕЛЬНО).
- Фазы parent (места падений): `resolveRemovedBrokerIds` (discovery удаляемых хостов по ДЦ +
  `describeBrokerIds` реестра кластера) → `reassignPartitionsFromRemovedBrokers`
  (child-реассигн round-robin по survivors + поллинг `isBrokerDrained` 30с до TTL →
  `BROKER_NOT_DRAINED`) → `unregisterBrokers` (KRaft, идемпотентен) → `downscaleBrokers`
  (параллельные InDc-child: rescale / при цели 0 stop+withdraw сервиса и стораджа).
- API: `DELETE /api/v1/mdb/processing/kafka/clusters/{clusterId}/hosts/brokers`,
  DTO `brokersPerDc` (абсолютные) + `queueInfo` + `connectionParams`.

## Ограничения масштаба (обязательно)

- **Выживающие ≥ max RF кластера**: survivors после удаления должны покрывать replication
  factor (иначе guard `REASSIGN_INVALID_TARGET_BROKERS`). Для test-кластера с RF=3 минимум
  3 выживающих брокера в кластере.
- Seed-кластер test-modify3 — по 1 брокеру на ДЦ: перед B-сценариями обязателен
  **upscale брокеров до 2 в целевом ДЦ** (через mdb-data), откат между сценариями — upscale'ом.
- Пачка ≤2 брокеров за запуск (аналог лимита контроллеров): `{dc:1}` или `{dc:1,hc:1}` из 2×2.
- НЕ гонять `{dc:0,hc:0,kc:0}` — вывод всех брокеров кластера.

## Специфика брокеров (отличия от контроллеров)

- Нужны **живые партиции**: до серии создать 2–3 тестовых топика (RF=3, 6–12 партиций) через
  mdb-data API и записать данные — иначе reassign нечего двигать и drain мгновенный.
- KafkaAdminClient обязан достучаться до :9092 — vault-секреты + PEM truststore по секции
  «Локальный downscale: vault-секреты» выше; при отсутствии сети — `mcc tp-port-forward` на 9092.
- Верификация reassign: после флоу `describePartitions` (по API reassign или kafka-topics.sh
  через mcc) — ни одна партиция не содержит удалённые brokerIds; распределение по survivors
  примерно равномерное (round-robin).
- Верификация unregister: `describeCluster`/`kafka-broker-api-versions.sh` — удалённые id
  отсутствуют в реестре.
- ✅ Save в mdb-data (`saveDownscaledKafkaBrokers`) **реализован map-контрактом** (10.09,
  mdb-data `ershov/MDBDEV-2900-downscale-brokers-save-map` `4d8eef12`): processing шлёт ту же
  карту `remainingBrokersPerDc`, mdb-data сам считает жертв по host_state (старшие индексы,
  `HostUtils.hostIndex`). Хвостов в host_state после COMPLETED больше нет; no-op ретрай
  закрывается валидатором mdb-data (400 Nothing to downscale). Разбор —
  `history/2026-09-10-B-save-map-contract-party.md`.

## Подготовка B-сценариев: upscale 4-го брокера в новый ДЦ (контракт brokersPerDc)

`POST /api/v2/mdb/kafka/clusters/{clusterId}/hosts/brokers`, тело `{"brokersPerDc": {...}}` —
**абсолютные цели по ВСЕМ ДЦ кластера**: существующие ДЦ остаются с текущим числом (child —
no-op), новый ДЦ получает первую реплику. Слать только новый ДЦ НЕЛЬЗЯ:
`UpscaleKafkaBrokerWorkflowImpl.resolveSourceDc` ищет broker-сервис-источник манифеста только
среди целевых ДЦ запроса → `NO_SOURCE_BROKER_SERVICE` non-retryable.

Прод-доказательство (скан 30 прогонов 01–07.09.2026, прод-Temporal): COMPLETED всегда с полной
картой — `{pc:1,kc:1,hc:1,ec:1}` (dzen-common, ec новый), `{nc:2,zc:2,ic:2}` (dsp-notices),
`{nc:1,ic:1,dc:1,pc:1}`; FAILED карты тоже полные (`PARTIAL_UPSCALE_FAILURE` — уже после
source-фазы). Ни одного прогона с картой из одиночного нового ДЦ.

Рабочие вызовы (06.09/07.09, 202 на всех):
- test-modify3: `{"pc":1,"kc":1,"hc":1,"ic":1}`
- test-modify4: `{"rc":1,"pc":1,"kc":1,"ic":1}`
- test-downgrade7: `{"pc":1,"kc":1,"hc":1,"ic":1}`

⚠️ Перед перезапуском закрывать упавшие операции (`UPDATE operations SET status='done'`),
иначе mdb-data вернёт 409. Наблюдение: без стаба ABC (:3000) upscale не доходит даже до
Temporal (500 на квотах) — см. таблицу инфраструктуры.

## План тестирования (downscale-брокеров)

| # | Сценарий | Аналог контроллеров |
|---|---|---|
| B1 | Happy path −1 брокер (rescale-путь): discovery → reassign по survivors → drain → unregister → rescale; топики без удалённого brokerId, ISR полный, кластер жив | D1 |
| B2 | Happy path пачкой из 2 ДЦ (`{dc:1,hc:1}` из 2×2): ОДИН общий reassign-план (survivors = все минус обе жертвы), InDc-children параллельно | — (новое: пачка) |
| B3 | Happy path target=0 в одном ДЦ: stop + withdraw сервиса и стораджа broker.*; партиции заранее уведены reassign'ом | D2 |
| B4 | Идемпотентный перезапуск после успеха: `current <= target` → `removedBrokerHosts` пуст, reassign/unregister/rescale скипаются, ноль side-effects | D3 |
| B5 | Рестарт после unregister до rescale: жертвы уже вне `describeBrokerIds` → `mapToBrokerIds` пуст → reassign-фаза скип, downscaleBrokers доводит rescale | D4/D5 |
| B6 | Terminate посреди поллинга drained: реассигн уже запущен в фоне Kafka → ретрай: тот же child-workflowId (`ignoreAlreadyStarted`), поллинг продолжается, сходится | D5 |
| B7 | AdminClient недоступен (битый vault / нет сети на 9092): `describeBrokerIds`/`listTopicNames` исчерпывают ретраи → FAILED; после восстановления перезапуск сходится | D6 |
| B8 | Partial failure: цель в 2 ДЦ, один InDc-child падает (terminate) → `PARTIAL_DOWNSCALE_FAILURE` non-retryable; ретрай доводит упавший ДЦ, reassign-фаза скипается (уже unregister) | D7 |
| B9 | Guard увеличения: `target > current` → child non-retryable `DOWNSCALE_NOT_ALLOWED`, zero side-effects | D8 |
| B10 | Capacity guard: survivors < max RF (например RF=3, после удаления остаётся 2) → non-retryable `REASSIGN_INVALID_TARGET_BROKERS` ДО каких-либо cloud/PMS изменений | D8 |
| B11 | Drain timeout: реассигн не завершается до TTL (заглушить порт/заморозить репликацию) → `BROKER_NOT_DRAINED`; фон Kafka доезжает → ретрай сходится | — |
| B12 | Контракт mdb-data ↔ processing: декодировать temporal input parent'а — `brokersPerDc` абсолютные, `connectionParams` (bootstrap + vault-path), child-реассигн: `topics=null`, `targetReplicationFactor=null`, TTL=6ч; MapStruct-импл проверить (грабля mavenLocal vs nexus) | T7 |
| B13 | Десинк host_state после успеха: save в mdb-data отсутствует → после COMPLETED host_state содержит удалённые хосты; повторный запуск через mdb-data строит цель из устаревшего host_state — фиксировать фактическое поведение, критерий будущего фикса = аналог remaining-контракта контроллеров (MR !440/!481) | D11 |
| B14 | Серия ретраев (3+ terminate на одном operationId): survivors детерминированы сортировкой по host — каждый ретрай строит тот же план, состояние не деградирует | D10 |
| B15 | Обрыв withdraw-ребёнка в частичном состоянии: terminate после `stopService`, до `withdrawService`/`withdrawStorage` → ретрай тем же opId: ребёнок повторяет isServiceExists→stopService (идемпотентен на остановленном) → withdraw → storage → converge | D11-зона риска для брокеров (save нет — ручная чистка host_state хвоста обязательна после) |
| B16 | Обрыв в drain-поллинге при drain=**false** (Kafka переливает в фоне): fill ВСЕХ топиков перед запуском → terminate в поллинге → ретрай: поллинг возобновляется и ждёт фактического конца переливки → converge | D5-аналог по брокерскому drain |
| B17 | Обрыв внутри reassign-child ДО apply alter: terminate родителя убивает ребёнка (PARENT_CLOSE_POLICY_TERMINATE) → ретрай: child TERMINATED реюзабелен (ALLOW_DUPLICATE_FAILED_ONLY) → пересоздаётся и повторяет идемпотентный alter → converge | — |

**Карта покрытия обрывов (08.09):** reconcile ✓(все ретраи) / discovery ✓(read-only) /
reassign-child до alter = B17 ✓ / drain при false = B16 ✓ / drain при true = B14#1 ✓ /
unregister = B5 ✓ / rescale-дети = B8, B14#2 ✓ / withdraw-частичное = B15 ✓ / kill воркера —
механизм T3c/D5 (контроллеры), для брокеров эквивалентен terminate-ретраю. **Полное покрытие.**

### Статусы B-сценариев (партия 08.09, три кластера параллельно)

| # | Статус | Суть проверки (кратко) | Примечание |
|---|---|---|---|
| B1 | ✅ PASS 08.09 | Полный цикл rescale ic 2→1: reassign 76 партиций (вкл. CC-топики) → drain → unregister 23002 → rescale | modify3 `1f3e2c71`; 2 рана на инвертированном цикле (баг #2) терминированы, 3-й COMPLETED |
| B2 | ✅ PASS 08.09 | Пачка 2 ДЦ (2.pc+2.ic из 2×2): ОДИН reassign-план, параллельные InDc | modify4 `62c713bf`; survivors [25001,21001,22001,23001] — сортировка по host |
| B3 | ✅ PASS 08.09 | Withdraw ic→0: stop → withdrawService → withdrawStorage | downgrade7 `ad9430ce`; вскрыл баг #1 (TOPIC_NOT_EXISTS на `__consumer_offsets`) — фикс, ретрай тем же opId довёл |
| B4 | ✅ PASS 08.09 | Повтор цели после успеха: все фазы skip, ноль side-effects | modify3, новый opId |
| B5 | ✅ PASS 08.09 | Рестарт после unregister до rescale: 23002 вне реестра → reassign/unregister skip → дети доводят | modify3 `f44bc768` (ран1 terminate в ic-child, ран2 COMPLETED) |
| B6 | ⚠️ покрыт косвенно | Terminate строго В drain-поллинге | окно <30с при мелких партициях не ловится; семантика ретрая покрыта B1-retry/B5/B14 |
| B7 | ✅ PASS 08.09 | Битый vault `super` → AdminClient ретраи → восстановление секрета → тем же раном сошёлся | modify3 `c873b90b` |
| B8 | ✅ PASS 08.09 | Terminate одного InDc в пачке → `PARTIAL_DOWNSCALE_FAILURE [pc]` → ретрай доводит | modify4 `91aaf192`; siblings не страдают |
| B9 | ✅ PASS 08.09 | Увеличение ic:1→2 → `DOWNSCALE_NOT_ALLOWED`, zero side-effects | downgrade7 `fccb2997` |
| B10 | ✅ PASS 08.09 | Survivors 2 < RF 3 → `REASSIGN_INVALID_TARGET_BROKERS` до изменений | downgrade7; только listTopics+describe в истории |
| B11 | ⏸️ не воспроизведён | Drain timeout до TTL (`BROKER_NOT_DRAINED`) | нет локального рычага замедлить репликацию; отложен на стенд |
| B12 | ✅ PASS 08.09 | Контракт: `brokersPerDc` абсолютные, TTL=6ч, child `topics=null`/`targetRF=null`, survivors по host | декод history parent+child |
| B13 | ✅ подтверждён 08.09 | Десинк host_state после COMPLETED (save не реализован) | хвосты 2.broker.* чистились перед каждым rollback-upscale |
| B14 | ✅ PASS 08.09 | 2× terminate на одном opId (drain-фаза, дети) → ретрай №3 COMPLETED | modify4 `c59e59f6`; состояние не деградировало |
| B15 | ✅ PASS 08.09 | Terminate mid-withdraw (stop сделан, withdraw нет) → ретрай: повторный stop на остановленном → withdraw → converge | downgrade7 `5d1827fc`; ловля — поллинг ic-child до появления `cloud_stopService` |
| B16 | ✅ PASS 08.09 | Terminate в drain при drain=false (fill-ALL перед запуском) → ретрай: поллинг возобновился, дождался фоновой переливки 76 партиций → COMPLETED | modify3 `8d6fc604`; последний полл перед terminate = `false` |
| B17 | ✅ PASS 08.09 | Terminate внутри reassign-child (list done, describe в полёте, alter НЕ применён) → ретрай: ребёнок пересоздан (TERMINATED реюзабелен), полный план повторно, alter применён → COMPLETED | modify3, ловля поллингом 3с за ~12с до старта ребёнка |

**Баги, найденные партией (фиксы в ветке MDBDEV-2900, к коммиту):**
1. `validateTopicsExist` сверял с `listTopics(listInternal(false))` → `__consumer_offsets` давал
   non-retryable `TOPIC_NOT_EXISTS` в reassign-child (любой кластер с consumer-группами).
   Фикс: `listAllTopicNames()`.
2. **Инверсия цикла drain-поллинга**: `while (noneMatch(notDrained))` = «пока все дренированы —
   спать»; флоу висел вечно при успехе и пропускал ожидание при недренаже. Фикс: `anyMatch`.
3. Ретрай тем же operationId падал `WORKFLOW_ALREADY_EXISTS` на reassign-child (COMPLETED в
   прошлом ране; reconcile был обёрнут, reassign — нет). Фикс: `runIgnoringAlreadyStarted`.
4. (Minor, фикс) `KafkaHostActivityImplTest` — мок обновлён под семантику фикса №1.

Разбор — `history/2026-09-08-B-broker-downscale-party1.md`.

### B13. Десинк host_state — ВЫЛЕЧЕН 10.09 (map-контракт)

Контроллерный аналог D11 лечился remaining-списком; брокеры пошли дальше — processing
шлёт map-контракт (`remainingBrokersPerDc` as-is), жертв считает сам mdb-data по host_state.
Проверено живым прогоном (test-modify3, kill mdb-data в окне save → FAILED → ретрай тем же
requestом самолечил host_state за 15с, хвостов нет, PMS не тронут). Критерий п.5 выполнен:
после COMPLETED host_state сходится с облаком, повторный запуск — 400 no-op в валидаторе.
Разбор — `history/2026-09-10-B-save-map-contract-party.md`.

### Ре-чек на финальном коде (11.09, после влития MDBDEV-2900 в master + mdb-data !496)

Один deploy-контур: processing master `8bf5f3fa` + mdb-data `ershov/MDBDEV-3301-topic-other-properties`
(merge `a2e643f2`), оба пина релизные и содержат контракт (`data-api:1.104.0`,
`processing-api:3.62.1` — проверено по jar-ам gradle-кеша, mavenLocal-грабля снята).

- ✅ B1+save (`bc4bc5af`): reconcile(isWan)→…→save последним, host_state чист сразу.
- ✅ B2 (`4156a19e`), ✅ B8 (terminate pc-InDc → `failed in 1 DC(s): [pc]` → ретрай `49f5097f`
  дослал rescale, save снёс оба хвоста), ✅ B15 (`effad900`→`224f9594`: stop идемпотентен →
  withdrawService → withdrawStorage → save).
- ✅ НОВОЕ (mdb-data валидации, 400 до Temporal): B4 «Nothing to downscale», B9 «must not be
  greater than current», min-3 «min value of brokers is 3» — processing-side guard'ы остались
  defense-in-depth, недостижимы через mdb-data.
- ✅ B12: isWan=false + TTL=6ч в parent input; child topics/targetRF null, жертвы исключены.
- ✅ B13 самолечение ×2: каждый COMPLETED без хвостов + стартовый десинк downgrade7
  (ic-сервис отсутствовал в облаке с 08.09, host_state с хвостом → запуск ic:0 → save удалил).
- Пропущено с обоснованием: B5/B6/B7/B14/B16/B17 (код путей не менялся, save идёт после
  детей — ретрай-семантика подтверждена B8/B15), B10 processing-side (не менялся, через
  mdb-data недостижим), B11 (нет рычага, стенд).
- ⚠️ Находка: после withdraw-партий сверять ОБЛАКО, а не только host_state (хвост 08.09
  вскрывался только запуском). ⚠️ В COMPLETED-событиях истории `activityType.name=null` —
  имена брать из SCHEDULED. ⚠️ Перед ре-чеком проверять lstart сервисов против коммитов.

Разбор — `history/2026-09-11-B-recheck-final-code.md`.

### Порядок первой партии

B1 → B4 (сходятся на том же состоянии) → B6/B5 (рестарты) → B9/B10 (guards, прямой запуск
допустим) → B2 (пачка, после upscale до 2×2) → B3 → B8 → B7/B11 (по возможности) →
B12 (контракт) → B13 (зафиксировать десинк). Откат между сценариями — upscale брокеров,
не руками.

## Верификация кластера ДО и ПОСЛЕ каждого сценария

Чтобы иметь полную картину (что изменил именно наш флоу, а не фон), проверка обязательна
**до** сценария и **после** него — с diff между снимками:

**До сценария (baseline-снимок):**
- `host_state` mdb-data: текущий состав контроллеров по ДЦ;
- PMS: `kafka.controller.quorum`, `kafka.layout` (снапшот значений);
- `/kafka-cluster-inspector`: KRaft quorum собран, состав voters, все брокеры/контроллеры живы;
- `/kafka-config-inspector`: что отрендерено на хостах сейчас (`controller.properties`/`broker.properties`).

**После сценария** — те же точки, сравнение с baseline:

1. **Живость кластера** — скилл `kafka-cluster-inspector`: KRaft quorum собран со всеми
   контроллерами (в т.ч. новыми), регистрация брокеров/контроллеров, нет `Broker is dead`,
   ISR/репликация не сломаны. Список хостов — из `host_state` test-modify3
   (`1.controller.test-modify3-mdbdev-kafka.<dc>.one-infra.ru`).
2. **PMS-конфиги** — скилл `kafka-config-inspector`: сверить `kafka.controller.quorum`
   (каждый хост ровно один раз, соответствует финальному составу), `kafka.layout`,
   и что отрендеренные конфиги на хостах (`controller.properties`/`broker.properties`)
   физически содержат новый кворум после reload-фазы.

Если кластер не собирает кворум или конфиги на хостах не соответствуют PMS — это баг
нового кода, фиксировать в `history/` сценария. Любое изменение, которого не было в
baseline и которое не объясняется флоу, — подозрение на побочный эффект.

## Откат состояния между сценариями

Local-грабли (актуально):
- **Terminate workflow**: только через UI API — GET любого API (получить cookie `_csrf`) → POST `…/api/v1/namespaces/default/workflows/{wid}/terminate` c header `X-Csrf-Token: <cookie>` и body `{"reason":...}`. DELETE и temporal-CLI в контейнере НЕ работают.
- **409 при новом запуске**: mdb-data блокирует операции кластера, пока последняя active/failed — в тестах UPDATE `operations SET status='done'`.
- **Downscale локально падает** на `migrateLeader` (KafkaAdminClient не достучится до :9092 с ноутбука). Откат между сценариями делать не downscale-флоу, а выбором другого целевого ДЦ; либо поднимать tp-port-forward на 9092.

## Локальный downscale: vault-секреты + PEM truststore (делает downscale рабочим)

Источники секретов — прод-vault хоста брокера (см. `mdb-local-tester/history/kafka-controller-downscale-test-modify3-2026-08-13.md`):
1. На broker-хосте `/root/.vault-token` + `VAULT_ADDR=https://pc.vault.infra.one-infra.ru` (env внутри контейнера хоста).
 2. Путь секрета = PMS `zen.kafka.vaultRoot` (узнать через `pms-read.sh`, скилл [`pms-worker`](../pms-worker/SKILL.md)) + `/<secret>`; чтение KV v2:
   `curl -s -H "X-Vault-Token: $(cat /root/.vault-token)" $VAULT_ADDR/v1/zkv/data/mdb/mdbdev/kafka/<queue>/<secret>` (⚠️ `/data/` в пути! без него — permission denied).
3. Нужны три секрета (ключ `password`): `super`, `keystore-password`, `truststore-password`.
   Проверка `super`: совпадает с `user_super` в `/opt/kafka/config/jaas.conf` на брокере.
4. Залить в локальный vault:
```bash
docker exec mdb-processing-vault sh -c \
  "VAULT_ADDR=http://localhost:8200 VAULT_TOKEN=root vault kv put \
   zkv/mdb/mdbdev/kafka/<queue>/<secret> password=<VALUE>"
```
5. PEM CA: `mcc --local -n infra scp <broker>:/opt/kafka/ssl/tls_ca.crt /tmp/kafka-secrets/` → `~/.mccloud/kafka-tls-ca.crt`,
   затем в `application-local.yaml` (⚠️ revert перед коммитом):
```yaml
app:
  kafka:
    namespaces:
      infra:
        ssl-truststore-location: ${HOME}/.mccloud/kafka-tls-ca.crt
```
6. После правки yaml — рестарт mdb-processing.
⚠️ Даже с секретами KafkaAdminClient должен достучаться до :9092 брокеров — если сети нет,
нужен `mcc tp-port-forward` на 9092 (не проверено).

Чтобы прогонять сценарии T1–T9 многократно на одном кластере, после каждого сценария
возвращай кластер к исходному составу (1 контроллер на ДЦ) через **`downscaleKafkaController`**
(обратная операция того же семейства, не руками через PMS/one-cloud!). После отката —
снять baseline-снимок заново и убедиться, что кворум/layout в PMS вернулись к исходным
и кластер жив (`/kafka-cluster-inspector`). Откат руками (удаление хостов, правка PMS
напрямую) запрещён — ломает картину для следующего сценария.

## Правило запуска операций

Запускать флоу **только через mdb-data API** (`POST/DELETE …/hosts/controllers?dc=…`), НЕ напрямую
через `temporal workflow start`. Прямые запуски рассинхронизируют локальную БД с облаком:
mdb-data строит `controllersPerDc` из host_state, а при direct-start save не выполняется →
следующий запуск через mdb-data падает с `DOWNSCALE_NOT_ALLOWED` (цель ниже фактических
контроллеров в облаке). Прямой запуск допустим ТОЛЬКО для негативных тестов (T5 guard), где
side-effects не важны, и цель не должна выполняться.

## Чек-лист верификации после каждого теста

- [ ] Статус workflow COMPLETED (или ожидаемый FAILED с нужной причиной)
- [ ] **host_state** (mdb-data): контроллеры `1..N` в каждом целевом ДЦ, без дублей — проверять ВСЕГДА
- [ ] **db_cluster_version** (mdb-data): новая версия создаётся ТОЛЬКО при добавлении контроллера
      в НОВЫЙ ДЦ (появление ДЦ в `controllerDcs` + units); upscale в существующих ДЦ версию не меняет —
      это норма. Проверять обе таблицы после каждого теста, чтобы отличить норму от потерянного save.
- [ ] PMS `kafka.controller.quorum`: каждый хост ровно один раз; `kafka.layout` — строка на ДЦ
- [ ] `operations` в mdb-data: одна операция, статус done/in_progress корректен
- [ ] Повторный запуск не создаёт вторую операцию/дубли хостов
- [ ] Живость кластера проверена через `/kafka-cluster-inspector` (KRaft quorum, нет dead-брокеров)
- [ ] PMS и отрендеренные конфиги на хостах сверены через `/kafka-config-inspector`

## Сохранение результатов

Каждый прогнанный сценарий — в `history/` этого скилла: seed SQL, modify JSON, workflowId,
декодированный input, результат, найденные баги. Прежде чем писать новый сценарий — `ls history/`.

Готовые сценарии modify-тестов и снапшоты инфраструктуры — история скилла **`mdb-local-tester`**:
`ls ~/.claude/skills/mdb-local-tester/history/` (там же — детали запуска инфраструктуры, порты, грабли psql/auth/PMS).
