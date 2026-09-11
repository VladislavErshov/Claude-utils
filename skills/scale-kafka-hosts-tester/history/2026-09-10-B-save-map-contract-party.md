# 2026-09-10: B-save party — map-контракт saveDownscaledKafkaBrokers (B13-фикс)

Контракт: processing шлёт в `POST /save/downscaled-brokers` ту же карту
`remainingBrokersPerDc`, что пришла в запросе; mdb-data сам вычисляет жертв по
host_state (в ДЦ карты — брокеры сверх цели со старшими индексами hostname,
`HostUtils.hostIndex`; ДЦ без записи не меняется).

## Ветки/коммиты

- mdb-data: `ershov/MDBDEV-2900-downscale-brokers-save-map`, коммит `4d8eef12`.
- data-api в mavenLocal: `1.103.0-map-local` (init-script pin, `./gradlew -I /tmp/init-data-api.gradle :api:publishToMavenLocal`).
- mdb-processing: master (рабочее дерево), activity `saveDownscaledKafkaBrokerInfo(UUID, Map)` → workflow `saveRemainingBrokers` передаёт `request.remainingBrokersPerDc()` as-is.
- mdb-data NullAway проверен: probe с `@NullMarked` + null-return ловится как error (чекер живой).

## Инфра

Всё стандартное (temporal:8233, pg:6434, wiremock:8088, ABC:3000); mdb-data:8081 и
mdb-processing:8080 перезапущены со свежим кодом (старые висели со вчера).
PMS до/после: `kafka.layout` / `kafka.controller.quorum` не изменились (брокерный
флоу их не трогает). PMS-грабля: переменные живут на pmsHost `<queue>.clouds`
с application=**mdb** (не kafka).

## Сценарии (test-modify3, 9fc47c1b-011d-4aaa-b411-de5345a0204e)

### 1. Подготовка: upscale ic 1→2 (полная карта {"pc":1,"kc":1,"hc":1,"ic":2})
202, workflow `53380b51` COMPLETED за ~3.5 мин, `2.broker.ic` в host_state.

### 2. B1+save happy path: downscale {"ic":1}
- 202, workflow `56c4b2f3` COMPLETED за ~3 мин.
- Фазы по history: describeBrokerIds×2 → isBrokerDrained → unregisterBroker →
  (InDc rescale) → **saveDownscaledKafkaBrokerInfo** — последний activity.
- mdb-data: «Save remaining Kafka brokers per DC: {ic=1}» → «removing 1 stale hosts:
  [2.broker.test-modify3-mdbdev-kafka.ic.one-infra.ru]».
- **host_state чист сразу** — хвоста нет (до фикса B13 хвост оставался). PASS.

### 3. No-op валидация (побочное доказательство фикса)
Повторный DELETE {"ic":1} → 400 «Nothing to downscale»: валидатор строит current
из host_state, который теперь синхронен с облаком. До фикса request проходил
валидацию и workflow делал пустой ран. PASS.

### 4. T15/D11-аналог: kill mdb-data в окне save → ретрай самолечится
- Подготовка: upscale hc 1→2 (`2.broker.hc` в host_state).
- DELETE {"hc":1} (202, operation `42c26a5f`) → через 2с kill mdb-data.
- Workflow `42c26a5f` RUNNING ~7.5 мин: drain→unregister→rescale прошли, save
  ретраился (mdb-data down) → **FAILED на saveDownscaledKafkaBrokerInfo**.
  Десинк воспроизведён: облако hc=1, host_state ещё содержит `2.broker.hc`.
- mdb-data поднят, операция закрыта (`UPDATE ... SET status='done'`).
- Ретрай DELETE {"hc":1} → operation `107c9115`, workflow COMPLETED за **15 сек**:
  discovery по облаку пуст (жертвы уже нет) → reassign/unregister/rescale skip →
  save с {hc:1} → «removing 1 stale hosts: [2.broker.hc...]».
- host_state: 0 хвостов `2.broker.*`. Операция закрылась `done` (статус-поллинг
  сошёлся на втором цикле; WARN WorkflowQueryException сразу после COMPLETED —
  временный, воркер ещё держал workflow). PVS/PMS не тронуты. **PASS — главный
  сценарий: ручная чистка host_state больше не нужна.**

## Остаточное состояние

modify3: брокеры 1×(hc,ic,kc,pc) — исходный состав, откат не требуется.
