# 2026-09-03 — Downscale InCluster: первые прогоны D-сценариев, plait-PREFAIL блокер, фикс MDBSUP-4939

Ветка `ershov/MDBDEV-3180-Kafka-downscale-controllers-per-DC-with-idempotent-withdraw-restart`.
Первый выезд новых D-сценариев (downscale) на живых dev-кластерах. Полностью D-сценариев
не пройдено — оба кластера упёрлись в инфраструктурный блокер (plait-PREFAIL). Главные
результаты сессии — пойманная регрессия MDBSUP-4939 в новом коде и живое подтверждение
зависания reload-цикла на PREFAIL (открытый MDBSUP-4938).

## Исходное состояние

- mdb-processing 8080 / mdb-data 8081 / temporal 8233 / pg 6434 / vault 8200 — подняты;
  processing перезапущен на ветке (truststore добавлен в `application-local.yaml`:
  `app.kafka.namespaces.infra.ssl-truststore-location: ${HOME}/.mccloud/kafka-tls-ca.crt`,
  ⚠️ revert перед коммитом).
- Секреты: в локальном vault уже лежат `super` для test-modify3/4, test-downgrade7
  (keystore/truststore-password НЕ нужны для downscale). :9092 брокеров доступны с ноутбука напрямую.
- ДЦ для работы: **hc, kc, pc, ic** (запретные: dc, rc, прочие).
- Квота project 7478 (mdbdev) исчерпана (`vCPU=81 > 80`) — юзер освободил вручную.

## Кластеры (актуальное состояние на конец сессии)

### test-modify3 (9fc47c1b) — НЕ трогать в этой сессии
- В облаке только dc: 1 broker RUNNING, 1.controller RUNNING, 2.controller STARTING(битый,
  держит квоту). Контроллеров hc/kc/ic нет; PMS-кворум dc/hc/kc (stale) не соберётся.
- Застрявшая операция `0e10346a` (upscale {dc:2,ic:1,hc:1,kc:1}, висела с 12:03 в
  waitServiceRunning) — terminated + закрыта (done) в локальной БД.
- Ремонт: этапы из плана (очистка квоты удалением битого инстанса → solo-кворум → ресид) — не выполнялись.

### test-downgrade7 (23f108ac) — заморожен до очистки plait
- Seed перезалит из прода (brokers+controllers hc/kc/pc, cruise kc) — операция
  `2b49ae72` (upscale hc, D1-шаг1):
  - attempt 1: упала на сетевом сбое pc-мастера (`ServiceUnavailableException` в
    getInfoForInstances, рестарт pc-брокера) → PARTIAL;
  - attempt 2 (`75c9e747`): 2.controller.hc задеплоен ✓, кворум PMS +10002 ✓,
    брокеры hc/kc перезагружены ✓, но reload завис на **pc-брокере availability=PREFAIL**
    (plait-шум `No IPs matched by pl*-*-sg_onecloud-dzen_vmagent... cannot update: Not
    enough running replicas`) → terminate + закрыта.
- **Десинк для ретрая**: облако/PMS впереди БД — 2.controller.hc есть в облаке и кворуме,
  host_state save не прошёл. Ретрай операции сходится (child скипает достигнутое, reload
  идемпотентен, save доедет).
- pc-брокер: хост жив (systemctl active, up 69d, load ~20), журнал хоста начинается
  13:37 (перезагрузка во время сбоя pc-мастера). PREFAIL залип — cleared только облаком.

### test-modify4 (3fb46c41) — основной полигон, операция зависла
- Seed перезалит из прода после завершения прод-modify (brokers kc/pc/rc, controllers
  hc/kc/pc, cruise hc). Старая упавшая по квоте операция закрыта.
- Операция `07a21cb7` (upscale kc, D1-шаг1): 2.controller.kc задеплоен ✓, фантомный
  voter 11002@2.controller.kc (хвост квота-фейла) «оживлён» ✓, pc-брокер перезагружен ✓,
  но с 14:36 reload **завис**: kc и rc-брокеры стали PREFAIL (та же plait-эпидемия).
  Операция осталась RUNNING (зависнет до TTL 3ч) — перед продолжением: terminate UI-API
  + `UPDATE operations SET status='done' WHERE id='07a21cb7-b0bf-479b-9984-7130648d397a'`.
- **Десинк для ретрая**: аналогично downgrade7 — облако/PMS впереди host_state.

## Главные находки сессии

1. **Регрессия MDBSUP-4939 — найдена и исправлена в коде** (не закоммичено): в
   `reloadBrokersAndControllers` кворум читался ПОСЛЕ `upsertPms` → карта nodeId→host без
   удаляемых → миграция лидера мертва → гарантированный LEADER_NOT_FOUND_IN_QUORUM при
   лидере на удаляемом хосте (= прод `efabd6aa`, кейс MDBSUP-4939). Фикс: кворум читается
   в main ДО чистки и передаётся параметром. Тесты/checkstyle зелёные, дока обновлена.
   ⚠️ mdb-processing ещё работает на коде ДО фикса — перезапустить перед D-даунскейлом
   (и terminate зависших workflows ДО рестарта — изменение порядка activity → nondeterminism
   при replay живых workflow).
2. **Reload-цикл висит вечно на PREFAIL** (живое подтверждение открытого MDBSUP-4938):
   `KafkaIteratePolicy` скипает PREFAIL-availability → takeNext вечно пуст → цикл
   поллингует до workflowTtl. Задело ОБА кластера (pc → затем kc/rc). Кандидат на фикс —
   скип с warn по образцу MDBDEV-2375 (offline-фильтр): PREFAIL-хост исключается из
   reload-цикла, конфиг применится при следующем старте.
3. plait-шум `dzen_vmagent: not enough replicas` → PREFAIL расползается между ДЦ
   (модуль zinfra, не MDB); живость хоста смотреть по state/ip/minion (см.
   mcc-host-worker/commands/query.md).
4. Грабли: `mcc instances` лагает (пусто на живые инстансы) — верить sshexec/FQDN-запросу
   `mcc --local -n infra -c <dc> instances "<FQDN>" -f yaml`; прод-туннель 53480 умирает —
   переподнимать `mcc tp-port-forward 1.db.mdb-etp-pgsql.pc.wan.idzn.ru:7432 --local-port 53480`.
5. mdb-data валидация: downscale блокируется при total controllers < 3 («min value of
   controllers is 3») → для D-сценариев нужен кластер ≥4 контроллеров (сначала upscale).

## Прод-сверка кворум-кейсов (по истории jira-mdbsup-solver)

- MDBSUP-5093 (оставшиеся контроллеры со старым voter-списком): покрыто нашим
  `reloadControllers` — `confp --oneshot && restart && rscheck restart` (KafkaCommand).
- MDBSUP-5044 (фантомный voter): покрыто рестартом лидера с wipe metadata.
- MDBSUP-4939 (лидер на выводимом): покрыто ТОЛЬКО после фикса из п.1.

## Как продолжить

1. Разблокировать modify4: terminate `07a21cb7…` (+детей) через UI-API, закрыть операцию.
2. Дождаться/починить plait (или применить фикс-скип PREFAIL из п.2).
3. Рестарт mdb-processing (код с 4939-фиксом).
4. Ретрай upscale modify4 → COMPLETED (save доедет, host_state = 4 контроллера).
5. **D1 downscale**: `DELETE …/hosts/controllers?dc=kc` → полный прогон нового флоу
   (миграция при лидере на kc, reload, child rescale, save) + чек-лист верификации.
6. downgrade7 — после очистки pc-PREFAIL: ретрай upscale → затем D2/D9-варианты.
