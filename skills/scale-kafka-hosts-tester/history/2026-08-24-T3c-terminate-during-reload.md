# T3c Terminate во время reload + рестарт (2026-08-24)

**Результат: PASS** — рестарт после terminate сошёлся к корректному состоянию.

## Ход

1. Цель `{hc:2}` через mdb-data (`POST …/hosts/controllers?dc=hc`) → workflow `4fc9a975-0d4f-45e1-b0c6-9e4d74f0cfd2`.
2. Фазы прошли: discovery → upsertPms×3 → 3 child (hc-деплой rescale 1→2; dc/kc skip) → начался
   reload (`updateConfigKafkaBroker` RUNNING, один `reloadKafkaBrokerInstance` COMPLETED).
3. **Terminate** parent в этой точке → parent + updateConfigKafkaBroker TERMINATED.
   Состояние: hc-хост задеплоен, PMS-кворум уже 5 voters, host_state НЕ сохранён, reload частично.
4. В БД: операция `4fc9a975` failed (блокирует новые — 409 "Already has active or failed operation";
   для теста выставил status='done').
5. Рестарт той же цели: новая операция `2f759ecf` с controllersPerDc `{dc:2(!), hc:2, kc:1}` —
   mdb-data корректно посчитал текущий состав (dc=2 из host_state после T2).

## Рестарт — сошлось

- child: hc — доведён до 2 (мгновенный skip: сервис уже 2 после первой попытки), dc/kc skip.
- Полный флоу заново: upsertPms×3 идемпотентны → children → reload broker+controller → save.
- **host_state**: 5 контроллеров (появился 2.controller…hc), без дублей.
- **PMS quorum**: 5 voters, каждый ровно один раз (порядок: hc-новый перед dc-новым — union-merge
  порядок не детерминирован, но дублей нет).
- Кластер жив: 2.controller…hc и kc-контроллер отвечают (follower), лидер — один из старых.

## Грабли/факты (в общий скилл тоже)

- Terminate через UI API: нужен cookie `_csrf` (сначала GET любого API) + header `X-Csrf-Token`
  + **POST** `…/workflows/{wid}/terminate` c body `{"reason":...}` (не DELETE!).
- `temporal` CLI внутри контейнера mdb-processing-temporal не достучался до 127.0.0.1:7233 — не работает.
- mdb-data 409 «Already has active or failed operation for cluster» блокирует новые операции,
  пока последняя не закрыта (в тестах — ручной UPDATE operations.status='done').
- Downscale локально падает на migrateLeader (нет доступа к :9092) — для T3-целей использовать
  другие ДЦ вместо отката.

## Доп: подготовка vault для downscale (2026-08-24, вечер)

По истории /mdb-local-tester (kafka-controller-downscale-test-modify3-2026-08-13.md):
- Секреты читаются с broker-хоста: /root/.vault-token + VAULT_ADDR=https://pc.vault.infra.one-infra.ru,
  KV v2 путь с /data/ (без него permission denied): /v1/zkv/data/mdb/mdbdev/kafka/<queue>/<secret>.
- super совпадает с user_super в /opt/kafka/config/jaas.conf (быстрая самопроверка).
- Залиты в локальный mdb-processing-vault: super, keystore-password, truststore-password (kv v2).
- ~/.mccloud/kafka-tls-ca.crt уже на месте (PEM CA с прод-брокера).
- ⚠️ app.kafka.namespaces.infra.ssl-truststore-location в application-local.yaml пока НЕ прописан
  (поле пустое в application.yaml) — прописать перед downscale-тестом, затем revert перед коммитом.
- Осталось проверить сетевой доступ к :9092 (mcc tp-port-forward) — без него migrateLeader всё равно упадёт.

## Итог: downscale hc заWORKал локально (2026-08-24, ночь)

После заливки vault-секретов + PEM CA + `ssl-truststore-location` в application-local.yaml
и рестарта mdb-processing:
- `DELETE …/hosts/controllers?dc=hc` → 202 → workflow `79280c8c…` **COMPLETED** (migrateLeader прошёл —
  KafkaAdminClient достучался до :9092, tp-port-forward не понадобился).
- host_state: 2.controller…hc удалён, осталось 4 контроллера (dc×2, hc×1, kc×1).
- PMS quorum: 4 voters (11002@2.controller…hc убран).
- Кластер жив: лидер мигрировал на 1.controller…hc, остальные follower.
- Сетевой доступ к 9092 был и раньше — падал только на SASL_SSL/PEM (пустой truststore).

Вывод: для downscale локально нужны ТОЛЬКО vault-секреты + PEM truststore (сеть до 9092 открыта).
Рабочая правка application-local.yaml (mdb-data:8081 + ssl-truststore-location) — LOCAL-ONLY, revert перед коммитом.
