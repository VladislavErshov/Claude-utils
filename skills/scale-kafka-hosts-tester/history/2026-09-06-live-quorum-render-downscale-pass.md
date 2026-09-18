# 2026-09-06: D-сценарии на новом коде — live-рендер карты кворума (MR !440, фикс замечания #2)

## Что тестировалось

Новая логика `DownscaleKafkaControllerInClusterWorkflowImpl` (коммиты поверх `a1f6eaf5`,
рабочий коммит до пуша):
1. **`KafkaHostActivity.getControllerQuorumSsh`** — карта nodeId→host строится ТОЛЬКО по
   живому рендеру `controller.quorum.voters` (`sed -n 's/^controller.quorum.voters=//p'
   /opt/kafka/config/controller.properties`, комманда `READ_CONTROLLER_QUORUM_VOTERS`)
   с одного из оставшихся контроллеров. PMS `getControllerQuorum` из флоу убран.
2. **Миграция лидера откатена на классику**: рестарт ТОЛЬКО лидера на удаляемом хосте
   (+ `waitLeaderMigrated`); unmapped-лидер → скип. `restartAliveRemovedHosts` удалён.

Инфра: локальный Temporal (8233) + mdb-processing local (8080, новый код) + mdb-data
(8081, branch MDBDEV-3180). Брокеры :9092 доступны с ноутбука НАПРЯМУЮ через VPN
(проверять `nc -z`, НЕ `timeout bash -c` — на macOS нет `timeout`, давал ложные closed).
Port-forward не нужен.

## Прогоны (кластер test-modify3 `9fc47c1b`, лидеры по kafka-metadata-quorum describe)

| # | workflowId | Сценарий | Результат |
|---|---|---|---|
| 1 | `8e7bcbc6` | DELETE dc=hc при [dc:2, hc:1, kc:1], лидер 11001@hc на удаляемом | **PASS**: history = `removeControllerFromQuorum` → **`kafka_host_getControllerQuorumSsh`** → `getLeaderId` → **один** `restartControllerInstanceSsh` (лидер) → waitMigrated → reload → restore → дети (hc=withdraw) → save. Ноль фейлов. Итог: voters [10001,10002,12001], PMS=рендер=БД |
| 2 | `d43ec655` | DELETE dc=dc при [dc:2, hc:1, kc:1], лидер 10002@2.controller.dc на удаляемом | **PASS**: миграция лидера сошлась, dc:2→1, консистентность 3/3/3 |
| 3 | `89d4dbfa` | DELETE dc=hc при [dc:1, hc:2, kc:1], лидер 10001@dc НЕ удаляемый | **PASS**: миграция скипнута корректно, removed 11002, converge |
| 4 | `0aad9d7f` | **Kill-воркера на +5с** (окно: quorum-done +3.2с, reload +7.6с): PMS почищен, рендер ещё полный | **PASS**: PMS без 11002 И рендер с 11002 подтверждены после kill; после рестарта воркера workflow возобновился, карта из живого рендера, рестарты оставшихся + restore лидера + save → COMPLETED; финал 3/3/3 |

## Замечания к гонке terminate (не успевалось)

- Окно `removeControllerFromQuorum` → `updateConfigKafkaBroker` ≈ **4.4с** (3.2с → 7.6с от
  старта). Поллинг истории локального Temporal с латентностью ~1с не ловит: сначала
  ищешь парент среди Running (там же ребёнок `_reconcile-cluster`, фильтровать по
  `type.name == downscaleKafkaControllerInCluster`). Надёжный способ деградации —
  **kill -9 воркера на T0+5с**: workflow остаётся RUNNING, состояние ровно между
  «PMS вычищен» и «reload начат».

## Проверки после прогонов

- KRaft: `kafka-metadata-quorum.sh --command-config` (⚠️ у этой утилиты `--command-config`,
  у `kafka-leader-election.sh` — `--admin.config`; полный путь `/opt/kafka/bin/` — в sshexec
  PATH нет; скрипт с pipe (`| grep`) падает OCI runtime error — писать в /tmp и cat).
- PMS (`pms-read.sh`), `host_state`, рендер на контроллерах — везде идентичный кворум.
- Vault-секреты `zkv/mdb/mdbdev/kafka/test-modify3-mdbdev-kafka.mdbdev.db.production.mdb.prod/*`
  в локальном `mdb-processing-vault` — живы с прошлых сессий.
- Финал сессии: кластер [dc:1, hc:1, kc:1], лидер 11001@hc, лаг 0, 3 контроллера
  (минимум для кворума; mdb-data guard «min value of controllers is 3» — срезать до 2 нельзя).

## Статус замечаний MR !440

- #2 (High, ретрай после чистки PMS) — закрыт кодом + подтверждён прогонами 1, 2, 4
  (ключевой — 4: деградация ровно та, что в замечании; ретрай сходитcя по живому рендеру).
- #1 (High, wire-формат) — ответ отправлен (обоснование, без compat-пути).
- #3 (Medium, docs) — закрыт секцией «Совместимость и порядок релиза».
