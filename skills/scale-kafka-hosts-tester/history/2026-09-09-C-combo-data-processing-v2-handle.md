# Связка mdb-data + mdb-processing: downscale брокеров через v2-ручку (2026-09-09)

**Контекст:** B-сценарии (08.09) гоняли processing-флоу напрямую/через старые вызовы. Эта партия —
первая на **новой ручке mdb-data** `DELETE /api/v2/mdb/kafka/clusters/{cid}/hosts/brokers` с
переименованным полем `remainingBrokersPerDc` (MR mdb-data !492, processing master после 654125b5
+ rename+javadoc).

Инфра: локальные mdb-data:8081 (branch MDBDEV-2900, processing-api `master-SNAPSHOT` из
publishToMavenLocal — в nexus 3.60.0 старое имя поля `brokersPerDc`!) + mdb-processing:8080
(master, тот же jar) + temporal:8233 + abc-stub:3000. Кластер test-modify3
`9fc47c1b-011d-4aaa-b411-de5345a0204e`, исходно 1 брокер/ДЦ (pc/kc/hc/ic).

## C-сценарии (все через mdb-data API, НЕ direct-start)

| # | Сценарий | Результат |
|---|---|---|
| C2 | Валидации: `ic:2` при 1 → 400 «must not be greater than current»; `ic:1`==current → 400 «Nothing to downscale»; все 0 → 400 «min value of brokers is 3»; `{}` → 400 «не должно быть пустым» (field=remainingBrokersPerDc) | PASS |
| C1 | Контракт temporal input parent: **`remainingBrokersPerDc`** абсолютные {pc:1,kc:1,hc:1,ic:1}, queueInfo, connectionParams (bootstrap 5 хостов + vault super), isWan=false, TTL 21600с | PASS (wf `f1e73e14`) |
| C3 | Happy path: upscale ic 1→2 (`1bc028c4`, 225с) → downscale → COMPLETED (195с): reassign-child done, `kafka_host_isBrokerDrained` ×1, `unregisterBroker` ×1 (жертва 2.broker.ic), ic-child `rescaleService` `instanceIndexList:[1]` → облако 1.broker RUNNING, операция done | PASS |
| C4 | B13-десинк подтверждён: после COMPLETED `2.broker.ic` остался в host_state (колбэка в processing нет). Internal save → 200, дифф удалил хвост, осталось 4×1.broker | PASS |
| C5 | Ретрай после terminate: terminate parent в reassign/drain (45с от старта) → операция failed → повтор 409 «Already has active or failed operation» → закрыл в БД (`UPDATE operations SET status='done'`) → retry 202 → НОВЫЙ workflow COMPLETED за 60с по B5-пути (run1 успел unregister → retry: describeBrokerIds жертвы не нашёл → reassign/unregister skip → дети довели rescale) | PASS |

## Грабли

1. **serviceAuth на internal-ручке**: токен — JWT (HS256, secret из `mdb.auth.jwt.secret-key`
   local-конфига) с claim `serviceName`, заголовок `Authorization: <jwt>` **БЕЗ `Bearer `-префикса**
   (иначе «Invalid service token»). Строка `mdb.auth.jwt.token` в application-local.yaml
   processing — невалидный JWT (не декодируется), генерил свой python-скриптом.
2. `rg -qE "A|B"` на macOS rg = флаг encoding, не regex — поллинг-цикл не брейкался (безвредно).
3. processing-api `master-SNAPSHOT` обязателен для локальной связки: nexus 3.60.0 не содержит
   `remainingBrokersPerDc` → compileJava падает.

## Выводы для релиза

- Деплой mdb-data строго после processing-релиза с rename; до этого MR !492 не компилируется
  против nexus 3.60.0.
- Internal save работает (дифф/wan/units) — processing-колбэк остаётся единственной дырой
  (задача на следующую итерацию processing, семантика = remaining-контракт контроллеров).
- B13-процедура дежурного до колбэка: curl internal save со списком остающихся
   (raw JWT, см. граблю 1) либо SQL-чистка host_state.

## Сравнение флоу: старый (backstage + one-cloud-ops) vs новый (mdb-data + mdb-processing)

| Аспект | Старый: backstage + one-cloud-ops | Новый: mdb-data + mdb-processing |
|---|---|---|
| Точка входа | Modify-hosts в backstage → цепочка тасков (поллинг PT10S) | `DELETE /api/v2/.../hosts/brokers` → 202 → REST processing → Temporal |
| Формат цели | `--cloud=<dc> --replicas=<N>` — один ДЦ, шаг −1 | `remainingBrokersPerDc` — абсолютные цели, несколько ДЦ за запуск |
| Валидации на границе | Precondition-ы оператора, UI-цепочка без guard'ов | 400 на входе: цель ≤ текущей, «нечего удалять», суммарно ≥3, констрейнты DTO |
| Оркестрация | State machine оператора (goal/precondition/rule, transient-поля) | Deterministic workflow: фазы + параллельные InDc-дети, replay |
| Миграция партиций | `decommissionBrokerById` — встроенный Kafka decommission | `ReassignKafkaPartitionsWorkflow`: round-robin по survivors + guard survivors ≥ max RF ДО изменений |
| Ожидание drain | Поллинг с backoff, без TTL-дедлайна | Поллинг 30с + дедлайн TTL−5мин → `BROKER_NOT_DRAINED` вместо вечного висения |
| Unregister (KRaft) | После rescale | Идемпотентен и выполняется ДО cloud-удаления |
| Cloud-операции | DownscaleMdbReplicasTask (rescale) | InDc-дети: rescale; при цели 0 — stop+withdraw сервиса И стораджа |
| Частичный отказ | Критический таск падает целиком | `PARTIAL_DOWNSCALE_FAILURE`: siblings доводятся, ДЦ добирается ретраем |
| host_state | Удаляется ДО оператора (MODIFY_KAFKA_HOSTS) — фейл = БД впереди облака | Internal `save/downscaled-brokers` (remaining-дифф, @Transactional) ПОСЛЕ успеха; ⚠️ колбэк processing пока не вызывается (B13) |
| connectionUrl | Отдельный таск UPDATE_DB_CONNECTION_URL | Убран — будет строиться напрямую из host_state |
| Идемпотентность ретрая | Повтор оператора по goal-модели | operationId=workflowId — ретрай сходится из любого состояния (B5-скипы) |
| Типизация ошибок | Алерты, нетипизированные | `DOWNSCALE_NOT_ALLOWED` / `REASSIGN_INVALID_TARGET_BROKERS` / `BROKER_NOT_DRAINED` / `PARTIAL_DOWNSCALE_FAILURE` |
| Observability | Логи/алерты оператора | Temporal UI (история фаз, декод input/output), MDC ClusterId/OperationId |
| Прод-эталон | Годы в проде | B1–B17 + C1–C5 локально; прод впереди |

Суть миграции: императивный оператор с правкой БД наперёд → декларативный workflow с пост-фактум
синком БД, батчем по ДЦ, типизированными отказами, идемпотентными ретраями. Дыра нового флоу —
незакрытый save-колбэк (B13): до добавления в processing хвост host_state чистится internal-ручкой.

