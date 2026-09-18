# MDBSUP-5479 (2026-09-17): dvcp mongo — resize mongos и hidden-реплик (CPU/RAM)

- cluster_id: 706d5117-e824-4333-9e30-795ae07e01c2, кластер `dvcp` (notifications/mongodb),
  sharded: 3 mongos (ic/nc/zc) + 6 шардов (db ×3 ДЦ + hidden в ic) + rscfg ×3 + rscfg-hidden.
- Запрос: mongos 10→20 vCPU (RAM 48G не меняется), hidden shard1-6 →12 vCPU / 32→64G.

## Состояние до работ

- mongos: 10 vCPU / 48G; shard2-6-hidden: 6 / 32G; **shard1-hidden: манифест 16/48G,
  фактически 6/32G** — с 12.09 сервис висел в UPDATING (заявка d.duzhinskiy/12.09 не применялась).
- Операций в mdb-data по кластеру нет (последняя change_primary 26.08, done). Mongo-флоу
  в mdb-processing-temporal отсутствуют — операции гоняет mdb-backend напрямую через
  задачи cloud-ops оператора (тикет `start_mongodb_change_primary_operator` → taskName
  `change-primary`, operatorName `mongodb`, fullQueue из one_cloud_meta).

## Ключевые механизмы (канон)

1. Ресурсы хоста = `alloc` в манифесте сервиса one-cloud (`mcc manifest <svc> -t service`).
   `mcc submit` меняет только demand; применение — при пересоздании таска инстанса
   (кроме CPU-only роста — он применился на живую).
2. UPDATING-ловушка: сервис, чей demand не влезает на минион, зависает в UPDATING
   («cannot fit ... unsatisfied MEM=... ; instance is not movable»), и **любые новые
   submit на него падают с EOF** — даже идентичный манифест. Разблокировка: stop/start,
   migrate или эвакуация.
3. Free-память на минионе смотрится в `mcc status -t storage "...": minion: (... MEM=...G free)`.
   Fit-чек требует влезания полного нового таска рядом со старым (старые 32G не вычитаются).

## Ход работ (что сработало)

1. **mongos (CPU-only)**: `mcc submit` с vcores=20 применился сразу во всех ДЦ без рестарта
   (инстансы остались RUNNING, alloc=20).
2. **hidden**: submit 12/64G → сервисы ушли в UPDATING (на миньонах нет 64G).
   Рецепт применения: `mcc stop <инстанс>` → дождаться FINISHED → `mcc start` → если
   не влезает — мастер сам эвакуирует тома (shard2: ~18 мин) → RUNNING на новом минионе.
3. **Детерминированная миграция**: после stop сразу
   `mcc migrate dvcp-notifications-mongo.notifications.db.production.mdb.prod/shardN-hidden/1 --relocate`
   — переносит все тома шарда (data 300G + backup 600G hdd + logs) на новый минион
   (фактически 5-10 мин, копируется ~46G), затем `mcc start`.
   С работающим таском migrate отвечает EOF — сначала stop.
4. **Параллельность**: миграции разных шардов можно гнать одновременно — hidden одна
   на шард, при её даунтайме в RS остаются 3 db-ноды, кластер доступен (проверено на 4
   параллельных).
5. **shard1 (заклиненный)**: stop → migrate --relocate → start поднял на старой заявке
   16/48G (srvi319); submit 12/64G прошёл (сервис больше не UPDATING); stop → start не
   влез (на минионе осталось ~16G) → повторный migrate --relocate → RUNNING 12/64G на srvi378.

## Не сработало / грабли

- **mongodb.resize у cloud-ops оператора**: стартует (`mcc op_start <queue://...> mongodb.resize
  -- --vcores=.. --mem=.. --lan-in=.. --lan-out=.. --volume=data --size=300g --type=ssd
  --service-for-update-pattern=shard.-hidden`; аргументы строго kebab-case — Missing required
  options подсказывает состав), но виснет в «Waiting for operator refresh»: модель
  mongodb-оператора не fresh, её обновляет mongodb.watch, который для партиции не запущен
  (root.start поднимает только mongodb.availability + mongodb-sharded.watch). Даже со
  стартованным вручную watch (обновляет vault/PMS/ноды) задача не дошла до действий.
- **isFresh() mongodb-оператора**: super.isFresh() && replicaSetHosts.isRefreshed() &&
  instancesCount == actualReplicaCount && все RS-ноды обновлены — какая-то из компонент
  не проходила; разбирать не пришлось (ручной путь быстрее).
- CPU-лимиты на хосте: vcores ↔ cpu.shares = vcores × 1024 × 0.7 (10 ядер = 7168) —
  фактическое применение видно по shares, но проще по `mcc instances` alloc.
- После переездов алерт оператора «PARTIALLY_AVAILABLE / Undefined nodes ... Instance is
  not running» — протухшая модель; лечится `op_stop` + `op_start mongodb-sharded.watch`
  (приём MDBSUP-5053), через ~2 мин → AVAILABLE.

## Итог

- mongos ic/nc/zc: 20 vCPU / 48G (сервисы RUNNING).
- hidden shard1-6: 12 vCPU / 64G, node status HIDDEN, новые миньоны:
  s1→srvi378, s2→srvi3273, s3→srvi2003, s4→srvi412, s5→srvi6026, s6→srvi5591.
- Оператор: Cluster is AVAILABLE, failed hosts нет.
- Остаток: storage prealloc сервисов остались 6/32G (алерты «alloc differs from storage
  prealloc», у mongos — ещё с создания кластера) — не критично, product их не трогал.
- mdb-data UI продолжит показывать старые данные хостов — ресурсы менялись на уровне
  облака, БД/host_state не трогали.
