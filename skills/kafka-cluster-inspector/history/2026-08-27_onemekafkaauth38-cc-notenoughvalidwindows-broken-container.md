# NotEnoughValidWindows из-за битого контейнера (JDK cgroup NPE) — лечение mcc migrate

**Дата**: 2026-08-27
**Кластер**: `onemekafkaauth38-oneme-kafka` (9 брокеров ec/kc/pc, KRaft, Kafka 3.8.0, image 2.4.4)
**Хосты**: `1.cruise.<cluster>.ec` (CC), `1.broker.<cluster>.kc` (виновник, brokerId 21001)

## Симптом

`POST /rebalance` (dryrun из mdb-data health-проверки) падает:
`NotEnoughValidWindowsException: There are only 0 valid windows when aggregating in range [-1, ...] for aggregation options (minValidEntityRatio=0.95, numEntitiesToInclude=3151, granularity=ENTITY)`.

CC при этом жив (> суток uptime), конфиг валидный, topic `__CruiseControlMetrics` здоров (9 парт, RF=3, ISR полный).

## Диагностика

| Шаг | Что смотрел | Результат |
|---|---|---|
| `GET /state` | NumValidWindows **0/5**, NumValidPartitions **2800/3151 (88.89% < 95%)** | системно невалидные окна, не разовый тайминг |
| Арифметика | 3151−2800 = 351 ≈ 3151/9 = 350 → **один брокер** | `kafka-topics --describe` → у 21001 ровно 350 лидерских парт |
| Логи брокеров | `grep 'Failed reporting CPU util'` — **только на 1.broker.kc**, каждую минуту с 22.08, пережила рестарт 25.08 | `IOException: Java Virtual Machine recent CPU usage is not available` → репортер пропускает весь цикл отправки |
| Воспроизведение | тривиальная JVM с `getProcessCpuLoad()` на 1.kc vs 2.kc | на 1.kc **NPE в JDK**: `CgroupV1Subsystem.getCpuQuota → CgroupUtil.readStringValue → Paths.get(null)`; на 2.kc работает |
| Корень | `/proc/self/cgroup`: `cpu,cpuacct → .../pids-batch`, а mountinfo root `.../pids-prod/libpod-...` | JDK вычисляет несуществующий путь к `cpu.cfs_quota_us` → null → NPE. Дефект контейнера на миньоне **srvk4455** |

Воркараунд проверен на месте: `java -XX:-UseContainerSupport` чинит чтение CPU (не понадобился — мигрировали).

## Фикс

`mcc migrate --relocate` шарда инстанса на другой миньон (данные сохраняются, контейнер пересоздаётся):

```bash
# найти storage/шард: mcc tool_status --type storage "<queue>/broker" (queue из mcc instances)
# очистка: имена volumes/uuids + minion
mcc --local -n infra -c kc migrate --relocate "onemekafkaauth38-oneme-kafka.oneme.db.dev.mdb.prod/broker/1"
```

Тайминг: volumes BOOTSTRAPPING на новом миньоне (~3 мин, ~9GB used) → контейнер FINISHED→STARTING→RUNNING на srvk6240 →
брокер registered + `Kafka Server started` через ~1 мин. CPU-ошибка исчезла (последняя — до пересоздания).
Через ~30 мин `/state`: NumValidWindows 5/5, NumValidPartitions 3151/3151, isProposalReady=true → rebalance dryrun Completed.

## Грабли / уроки

1. `NotEnoughValidWindows` при живом CC (>30 мин uptime) → смотреть NumValidPartitions: недостающая доля ≈ 1/N_брокеров = ищи молчащий брокер, а не тайминги прогрева (отличие от MDBSUP-4761).
2. Репортер с упавшей CPU-метрикой молчит **целиком** — partition-метрики тоже не идут (continue в run-цикле).
3. Рестарт сервиса НЕ лечит битый cgroup/mountinfo контейнера — только переезд/пересоздание контейнера (migrate или lifecycle).
4. Диагностика JVM-level: однолинейный CpuTest.java на подозрительном и здоровом хосте мгновенно разделяет «битый контейнер» vs «проблема кластера».
5. `grep -c` при 0 совпадений выходит с code 1 → mcc sshexec показывает `OCI runtime error` — это НЕ ошибка хоста.
6. Уравнение mcc бывает с `mod` — pexpect-регексп из lifecycle.md (`[-+*/]`) его не ловит, миграция молча не стартует. Нужен `(mod|[-+*/])`.
7. `/state` NumValidPartitions показывает текущее окно — может отставать, пока окна копятся; главный маркер — растущий NumValidWindows.
