# MDBSUP-5485 — frontlogs2-adtech-kafka: remove_broker падает, брокер 21004 не отдаёт BROKER_CPU_UTIL (битый миньон srvk6086, вылечено migrate)

**Дата**: 2026-09-16
**Кластер**: `frontlogs2-adtech-kafka` (`a12b488e-3584-4a0e-bdf5-983be4e29ed0`), 25 брокеров (hc/kc/pc/rc/uc × 5), cruise в rc
**Тикет**: MDBSUP-5485 (remove_broker пользователя блокирован)

## Симптом

`POST /remove_broker` падает `NotEnoughValidWindowsException` (0 valid windows, minValidEntityRatio=0.95);
покрытие сэмплов 419/444 ≈ 0.943 < 0.95; в CC-логе «Skipping proposal precomputing because load monitor
does not have enough snapshots» каждые 30 с; 25 партиций skipped by broker 21004.

## Диагностика (подтверждение паттерна MDBSUP-5237/MDBSUP-4614/onemekafkaauth38)

| Шаг | Результат |
|---|---|
| Маппинг ID | `kafka-broker-api-versions.sh` с брокера hc-1 (FQDN + `--command-config /opt/kafka/config/client.properties`, полный путь `/opt/kafka/bin/...`): **21004 = 4.broker.frontlogs2-adtech-kafka.kc.one-infra.ru** |
| Лог брокера kc-4 | `Failed reporting CPU util` каждую минуту, 1393 в текущем логе; в архивах минимум с **02.09** (хроника, не разовое); пережил рестарт брокера 15.09 17:57 |
| Jolokia kc-4 | `java.lang:type=OperatingSystem` → `ProcessCpuLoad=-1.0, SystemCpuLoad=-1.0` |
| Jolokia kc-1 (сосед) | `0.0 / 0.0` — образ/JVM/ядро идентичны → проблема миньона |
| cgroup на kc-4 | `/sys/fs/cgroup/cpu,cpuacct` смонтирован из `/porto%4.broker.../pids-prod/libpod-...`, `/proc/self/cgroup` указывает в другое место → JDK cgroup v1 не находит `cpu.cfs_quota_us` (=-1, unlimited) → CPU load недоступен |
| CC | рестарт 15.09 18:05, >24 ч с 0 валидных окон → не прогрев (отличие от MDBSUP-4761) |
| Миньон | **srvk6086**, uptime 646 дней |

## Фикс

```bash
# валидация имени (упадёт на уравнении c mod — норм):
mcc --local --cloud kc -n infra migrate --relocate "frontlogs2-adtech-kafka.adtech.db.production.mdb.prod/broker/4"
# запуск:
mcc --local --cloud kc -n infra migrate --relocate --auto_solve "frontlogs2-adtech-kafka.adtech.db.production.mdb.prod/broker/4"
```

Тайминг: запрос → контейнер `FINISHED/PREEMPTED` (~3 мин) → контейнер RUNNING + `kafka-broker` active
на **srvk4365** (~8 мин от запроса). Jolokia сразу `ProcessCpuLoad=0.0, SystemCpuLoad=0.0`,
ошибок репортера в новом логе 0. CC собрал 5/5 окон (coverage 100%, 444/444 партиций,
`isProposalReady=true`) — remove_broker разблокирован в тот же час.

## Грабли / уроки

1. `mcc instances '*<cluster>*'` на старте переезда теряет инстанс (пустой grep) — состояние
   смотреть повторным вызовом или sshexec'ом.
2. curl к CC REST (порт 9000) с самого cruise-хоста даёт HTTP=000 даже на 127.0.0.1 — состояние
   CC надёжнее читать из `operationLogger`-строк `/state` в `cruise-control.out.log` (grep по `windows`).
3. В `recentBrokerFailures` CC остаётся аномалия «21004 failed» в статусе `CHECK_WITH_DELAY`
   (self-healing выключен) — не блокирует remove_broker, рассосалась сама.
4. Хранение: `/opt/kafka/bin/...` полный путь — `kafka-*-*.sh` не в PATH; grep без совпадений →
   `OCI runtime error` (не ошибка сессии).

## Продолжение (вечер того же дня, 16.09)

В 17:52 и 17:56 (до апдейта CC) две попытки `POST /remove_broker` **разом на все IC-брокеры
21001–21005** (dryrun=false, с 127.0.0.1 — через mdb-data/processing) упали
`NotEnoughValidWindowsException` (0 valid windows), хотя `/state` показывал 5/5 окон:
параллельно CC ловил OOM (21 вхождение в `cruise-control.err.log`: qtp/Jetty-треды,
ConcurrencyAdjuster-1, GoalOptimizerExecutor-0) — OOM подсёк load monitor → 0 валидных окон.

Апдейт (новый образ + конфиги, heap `-Xms/-Xmx 4096m`), рестарт CC 18:01:14 → через ~25 мин
снова 5/5 окон, coverage 100% (444/444), isProposalReady=true, executor idle. Брокер 21004
после migrate перезапускался 16:38, метрика CPU в норме, `Failed reporting CPU util` = 0.

Урок: OOM в CC может давать «0 valid windows» при внешне здоровом `/state` — при таком
расхождении сразу grep'ать `OutOfMemoryError` в err.log и смотреть недавние remove_broker
(удаление 5 брокеров разом — тяжёлый план для heap).
