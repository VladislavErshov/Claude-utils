# trg-195060-dwh-datatransfer-kafka — CC «Proposals are not ready», coverage 45%

Дата: 2026-09-03. Кластер `trg-195060-dwh-datatransfer-kafka` (prod, datatransfer,
queue `...datatransfer.db.production.mdb.prod`): по **1 брокеру в kc/pc/uc**
(1.broker.<dc>.one-infra.ru), CC — `1.cruise.trg-195060-dwh-datatransfer-kafka.kc.one-infra.ru`.
ID брокеров не по стандартной таблице скилла: kc=21001, pc=22001.

## Симптом

Нет proposals. `GET /kafkacruisecontrol/state?json=true` (localhost:8080 на cruise-хосте):

- `isProposalReady=false`, `trained=false`, `trainingPct=20.0`
- **`monitoringCoveragePct=45.16`** (нужно ≥95%), `numValidPartitions=83` из `numTotalPartitions=186`
- `numMonitoredWindows=5` (окна формально копятся)
- ready только 3 безметричных goals: RackAwareDistribution, TopicReplicaDistribution, LeaderReplicaDistribution
- ExecutorState `NO_TASK_IN_PROGRESS`; AnomalyDetector жив, метрики по брокеру 22001 (pc) текут

Классическая картина из `commands/cruise_control_ops.md` «Не идут метрики от брокеров»:
метрики в `__CruiseControlMetrics` идут не со всех брокеров → CC не добирает покрытие.

## Диагностика

1. `cruise-control.service` active, CC-стейт `RUNNING`; `cruise-control.err.log` чистый
   (только gson illegal-access warnings, не ошибка).
2. Все 3 брокера `kafka-broker.service` active (uptime с 2026-08-27 13:20-13:29 MSK).
3. Конфиг на брокерах в порядке: `broker.properties` содержит полный блок
   `metric.reporters=CruiseControlMetricsReporter` + `cruise.control.metrics.*`
   (bootstrap.servers=FQDN:9092, SASL_PLAIN, auto.create, 9 партиций, rf=2);
   отрендерен 2026-08-27 13:20. Jar `cruise-control-metrics-reporter-2.5.141.jar` на месте.
   Версии совместимы (кейс known_issues 2.5.141-vs-Kafka4.x не наш — подтверждено владельцем).
4. Нюанс: на момент проверки в текущем `kafka-broker.out.log` НЕ было строки
   `Starting Cruise Control metrics reporter ...` и ошибок reporter'а, при этом брокеры
   стояли с 27.08 — репортер после августовского рестарта фактически не поднимался.

## Что сделали

Рестарты брокеров владелец выполнял сам (поочерёдно), агент — только диагностика и история:

- 2026-09-03 17:42 MSK — перезапущен `1.broker...kc`: в out.log
  `Successfully registered broker 21001` + `Kafka Server started`; после рестарта
  в `kafka-broker.out.log` пошли упоминания `CruiseControlMetricsReporter` (>10k строк) —
  репортер поднялся. Далее аналогично pc, uc.

## Проверка после рестартов (~30 мин на 5 окон × 5 мин)

```bash
mcc --local -n infra sshexec 1.cruise.trg-195060-dwh-datatransfer-kafka.kc.one-infra.ru \
  "curl -s 'http://localhost:8080/kafkacruisecontrol/state?json=true' | head -c 600"
# ждём: isProposalReady=true, trained=true, monitoringCoveragePct → ~100
```

Если покрытие не растёт: `cruise-control.err.log` (OOM), `kafka.cruisecontrol.properties`
(bootstrap.servers), маркер на каждом брокере
`grep "Starting Cruise Control metrics reporter" /mnt/logs/dbms/kafka-broker.out.log | tail -1`.

## Грабли

- `mcc instances '*broker.<cluster>*'` **без `-c <dc>`** → `EntityNotFoundException` даже
  при живых хостах; список брокеров собирать по-ДЦ: `mcc --local -n infra -c kc|pc|uc instances ...`.
- Отсутствие строки `Starting Cruise Control metrics reporter` при стоящем неделями брокере —
  достаточный признак молчащего репортера, даже если в логе нет `Connection to node -1`.
