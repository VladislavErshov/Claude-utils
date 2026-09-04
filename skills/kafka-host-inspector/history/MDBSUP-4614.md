# MDBSUP-4614 — Cruise Control NotEnoughValidWindowsException из-за одного брокера без BROKER_CPU_UTIL

**Кластер:** `onemekafkaauth38-oneme-kafka` (hc)
**CC-хост:** `1.cruise.onemekafkaauth38-oneme-kafka.hc.one-infra.ru`
**Версия CC:** 2.5.141
**Дата:** 2026-08-18

## Симптом

`POST /remove_broker` падает:
```
NotEnoughValidWindowsException: There are only 0 valid windows when aggregating in range [-1, 1787058624350]
for aggregation options (minValidEntityRatio=0.95, minValidEntityGroupRatio=0.00,
minValidWindows=1, numEntitiesToInclude=3151, granularity=ENTITY)
```

## Корень

Один брокер — `broker.id=21001` — не отдаёт метрику `BROKER_CPU_UTIL`. CC пропускает **все
партиционные метрик-семплы этого брокера** (350 шт.), и покрытие партиций падает ниже порога:

| Метрика | Значение |
|---|---|
| Total partition assigned | 3151 |
| Collected partition samples | 2801 (88.8%) |
| Skipped на broker 21001 | 350 |
| Порог `minValidEntityRatio` | 0.95 (95%) |

88.8% < 95% → все окна invalid → `0 valid windows` → `/remove_broker` не работает.

Маркер в `/mnt/logs/dbms/cruise-control.out.log`:
```
WARN Skip generating metric sample for broker 21001 because the following required metrics are missing [BROKER_CPU_UTIL].
Generated 2801(350 skipped by broker {21001=350}) partition metric samples and 11(1 skipped) broker metric samples
```

**Важно:** на broker-level (11 broker samples) всё ок — 21001 broker metric samples шлёт. Проблема
только в `BROKER_CPU_UTIL`, который `CruiseControlMetricsReporter` считает сам через
`OperatingSystemMXBean` / `/proc` и не может отдать. В **Porto-контейнере** (MDB Kafka) это часто
ломается: репортёр не видит корректный cgroup, либо процесс только что стартовал и не накопил
CPU-семпл.

## Что делать

1. **Найти хост брокера 21001** через CC state:
   ```bash
   curl -s 'http://1.cruise.onemekafkaauth38-oneme-kafka.hc.one-infra.ru:9000/kafkacruisecontrol/kafka_cluster_state' \
     | jq '.KafkaClusterState.Brokers[] | select(.Id==21001)'
   ```
2. **Логи репортёра на брокере 21001**:
   ```bash
   mcc --local sshexec -n infra <broker_21001_host> \
     "grep -iE 'CruiseControlMetrics|BROKER_CPU' /mnt/logs/dbms/kafka-broker.out.log | tail -30"
   ```
3. **Быстрый обход** — опустить порог (рискованно, решение на неполных данных):
   ```
   POST /remove_broker?brokerid=...&min_valid_partition_ratio=0.85&min_valid_windows=0
   ```
4. **Правильный фикс** — разобраться, почему `CruiseControlMetricsReporter` на 21001 не отдаёт
   `BROKER_CPU_UTIL`. Часто помогает перезапуск брокера или проверка конфигурации репортёра в
   `broker.properties`.

## Как применять

При жалобах на `NotEnoughValidWindowsException` / `0 valid windows` на любом MDB Kafka CC —
**сразу** грепать лог CC на `Skip generating metric sample for broker X because the following
required metrics are missing`. Один битый брокер ломает весь кластер, не надо копать 11 брокеров —
виновник написан в WARN-строке.

```bash
mcc --local sshexec -n infra 1.cruise.<cluster>.<dc>.one-infra.ru \
  "grep -iE 'skip.*metric.*missing|skipped by broker' /mnt/logs/dbms/cruise-control.out.log | tail -20"
```

Этот инцидент хостового уровня — диагностика ведётся через [`kafka-host-inspector`](../SKILL.md)
(подключение к CC-хосту, пути к логам). Для контекста работы CC в кластере см. родительский
скилл [`kafka-cluster-inspector`](../../kafka-cluster-inspector/SKILL.md)
(`commands/cruise_control_ops.md`, `commands/known_issues.md`).
