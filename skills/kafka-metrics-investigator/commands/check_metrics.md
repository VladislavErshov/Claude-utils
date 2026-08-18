# Проверка метрик на Kafka-брокере

Kafka-брокер отдаёт метрики через **четыре** независимых exporter'а на разных портах.
Если в Grafana пропали метрики — нужно проверить каждый порт отдельно.

Доступ к хосту — через скилл [`mcc-host-access`](../../mcc-host-access/SKILL.md) (`mcc ssh`).
Здесь — только специфика проверки метрик.

## Порты

| Порт | Что отдаёт | Кто запущен | Метрики в Grafana |
|---|---|---|---|
| **8080** | JMX exporter (jmx_prometheus_javaagent) | Внутри JVM Kafka broker, через `-javaagent` в `KAFKA_OPTS` | `jvm_info`, `kafka_server_*` (Broker up, In-Flight, share group metrics), `kafka_controller_*`, `kafka_log_*`, `kafka_network_*` |
| **7777** | Jolokia (JMX-HTTP bridge) | Внутри JVM Kafka broker, через `-javaagent` в `KAFKA_OPTS` | Используется rscheck / host_checker, не Prometheus напрямую |
| **23569** | kafka-exporter (danielqsj, Go) | Отдельный systemd-юнит `kafka-exporter.service` | `kafka_consumergroup_lag`, `kafka_consumergroup_current_offset`, `kafka_topic_partition_current_offset` |
| **23570** | share-group-lag-exporter (Python) | Отдельный systemd-юнит `share-group-lag-exporter.service` | `kafka_share_group_lag`, `kafka_share_group_start_offset`, `kafka_share_group_exporter_last_success_timestamp_seconds` |

JMX (8080) — основной источник метрик. Если он мёртв — пропадает большинство дашборда.

## Быстрая проверка всех портов

Зайти на хост через скилл [`mcc-host-access`](../../mcc-host-access/SKILL.md) (`mcc ssh`)
и выполнить:

```bash
for p in 8080 7777 23569 23570; do
  printf "port %s: " $p
  curl -s -o /dev/null -w "%{http_code}\n" --max-time 10 localhost:$p/metrics || echo fail
done
```

**Важно:** `--max-time 10` минимум! JMX exporter (8080) отдаёт ~674KB, при `--max-time 5` curl не успевает скачать и возвращает `000` — выглядит как мёртвый, но на самом деле жив.

Ожидаемый вывод:
```
port 8080: 200      # JMX — жив
port 7777: 404      # Jolokia — 404 на /metrics это норма (нет корневого пути)
port 23569: 200     # kafka-exporter — жив
port 23570: 200     # share-group-lag-exporter — жив
```

`000` = port не отвечает, сервис упал или не слушает.

## Проверка статуса сервисов

На хосте (через скилл [`mcc-host-access`](../../mcc-host-access/SKILL.md), `mcc ssh`):

```bash
systemctl is-active kafka-broker kafka-exporter share-group-lag-exporter
```

Должно быть `active` для всех трёх.

## Если JMX (8080) мёртв

1. Проверить что Kafka broker запущен и в его команде есть `-javaagent:/opt/prometheus/jars/jmx_prometheus_javaagent-0.19.0.jar=8080:...`:

```bash
ps -ef | grep -i jmx_prometheus | grep -v grep | head -2
```

2. Проверить что порт слушается (через `netstat`, не `ss` — `ss` без root в контейнере может не видеть чужие сокеты):

```bash
netstat -tlnp 2>/dev/null | grep -E ":8080|:9092"
```

Должно быть `LISTEN 1504/java` для 8080 и 9092.

3. Если port не слушается — `jmx_prometheus_javaagent` упал внутри JVM. Смотреть `/mnt/logs/dbms/kafka-broker.err.log` и `.out.log` на предмет `BindException`, `Address already in use`.

4. Если `curl --max-time 5` даёт `000`, а `curl --max-time 10` — `200`, проблема в объёме ответа, не в сервисе. JMX exporter отдаёт ~674KB.

## Если kafka-exporter (23569) мёртв

Частая причина — виснет `pre-start-kafka-exporter.sh` на `kafka-acls.sh --list`. Сервис в статусе `activating (start-pre)` > 90 сек → systemd `TimeoutStartSec` kill → restart → снова виснет.

Проверить:
```bash
systemctl status kafka-exporter --no-pager | head -15
```

Если `Active: activating (start-pre)` — pre-start висит. Смотреть CGroup-дерево:
```bash
systemctl status kafka-exporter --no-pager | grep -A2 CGroup
```

Если там виден процесс `kafka-acls.sh --list --principal User:kafka_exporter` — брокер перегружен,
CLI висит. Рестартнуть брокера: `systemctl restart kafka-broker.service`, затем `systemctl restart kafka-exporter.service`.

## Если share-group-lag-exporter (23570) мёртв

Сервис зависит от `kafka-broker.service` (Wants). Если брокер упал — exporter тоже падает.

Проверить:
```bash
curl -s --max-time 10 localhost:23570/metrics | head -20
tail -10 /mnt/logs/dbms/share-group-lag-exporter.err.log
```

Возможные ошибки в `err.log`:
- `collect error: kafka-share-groups.sh exit=-6: java.lang.instrument ASSERTION FAILED` —
  в env остались `KAFKA_OPTS` с `-javaagent`. Скрипт должен их вычищать через `env.pop`.
- `collect error: kafka-share-groups.sh exit=1: ... TCPEndpoint.newServerSocket` —
  в env остался `JMX_PORT=9000`, `kafka-run-class.sh` пытается открыть RMI на занятом порту.
- `collect error: kafka-share-groups.sh timed out` —
  `kafka-share-groups.sh` не уложился в `SHARE_GROUP_LAG_COLLECT_TIMEOUT` (по умолчанию 90s).
  Брокер перегружен или share group state очень большой.

Метрика `kafka_share_group_exporter_last_collect_error{error=""} 0` — последний collect успешен.
Если `1` — последний collect упал, в лейбле `error="..."` короткая причина.

## Дедупликация лагов в Grafana

`share-group-lag-exporter` запущен на **каждом** брокере кластера (3 ДЦ). Каждый отдаёт одинаковые
значения лагов (состояние share-группы, не инстанса). В Grafana без агрегации будет 3x дубликатов.

Правильные выражения:
- `sum by (group) (max by (group, topic, partition) (kafka_share_group_lag{...}))` — суммарный лаг по группе
- `max by (group, topic, partition) (kafka_share_group_lag{...})` — лаг по партиции

`max by (group, topic, partition)` схлопывает дубликаты с разных инстансов (лаг одинаковый).

## Грабля: `nproc`/`/proc/cpuinfo`/`lscpu` через mcc — это ядра миньона, не хоста Kafka

⚠️ **Команды `nproc`, `lscpu`, `cat /proc/cpuinfo`, `cat /proc/uptime`, `cat /proc/loadavg`,
выполненные через `mcc sshexec`/`mcc ssh` на Kafka-хосте, возвращают ресурсы mcc-миньона
(прокси-узла), а не самого Kafka-хоста.** Это приводит к ложным выводам — например, расчёт
«средний CPU процесса = `process_cpu_seconds_total / uptime`» даст числа, привязанные к
конфигурации миньона (64 ядра), а не реального брокера (часто 8–16 ядер).

**Что НЕ работает для определения числа ядер Kafka-хоста через mcc:**
- `nproc` — возвращает ядра миньона
- `lscpu | grep CPU(s)` — то же
- `cat /proc/cpuinfo | grep -c processor` — то же
- `cat /proc/uptime` — uptime миньона, не Kafka-хоста
- `cat /proc/loadavg` — loadaverage миньона

**Что работает:**
- **JMX-метрика** `process_start_time_seconds` на порту 8080 (только для elapsed-time процесса,
  не для ядер) — но см. ниже граблю со стартовым временем.
- **Prometheus/VictoriaMetrics метрика числа ядер** — обычно `count(count without(cpu, mode)
  (node_cpu_seconds_total{instance=~"$instance"}))` или OneCloud `one_cloud_cpu_cores_value`.
- **mdb-data spec / OneCloud API** — число vCPU из конфигурации кластера (через `mcc instances`
  или PMS-API, см. скилл `kafka-cluster-inspector`).
- **Grafana дашборд** — на панели «CPU Usage» (host-level) используется
  `one_cloud_cpu_percent_value` — значение уже в % от общей мощности хоста, делить на число
  ядер не нужно.

**Правильный расчёт утилизации CPU процесса Kafka в % от хоста:**
```promql
100 * rate(process_cpu_seconds_total{mdb_kafka_cluster="$cluster",instance=~"$instance"}[$__rate_interval])
  / on(instance) group_left()
  count(count without(cpu, mode) (node_cpu_seconds_total{instance=~"$instance"}))
```

## Сопоставление имён графиков Grafana с метриками

Если на дашборде Grafana виден график с человекочитаемым именем (`Kafka broker up`,
`In-Flight requests`, `Share group lag`, `Under-replicated partitions` и т.п.) и нужно
понять, какую именно метрику он рисует и с какого exporter'а/порта приходит — используй
скилл [`/grafana-plot-creator`](../../grafana-plot-creator/SKILL.md).

Сценарии, когда это нужно:
- В Grafana график пустой или ведёт себя странно — надо проверить, что выражение
  ссылается на метрику, которая действительно отдаётся живым exporter'ом (таблица выше).
- Нужно добавить новый график или поправить существующий — правила именования панелей,
  размещения в dashboard JSON и форматов PromQL описаны в скилле `grafana-plot-creator`.
- Имя панели на дашборде не совпадает с именем метрики (например, `Kafka broker up`
  → `kafka_server_kafkaserver_brokerstate` или `up` для scrape-таргета) — сверка
  имени и выражения идёт через `grafana-plot-creator`, а проверка живости самой
  метрики на хосте — через этот скилл.

## Что НЕ покрывает скилл

- Throughput / latency — к Prometheus/Grafana напрямую, не к хосту.
- Настройка scrape-конфигов Prometheus — это в OneCloud spec (`prometheus_metrics_cfg`, `prometheus_labels`).
- Топики / ACL — через `kafka-topics.sh` / `kafka-acls.sh` (с `unset KAFKA_OPTS JMX_PORT`).
