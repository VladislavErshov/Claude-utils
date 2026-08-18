---
name: kafka-metrics-investigator
description: Инспекция метрик MDB Kafka-брокеров — проверка четырёх exporter'ов на разных портах (8080 JMX, 7777 Jolokia, 23569 kafka-exporter, 23570 share-group-lag-exporter), чтение Jolokia JMX MBean'ов (BrokerState, FencedBrokerCount, UnderReplicatedPartitions и т.п.), разница Kafka 3.x vs 4.x в MBean'ах, диагностика "Broker is dead" в UI mdb-data через Jolokia. Список хостов задаёт пользователь (формат 1.broker.<cluster>.<dc>.one-infra.ru / 1.controller.<cluster>.<dc>.one-infra.ru). Используй когда нужно проверить живость exporter'ов, прочитать конкретный MBean, разобраться почему rscheck/host_checker помечает брокера как dead, или найти причину пропадания метрик в Grafana.
allowed-tools: [Bash, Read, Write, Edit, Grep, Glob]
---

# Скилл инспекции метрик MDB Kafka

Скилл для проверки метрик-экспортеров и Jolokia MBean'ов на Kafka-брокерах под управлением mdb-data.

Отвечает на вопросы:
- Живы ли exporter'ы (JMX, Jolokia, kafka-exporter, share-group-lag-exporter)?
- Какой MBean отсутствует, из-за чего rscheck/host_checker падает → "Broker is dead" в UI?
- Какие MBean'ы доступны на broker vs controller хосте в Kafka 3.x vs 4.x?
- Почему пропали метрики в Grafana (какой порт упал)?

⚠️ Скилл проверяет **только метрики и MBean'ы**. Он НЕ проверяет: логи broker/controller/cruise
(см. `kafka-cluster-inspector`), KRaft quorum, конфиги, rscheck-скрипты целиком, throughput/latency
(к Prometheus/Grafana напрямую).

> Доступ к хостам и грабли Tcl/SSL/Namespace — в скилле
> [`mcc-host-access`](../mcc-host-access/SKILL.md). Ниже — только специфика метрик.

## Документация

- https://docs.vk.team/mdb/docs/kafka/kafka-intro.html — введение
- https://docs.vk.team/mdb/docs/kafka/kafka.html — детали

Доки лежат в соседнем репо `mdb-docs`.

## Архитектура метрик

Kafka-брокер отдаёт метрики через **четыре** независимых exporter'а на разных портах:

| Порт | Что отдаёт | Кто запущен |
|---|---|---|
| **8080** | JMX exporter (jmx_prometheus_javaagent) | Внутри JVM Kafka broker, через `-javaagent` в `KAFKA_OPTS` |
| **7777** | Jolokia (JMX-HTTP bridge) | Внутри JVM Kafka broker, через `-javaagent` в `KAFKA_OPTS` |
| **23569** | kafka-exporter (danielqsj, Go) | Отдельный systemd-юнит `kafka-exporter.service` |
| **23570** | share-group-lag-exporter (Python) | Отдельный systemd-юнит `share-group-lag-exporter.service` |

JMX (8080) — основной источник метрик для Grafana. Jolokia (7777) — используется rscheck /
host_checker для проверки состояния брокера, не Prometheus напрямую.

Подробности по каждому порту — `commands/check_metrics.md`.

## Формат хостов

```
{1,2,3,...}.broker.<cluster>.<dc>.one-infra.ru     — Kafka broker
{1,2,3,...}.controller.<cluster>.<dc>.one-infra.ru — KRaft controller
```

Пользователь даёт список хостов. Скилл не угадывает хосты — только работает с тем, что дал юзер.

## Что нужно

- **Доступ к хостам** — через скилл [`mcc-host-access`](../mcc-host-access/SKILL.md).
  Специфика метрик — `commands/check_metrics.md`.
- **Сопоставление имён графиков Grafana с метриками** — через скилл
  [`grafana-plot-creator`](../grafana-plot-creator/SKILL.md). Если на дашборде Grafana
  виден график с непонятным именем (`Kafka broker up`, `In-Flight requests`, `Share group lag`
  и т.п.) и нужно понять, какую метрику он рисует и с какого порта/exporter'а приходит —
  открывай `/grafana-plot-creator` и сверяй имя панели с Prometheus-выражением и списком
  метрик из таблицы в `commands/check_metrics.md`.

## Структура скилла

- `SKILL.md` — этот файл.
- `commands/check_metrics.md` — проверка метрик на 4 портах (8080 JMX, 7777 Jolokia, 23569 kafka-exporter, 23570 share-group-lag-exporter), дедупликация лагов в Grafana.
- `commands/jolokia_inspect.md` — Jolokia MBean'ы, разница Kafka 3.x vs 4.x, Kafka CLI.
- `commands/diagnose_broker_dead.md` — пошаговый разбор "Broker is dead" (через Jolokia MBean'ы).
- `history/` — разобранные кейсы из продакшена. Полезно перед диагностикой смотреть, не похож ли случай на уже разобранный.

## Ключевые MBean'ы (кратко)

Подробности — `commands/jolokia_inspect.md`.

### На broker-хосте (process.roles=broker)

| MBean | Что показывает |
|---|---|
| `kafka.server:name=BrokerState,type=KafkaServer` | Состояние брокера: 0=NotRunning, 1=Starting, 2=Recovery, **3=Running**, 6=PendingControlledShutdown, 7=BrokerShuttingDown |
| `kafka.server:name=CurrentControllerId,type=MetadataLoader` | ID текущего активного controller'а |
| `kafka.server:type=ReplicaManager,name=UnderReplicatedPartitions` | Кол-во under-replicated партиций (должно быть 0) |
| `kafka.server:type=ReplicaManager,name=UnderMinIsrPartitionCount` | Кол-во партиций с ISR < min.insync.replicas |

### На controller-хосте (process.roles=controller)

| MBean | Что показывает |
|---|---|
| `kafka.controller:name=ActiveControllerCount,type=KafkaController` | 1 на активном controller'е, 0 на остальных |
| `kafka.controller:name=FencedBrokerCount,type=KafkaController` | Кол-во fenced брокеров (должно быть 0) |
| `kafka.controller:name=OfflinePartitionsCount,type=KafkaController` | Оффлайн партиции (должно быть 0) |

## Известные проблемы (кратко)

Подробности — `commands/diagnose_broker_dead.md` и `commands/check_metrics.md`.

- **"Broker is dead" в UI** — rscheck/host_checker падает на MBean `kafka.server:type=raft-metrics/current-state`,
  удалённом в Kafka 4.x. Фикс — `kafka.server:name=BrokerState,type=KafkaServer`. Разбор — `diagnose_broker_dead.md`.
- **JMX (8080) мёртв** — `jmx_prometheus_javaagent` упал внутри JVM. Смотреть `kafka-broker.err.log` на `BindException`.
- **kafka-exporter (23569) виснет на pre-start** — `pre-start-kafka-exporter.sh` виснет на `kafka-acls.sh --list`. Рестартнуть брокера.
- **share-group-lag-exporter (23570) падает** — в env остались `KAFKA_OPTS` с `-javaagent` или `JMX_PORT=9000`.
- **Fenced брокер** — `FencedBrokerCount > 0` на controller-хосте.
- **Under-replicated partitions** — `UnderReplicatedPartitions > 0` на broker-хосте.
- **Высокий CPU процесса Kafka (`process_cpu_seconds_total`)** — может быть вызван не самой Kafka,
  а TOS agent (javaagent observability): у него были утечки памяти → частый GC → GC выжирал
  все ядра. Подробности и диагностика — `commands/check_metrics.md`.

## Что НЕ покрывает скилл

- Логи broker/controller/cruise-control — см. скилл `kafka-cluster-inspector`.
- KRaft quorum / controller registration — см. скилл `kafka-cluster-inspector`.
- Конфиги Kafka / rscheck / host_checker — см. скиллы `kafka-cluster-inspector`, `kafka-config-inspector`.
- Throughput / latency / performance — к Prometheus/Grafana напрямую.
- Настройка scrape-конфигов Prometheus — это в OneCloud spec (`prometheus_metrics_cfg`, `prometheus_labels`).
