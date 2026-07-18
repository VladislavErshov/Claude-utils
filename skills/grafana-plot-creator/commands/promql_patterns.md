# PromQL-рецепты для Kafka-метрик

Паттерны PromQL для MDB Kafka-дашборда. Источник данных — VictoriaMetrics.

## Переменные дашборда

Использовать во всех PromQL:

- `$cluster` — `mdb_kafka_cluster` label (например `test-43version-4-mdbdev-kafka`)
- `$instance` — regex для `instance` label (например `1.broker.test-43version-4-mdbdev-kafka.hc.one-infra.ru:8080`)
- `$consumer_group` — regex для `group` label

Типичный фильтр:
```
kafka_share_group_lag{mdb_kafka_cluster="$cluster", instance=~"$instance", group=~"$consumer_group"}
```

- `mdb_kafka_cluster="$cluster"` — точное совпадение
- `instance=~"$instance"` — regex (переменная может быть `.*` или конкретный хост)
- `group=~"$consumer_group"` — regex

## Aggregation по группе/партиции

```promql
# Суммарный лаг по группе и топику
sum by (group, topic) (kafka_share_group_lag{...})

# Лаг по конкретной партиции (без агрегации, просто отфильтровать)
max by (group, topic, partition) (kafka_share_group_lag{...})

# Кол-во участников share-группы
max by (group) (kafka_share_group_members{...})

# Состояние share-группы (флаг=1, чтобы строка появилась даже если state=Empty)
max by (group, state, coordinator) (kafka_share_group_state{...} * 0 + 1)
```

Трюк `* 0 + 1` — превращает метрику в константу 1, чтобы можно было использовать как «флаг существования» (для join'а с другими метриками).

## Per-minute rate

```promql
# Подтверждения в минуту по типу ack
60 * rate(kafka_server_sharegroupmetrics_record_acknowledgements_per_sec_total{...}[$__rate_interval])
```

- `rate(...[$__rate_interval])` — per-second rate
- `60 *` — переводим в per-minute
- `$__rate_interval` — Grafana-переменная, автоматически подстраивает окно под зум и scrape-интервал

В description НЕ писать формулу расчёта — только что показывает метрика.

## group_left — добавить labels из другой метрики

Использовать когда: у метрики A есть нужные values, но не хватает labels. Берём labels из метрики B.

```promql
# Добавить координатора к лагу share-группы
kafka_share_group_lag{...}
  * on(group) group_left()
kafka_share_group_state{...}
```

- `on(group)` — джойним по label `group`
- `group_left()` — левая метрика (kafka_share_group_lag) сохраняет свои values, добирает labels из правой

## Multi-metric через group_left (НЕ работает для таблиц)

`group_left` удобен для time series, но для таблиц с несколькими Value-колонками не подходит — PromQL возвращает одну Value. Для multi-column таблиц используй multi-target + merge/joinByField (см. `table_panel_patterns.md`).

## p95 / quantile

```promql
# p95 времени загрузки share-партиции
quantile_over_time(0.95, kafka_server_sharepartitionmetrics_partition_load_time_ms{...}[$__rate_interval])
```

Или агрегация по партициям:
```promql
quantile(0.95, kafka_server_sharepartitionmetrics_in_flight_message_count{...})
```

## Multi-broker дедупликация

`share-group-lag-exporter` запущен на каждом брокере кластера (3 ДЦ). Метрики `kafka_share_group_lag` дублируются. Схлопнуть:

```promql
max by (group, topic, partition) (kafka_share_group_lag{...})
```

`max` (а не `sum`) — потому что значение одинаковое на всех брокерах, берём любое.

## Типичные метрики Kafka 4.x (JMX exporter, порт 8080)

| Что | Метрика |
|---|---|
| Broker up | `jvm_info{...}` (или `kafka_server_kafkaserver_brokerstate` в 4.x) |
| Active controllers | `kafka_controller_kafkacontroller_activecontrollercount{...}` |
| Partitions count | `kafka_server_replicamanager_partitioncount{...}` |
| Under-replicated partitions | `kafka_server_replicamanager_underreplicatedpartitions{...}` |
| Offline partitions | `kafka_controller_kafkacontroller_offlinepartitionscount{...}` |
| Messages in rate | `kafka_server_brokertopicmetrics_messagesin_total{...}` |
| Bytes in/out | `kafka_server_brokertopicmetrics_bytesin_total{...}` / `bytesout_total` |
| Failed produce requests | `kafka_server_brokertopicmetrics_failedproducerequests_total{...}` |
| Consumer group lag (kafka-exporter, порт 23569) | `kafka_consumergroup_lag{...}` |
| Share group lag (share-group-lag-exporter, порт 23570) | `kafka_share_group_lag{...}` |
| Share group state | `kafka_share_group_state{...}` |
| Share group members | `kafka_share_group_members{...}` |
| Share ack rate (aggregate, без group label) | `kafka_server_sharegroupmetrics_record_acknowledgements_per_sec_total{ack_type="Accept\|Reject\|Release\|Renew"}` |
| Share in-flight messages (per group,topic,partition) | `kafka_server_sharepartitionmetrics_in_flight_message_count{...}` |

## Ловушки

1. **Kafka 4.x vs 3.x MBean'ы** — в 4.x некоторые MBean'ы удалены/переименованы (например `kafka.server:type=raft-metrics/current-state` удалён, надо `kafka.server:name=BrokerState,type=KafkaServer`). Если метрика пустая — проверить через `curl localhost:8080/metrics | grep <name>` на брокере.
2. **Share ack metrics только aggregate** — `kafka_server_sharegroupmetrics_record_acknowledgements_per_sec_total` НЕ имеет label `group`, только `ack_type`. Per-group ack получить нельзя.
3. **Share in-flight имеет group label** — `kafka_server_sharepartitionmetrics_in_flight_message_count` имеет labels (group, topic, partition), можно агрегировать `sum by (group)`.
4. **`rate()` требует counter** — если метрика gauge (например in-flight count), `rate` не имеет смысла, использовать как есть.
5. **`$__rate_interval` обязательно** — не хардкодить `[5m]`, scrape-интервал дашборда может быть другим.
