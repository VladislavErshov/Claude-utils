---
date: 2026-07-29
ticket: MDBSUP-4271
cluster: push-sender-notifications-kafka
hosts:
  - 1.broker.push-sender-notifications-kafka.ic.one-infra.ru
kafka_version: 3.8.0
kafka_exporter: danielqsj kafka_exporter (запущен 02 Jul)
resolution: open
---

# Message IN > Message Consume в Grafana — 17 из 24 партиций не читаются

## Симптомы

В Grafana на дашборде MDB Kafka:
- `Message IN Per Minute` ≈ 2.4M/мин
- `Message Consume Per Minute` ≈ 0.8M/мин (в 3 раза меньше)
- `Lag by Consumer and Share Group Partition` — показывает 0 на большинстве партиций

Пользователь думал, что консьюмеры не успевают за продюсерами.

## Что проверено

### 1. Метрики живы

Все 4 exporter'а на хосте работают:
- 8080 JMX — 200
- 7777 Jolokia — 404 (норма для /metrics)
- 23569 kafka-exporter — 200
- 23570 share-group-lag-exporter — 200

`BrokerState = 3` (Running), `UnderReplicatedPartitions = 0`.

### 2. JMX брокера (Jolokia) — реальный throughput

```
MessagesInPerSec  (push-sender-push-in) = 1629 msg/sec (OneMinuteRate)
BytesInPerSec     (push-sender-push-in) = 3.06 MB/s
BytesOutPerSec    (push-sender-push-in) = 3.05 MB/s   ← ≈ BytesIn!
```

`BytesIn ≈ BytesOut` на уровне брокера — брокер отдаёт консьюмерам почти столько же,
сколько принимает от продюсеров. Подсказка, что **в целом потребление есть**, проблема
локализована не на всём топике.

### 3. Kafka-exporter (порт 23569) — аномалия committed offset

За 30 сек замер `consumer_offset` и `LEO` по 24 партиям топика `push-sender-push-in`:

| Партиции | Δ consumer_offset / 30s | Δ LEO / 30s | Lag | consumer_offset | LEO |
|---|---|---|---|---|---|
| 7 шт: 0, 3, 5, 14, 19, 20, 22 | **+45k–100k** | +100k | 242–677 | ~3.4e8 | ~3.4e8 |
| 17 шт: 1, 2, 4, 6, 7, 8, 9, 10, 11, 12, 13, 15, 16, 17, 18, 21, 23 | **0** | +100k | **−200M…−214M** | **5.6e8** | ~3.5e8 |

`kafka_consumergroup_lag_sum` = **−3.50e9** (отрицательный).

На 17 партиях `consumer_offset` (5.6e8) **больше LEO** (3.5e8) — семантически невозможно
в нормальной работе. Committed offset статичен, не растёт со временем.

### 4. Других consumer groups нет

Через kafka-exporter перечислены все группы на топике `push-sender-push-in`:
только одна — `push-sender-push-in`. Значит 17 непотребляемых партиций **не читает
вообще никто**. Копится ~1.7M сообщений/мин (17 × ~100k) и позже выпадает по retention.

### 5. Рестарт kafka-exporter не помог

После `systemctl restart kafka-exporter.service` значения `consumer_offset` на 17
партиях идентичны до запятой (5.62393675e+08 и т.д.). Значит это **не кэш kafka-exporter** —
это реальные committed offset'ы из `__consumer_offsets`.

## Корневая причина

Consumer group `push-sender-push-in` **назначена только на 7 партий из 24**.
На остальных 17 партиях committed offset остался с прошлой активности (~5.6e8,
когда LEO был выше), и с тех пор не двигается — консьюмер к ним не обращается.

Номера читаемых партий (0, 3, 5, 14, 19, 20, 22) — не подряд, что похоже на результат
работы `cooperative-sticky` assignor после нескольких перебалансировок. Возможно,
часть партий осталась unassigned после некорректной перебалансировки, либо consumer
instance'ов стало меньше, чем нужно для покрытия всех 24 партий.

## Почему Grafana показывает IN > Consume

| Панель | PromQL | Что считает | Значение |
|---|---|---|---|
| `Message IN Per Minute` | `rate(kafka_topic_partition_current_offset[1m]) * 60` | сумма rate LEO по всем 24 партиям | ~2.4M/мин |
| `Message Consume Per Minute` | `increase(kafka_consumergroup_current_offset[1m])` | сумма Δ committed offset по всем 24 партиям | ~0.8M/мин |

7 партий из 24 дают вклад в Consume → 7/24 ≈ 29% от IN. Замерянное ratio 0.8M / 2.4M
= 33% — точно сходится с 7/24.

## Почему `Lag by Consumer and Share Group Partition` показывает 0

PromQL панели:
```promql
avg(clamp_min(kafka_consumergroup_lag{...}, 0)) by (consumergroup, topic, partition)
```

`clamp_min(..., 0)` обрезает отрицательный lag в 0. На 17 партиях lag = −200M →
на графике 0. Реальный маленький lag (242–677) виден только на 7 читаемых партиях.

Групповая панель `Lag by Consumer and Share Group Partition` использует
`clamp_min(kafka_consumergroup_lag_sum, 0)` → sum = −3.5e9 → тоже 0 на графике.
**Проблема полностью скрыта `clamp_min`.**

## Варианты фикса

### 1. Разбраться с consumer group (правильный путь)

Получить state группы через `kafka-consumer-groups.sh --describe --state` (нужен
SASL-доступ как у kafka-exporter: `--sasl.enabled --sasl.username=kafka_exporter
--tls.ca-file=/opt/kafka/ssl/tls_ca.crt`).

Покажет: кол-во активных членов, assignor, назначение партий на инстансы.

- Если членов < 7 → добавить инстансов.
- Если членов достаточно, но 17 партий unassigned → перезапуск consumer'ов с
  форсированной перебалансировкой, временно сменить assignor на `range`/`roundrobin`.
- Логи consumer'а на `OffsetOutOfRangeException` для 17 партий — если ошибок нет,
  consumer вообще не пытается их читать (проблема в assignor / subscription).

### 2. Сброс offset'ов — НЕ поможет

`kafka-consumer-groups.sh --reset-offsets --to-latest` поставит committed offset
= LEO на 17 партиях, но если consumer не подписан — offset останется статичным.
Картина в Grafana не изменится.

Сброс `--to-earliest` приведёт к пере-прочитыванию ~13M сообщений на партию
(разница oldest 3.27e8 → LEO 3.4e8) × 17 партий = ~210M сообщений. Это потеря
данных, если их не читал никто.

## Что НЕ помогло

- **Рестарт kafka-exporter.** Значения `consumer_offset` на 17 партиях идентичны
  до рестарта и после. Значит 5.6e8 — реальные данные из `__consumer_offsets`,
  не кэш kafka-exporter.
- **Рестарт брокера.** Не делался, но проблема не на стороне брокера — JMX
  `BytesIn ≈ BytesOut` подтверждает, что брокер отдаёт всё что принимает (для
  партиций, которые он лидерит).

## Ключевые уроки

1. **`BytesInPerSec ≈ BytesOutPerSec` на broker-level НЕ гарантирует, что весь
   топик потребляется.** Брокер лидерит только на части партий. Per-topic JMX
   показывает трафик только для партий, лидер которых — этот брокер. Нужно
   проверять все 3 брокера, чтобы увидеть полную картину.

2. **`clamp_min(lag, 0)` в PromQL скрывает реальную проблему.** Если committed
   offset > LEO (невозможно в норме), lag становится отрицательным и обрезается
   в 0 — на Grafana проблема невидима. Нужно проверять сырые значения
   `kafka_consumergroup_current_offset` vs `kafka_topic_partition_current_offset`
   на хосте.

3. **`increase(metric[1m]) = 0` на части партиций = consumer не читает.** Это
   надёжный признак. Если LEO растёт, а committed offset статичен — партиция
   точно не потребляется этой группой.

4. **Committed offset > LEO = «зомби-offset» с прошлой активности.** Не лечится
   рестартом kafka-exporter (он перечитает те же данные из `__consumer_offsets`).
   Лечится только сбросом offset'ов **после** того, как consumer начнёт реально
   читать эти партиции.

5. **`Message IN > Message Consume` с ratio ~7/24 ≈ 29%** — точный маркер того,
   что consumer group читает только часть партий. Ratio равно доле читаемых
   партий.

## Команды для повторения диагностики

### Сравнение Δ consumer_offset vs Δ LEO по партиям (30 сек)

```bash
# на хосте через mcc ssh
curl -s --max-time 30 localhost:23569/metrics > /tmp/m1.txt
sleep 30
curl -s --max-time 30 localhost:23569/metrics > /tmp/m2.txt

for p in 0 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20 21 22 23; do
  c1=$(grep "kafka_consumergroup_current_offset{.*partition=\"$p\",.*topic=\"push-sender-push-in\"}" /tmp/m1.txt | awk '{print $2}')
  c2=$(grep "kafka_consumergroup_current_offset{.*partition=\"$p\",.*topic=\"push-sender-push-in\"}" /tmp/m2.txt | awk '{print $2}')
  l1=$(grep "kafka_topic_partition_current_offset{.*partition=\"$p\",.*topic=\"push-sender-push-in\"}" /tmp/m1.txt | awk '{print $2}')
  l2=$(grep "kafka_topic_partition_current_offset{.*partition=\"$p\",.*topic=\"push-sender-push-in\"}" /tmp/m2.txt | awk '{print $2}')
  printf "p=%s c_delta=%s l_delta=%s\n" "$p" "$(awk -v a=$c1 -v b=$c2 'BEGIN{print b-a}')" "$(awk -v a=$l1 -v b=$l2 'BEGIN{print b-a}')"
done
```

`c_delta = 0` → партиция не читается.

### Проверка других consumer groups на топике

```bash
curl -s --max-time 30 localhost:23569/metrics \
  | grep -E "kafka_consumergroup_current_offset\{.*topic=.push-sender-push-in.\}" \
  | grep -v "^#" \
  | sed 's/.*consumergroup="\([^"]*\)".*/\1/' \
  | sort | uniq -c
```

### JMX per-topic throughput (Jolokia)

```bash
curl -s --max-time 30 "localhost:7777/jolokia/read/kafka.server:name=MessagesInPerSec,type=BrokerTopicMetrics,topic=push-sender-push-in"
curl -s --max-time 30 "localhost:7777/jolokia/read/kafka.server:name=BytesInPerSec,type=BrokerTopicMetrics,topic=push-sender-push-in"
curl -s --max-time 30 "localhost:7777/jolokia/read/kafka.server:name=BytesOutPerSec,type=BrokerTopicMetrics,topic=push-sender-push-in"
```
