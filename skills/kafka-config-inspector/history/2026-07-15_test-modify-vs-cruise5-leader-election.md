# Сравнение controller-конфигов test-modify vs test-cruise5

**Дата**: 2026-07-15
**Цель**: понять почему на test-cruise5 контроллеры выбирают лидера, а на test-modify — нет.

## Кластеры

| Кластер | cluster_id | DC | Хосты |
|---|---|---|---|
| test-modify | `0964c579-1f1b-4595-9c28-84dc783d2a29` | hc, kc, zc | `1.controller.test-modify-mdbdev-kafka.{hc,kc,zc}.one-infra.ru` |
| test-cruise5 | `789b22f3-7923-4dbb-b9e4-c049e19d203c` | dc, ic, uc | `1.controller.test-cruise5-mdbdev-kafka.{dc,ic,uc}.one-infra.ru` |

Файлы скачаны в `/tmp/kafka-inspect/<host>/`.

## PMS-API snapshot

На обоих кластерах PMS-переменные `<NOT_SET>`:
- `kafka.controller.properties` = `<NOT_SET>`
- `kafka.controller.quorum` = `<NOT_SET>`
- `kafka.isWanCluster` = `<NOT_SET>`

PMS-значения идентичны между кластерами (оба пустые) — PMS не источник различия.

## controller.properties — KRaft-критичная часть

**Идентична между кластерами:**

```properties
process.roles=controller
node.id=10001|11001|12001
controller.quorum.voters=<3 voters @ FQDN:9093>
listeners = CONTROLLER://:9093
controller.listener.names=CONTROLLER
listener.security.protocol.map=CONTROLLER:SASL_SSL,INTERNAL:SASL_SSL,WAN:SASL_SSL
sasl.enabled.mechanisms=PLAIN,SCRAM-SHA-256
sasl.mechanism.controller.protocol=PLAIN
authorizer.class.name=org.apache.kafka.metadata.authorizer.StandardAuthorizer
super.users=User:super
allow.everyone.if.no.acl.found=false
ssl.keystore.location=/opt/kafka/ssl/server.keystore.jks
ssl.truststore.location=/opt/kafka/ssl/client.truststore.jks
log.dirs=/mnt/data/log
metadata.log.dir=/mnt/data/metadata
```

## Различия в controller.properties (косметические)

| Параметр | test-modify | test-cruise5 |
|---|---|---|
| `broker.rack` | hc/kc/zc | dc/ic/uc (ожидаемо) |
| `controller.quorum.voters` FQDN | test-modify-... | test-cruise5-... (ожидаемо) |
| `ssl.keystore.password` | `I6gXfXwIJOXwUHp` | `uYCzFxpk6qdzNnz` (placeholder ID, оба vault) |
| `ssl.truststore.password` | `qGFCpDxwD8oQsUL` | `3EZzVEWiElqd3Zd` (placeholder ID, оба vault) |
| broker-style настройки | `num.partitions=1`, `log.cleaner.enable=true`, `log.retention.hours=168`, `log.segment.bytes=...`, `log.retention.check.interval.ms=...`, `num.io.threads=9` | `compression.type=uncompressed`, `auto.create.topics.enable=true` |

Broker-style настройки — мусор из j2-шаблона (шаблоны `controller.properties.j2` разные версии на кластерах). На KRaft controller leader election не влияют.

## meta.properties (`/mnt/data/log/meta.properties`)

**Все 6 хостов корректны:**

| Хост | node.id | cluster.id | directory.id | timestamp |
|---|---|---|---|---|
| test-modify hc | 10001 | 0964c579... | CePgdt9Z... | 2026-06-29 16:02 MSK |
| test-modify kc | 11001 | 0964c579... | ffScZaNLo... | 2026-06-29 16:06 MSK |
| test-modify zc | 12001 | 0964c579... | uIoYNLcjb... | 2026-06-29 15:50 MSK |
| test-cruise5 dc | 10001 | 789b22f3... | dAcB2i3f1... | 2026-03-03 13:43 MSK |
| test-cruise5 ic | 11001 | 789b22f3... | noqBMJND0... | 2026-03-03 13:44 MSK |
| test-cruise5 uc | 12001 | 789b22f3... | fjeDg8T5u... | 2026-03-03 13:44 MSK |

`cluster.id` совпадает с ожидаемым на всех хостах. `node.id` совпадает с `controller.properties`. `directory.id` уникален.

⚠️ **test-modify отформатирован 2026-06-29** (~2 недели назад), **test-cruise5 — 2026-03-03** (~4 месяца назад). test-modify недавно пере-создавался.

## KRaft metadata log (`/mnt/data/metadata/`)

| Кластер | Содержимое |
|---|---|
| test-modify hc | `.lock`, `__cluster_metadata-0/` (84 entry), `bootstrap.checkpoint`, `meta.properties` |
| test-cruise5 dc | `.lock`, `__cluster_metadata-0/` (5 entry) |

test-modify имеет `bootstrap.checkpoint` file и в 16+ раз больше записей в `__cluster_metadata-0`. Указывает на недавний reformat + активность/попытки.

## jaas.conf и log4j.properties — SOC audit

| Файл | test-modify | test-cruise5 |
|---|---|---|
| `jaas.conf` | 479B, есть блок `KafkaClient` (soc-logs-robot) | 333B, только `KafkaServer` |
| `log4j.properties` | 4.0K, есть `socKafkaAppender` (SOC audit) | 240B, минимальный Console appender |
| SOC audit status | **включён** | **выключен** |

Это различие не должно влиять на KRaft leader election — SOC appender это outbound Kafka producer для аудит-событий, не часть controller quorum.

## Вывод по конфигам

**Конфиг-файлы controller-ов не объясняют отказ leader election на test-modify.** KRaft-критичная часть `controller.properties` идентична между кластерами, `meta.properties` корректна на всех 6 хостах, PMS-переменные пустые на обоих.

## ⚠️ Реальная причина — найдена в логах `/mnt/logs/dbms/kafka-controller.out.log`

**На всех 3 controllers test-modify** повторяется критическая ошибка KRaft:

```
ERROR [RaftManager id=10001] Had an error during log cleaning (org.apache.kafka.raft.KafkaRaftClient)
org.apache.kafka.common.errors.OffsetOutOfRangeException: Cannot increment the log start offset to 1230690 of partition __cluster_metadata-0 since it is larger than the high watermark 1217794
```

| Controller | log start offset | high watermark |
|---|---|---|
| hc (id=10001) | 1230690 | 1217794 |
| kc (id=11001) | 1231360 | 1217794 |
| zc (id=12001) | 1231908 | 1217794 |

`__cluster_metadata-0` partition повреждена / рассинхронизирована:
- HWM застрял на `1217794` и не двигается
- Каждый controller имеет metadata log, ушедший **за пределы** HWM (1230690+)
- Это блокирует log cleaning и предотвращает выборы лидера

**На cruise5** этой ошибки **нет** — логи чистые (только JVM safepoint noise).

### Дополнительная проблема на test-modify

```
ERROR [Producer clientId=producer-1] Topic authorization failed for topics [soc-audit-log2]
```

SOC audit producer (тот самый `socKafkaAppender` из `log4j.properties`) не авторизован на SOC cluster. Не блокирует leader election, но указывает на кривую настройку SOC audit (на cruise5 SOC audit выключен — этой ошибки нет).

### Crash-loop подтверждение

В err-логах test-modify десятки повторов:
```
Log directory /mnt/data/log is already formatted. Use --ignore-formatted to ignore this directory and format the others.
```
Контроллеры регулярно пытаются реформатнуть log dir при рестарте → что-то их заставляет рестартовать (вероятно сам OffsetOutOfRangeException падает процесс, supervisor его поднимает, цикл повторяется). Это объясняет 102MB out.log на kc за 2 недели.

### Восстановление

KRaft metadata log повреждён. Стандартное лечение (данные кластера будут потеряны):
1. Остановить kafka-controller на всех 3 controllers test-modify
2. Очистить `/mnt/data/log/` и `/mnt/data/metadata/` на всех 3 хостах одновременно
3. Переформатировать через `kafka-storage format --cluster-id <cluster_id>` (или через mdb-processing recreate flow)
4. Запустить controllers

Если данные важны — пробовать `kafka-metadata-quorum` tool для инспекции и ручной правки metadata log (сложно, не гарантирует успеха).
