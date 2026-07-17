---
name: kafka-cluster-inspector
description: Инспекция MDB Kafka кластеров (KRaft, версии 3.x и 4.x) — диагностика "Broker is dead", чтение логов broker/controller/cruise-control через mcc scp, проверка Jolokia MBean'ов, разбор KRaft quorum / controller registration. Список хостов задаёт пользователь (формат 1.broker.<cluster>.<dc>.one-infra.ru / 1.controller.<cluster>.<dc>.one-infra.ru / 1.cruise.<cluster>.<dc>.one-infra.ru). Конфиги и логи читаются через mcc scp. Используй когда нужно проверить состояние Kafka-кластера, найти причину "Broker is dead" в UI mdb-data, разобраться почему broker/controller не стартует или не входит в KRaft quorum.
allowed-tools: [bash, read_file, write_file, edit_file]
---

# Скилл инспекции MDB Kafka кластеров

Скилл для разбора состояния Kafka-кластеров под управлением mdb-data.

⚠️ Скилл проверяет **только состояние процессов Kafka + Cruise Control** (запуск, регистрация в
quorum, MBean'ы, rscheck). Он НЕ проверяет: throughput / latency, настройки топиков / ACL,
rebalance execution, дисковое место, memory. Это к Prometheus/Grafana и mdb-data API.

## Документация

- https://docs.vk.team/mdb/docs/kafka/kafka-intro.html — введение
- https://docs.vk.team/mdb/docs/kafka/kafka.html — детали

Доки лежат в соседнем репо `mdb-docs`.

## Архитектура кластера

- **KRaft-only** — обе версии (3.x и 4.x) работают в KRaft, ZooKeeper не используется.
- **Разделение ролей** — broker и controller на разных хостах:
  - `process.roles=broker` — BrokerServer, KRaft observer
  - `process.roles=controller` — ControllerServer, KRaft voter
- **ДЦ** — любые (`hc`, `pc`, `uc`, `kc`, `ec`, `dc`, `rc`, ...). Формат хоста не зависит от ДЦ.
- **Количество хостов на ДЦ** — любое.
- **Cruise Control** — один на весь кластер. В некоторых кластерах CC вообще нет.
  Расположение CC — спросить у пользователя или посмотреть в UI mdb-data / через `/db-seed`.

## Формат хостов

```
{1,2,3,...}.broker.<cluster>.<dc>.one-infra.ru     — Kafka broker
{1,2,3,...}.controller.<cluster>.<dc>.one-infra.ru — KRaft controller
1.cruise.<cluster>.<dc>.one-infra.ru               — Cruise Control (один на кластер, может не быть)
```

Пользователь даёт список хостов. Скилл не угадывает хосты — только работает с тем, что дал юзер.

## Что нужно

- **mcc** (`/Users/vl.ershov/Documents/mcc/mcc`) — доступ к хостам. **Только `mcc scp`** —
  `mcc ssh` не принимает аргументы с пробелами/пайпами. Для интерактивных команд (curl Jolokia,
  kafka-topics.sh, journalctl) пользователь сам заходит на хост.

## mcc scp особенности

- Скачивание директории: `mcc scp "<host>:/path/" "<local_dir>/"` — локальная директория должна
  существовать заранее (`mkdir -p`).
- Скачивание файла: локальный путь — **директория**, не путь к файлу.
- `SSL Handshake is not finished` — повторить через 1-2 сек.
- `EOF на tar header` — опечатка в пути или файла не существует.

## Хосты и пути

| Что | Путь на хосте |
|---|---|
| Логи сервисов | `/mnt/logs/dbms/` (kafka-broker/controller/exporter, cruise-control) |
| Конфиги Kafka | `/opt/kafka/config/` (server.properties, client.properties) |
| SSL | `/opt/kafka/ssl/` (server.keystore.jks, server.truststore.jks) |
| Systemd | `/etc/systemd/system/kafka-*.service`, `cruise-control.service` |
| rscheck | `/etc/rscheck/` (kafka.conf.j2, modules/checkkafka.py) |
| host_checker | `/etc/host_checker/` (checks/check_kafka.py) |
| Prometheus JMX | `/opt/prometheus/` (kafka-2_0_0.yml, cruise-control.yml) |
| Cruise Control | `/opt/cruise-control/` (config/, libs/, dependant-libs/) |

## Структура скилла

- `SKILL.md` — этот файл.
- `commands/download_logs.md` — скачивание и анализ логов broker/controller/cruise.
- `commands/jolokia_inspect.md` — Jolokia MBean'ы, разница Kafka 3.x vs 4.x, Kafka CLI.
- `commands/diagnose_broker_dead.md` — пошаговый разбор "Broker is dead".
- `commands/known_issues.md` — детали известных проблем (симптомы, причины, фиксы).

## Известные проблемы (кратко)

Подробности — `commands/known_issues.md`.

- **"Broker is dead" в UI** — rscheck/host_checker падает на MBean `kafka.server:type=raft-metrics/current-state`,
  удалённом в Kafka 4.x. Фикс — `kafka.server:name=BrokerState,type=KafkaServer`. Разбор — `diagnose_broker_dead.md`.
- **`only N broker(s) are registered`** — не все брокеры успели зарегистрироваться, либо controller
  quorum не собрался.
- **`<unresolved>` controller hostname** — норма в момент initialization, проблема если не резолвится
  через минуту.
- **CC не запускается** — `UnsupportedClassVersionError` (Java 11 vs 17). Фикс — обновить Java в
  `ubuntu20-mdb-cruisecontrol-base`.
- **CruiseControlMetricsReporter не подключается** — JAR несовместим с версией Kafka (CC 2.5.141
  vs Kafka 4.x требует 2.5.147+), либо auth-проблема.
- **Fenced брокер** — `FencedBrokerCount > 0` на controller-хосте.
- **Under-replicated partitions** — `UnderReplicatedPartitions > 0` на broker-хосте.

## Что НЕ покрывает скилл

- Throughput / latency / performance — к Prometheus/Grafana.
- Настройка топиков / ACL / quotas — к mdb-data API.
- Rebalance execution — только чтение состояния CC, не запуск.
- KRaft log corruption — нужен `kafka-dump-log.sh`.
- Дисковое место / memory — к хостовым чекерам.
