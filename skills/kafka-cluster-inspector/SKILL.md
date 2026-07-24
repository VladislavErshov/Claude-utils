---
name: kafka-cluster-inspector
description: Инспекция MDB Kafka кластеров (KRaft, версии 3.x и 4.x) — архитектура кластера, разбор KRaft quorum / controller registration, каталог известных проблем (CruiseControlMetricsReporter, InvalidReplicationFactor, Java version mismatch, Broker is dead). Список хостов задаёт пользователь (формат 1.broker.<cluster>.<dc>.one-infra.ru / 1.controller.<cluster>.<dc>.one-infra.ru / 1.cruise.<cluster>.<dc>.one-infra.ru). Используй когда нужно понять состояние кластера, найти причину почему broker/controller не стартует или не входит в KRaft quorum, разобраться с известными проблемами. Работа с хостами — `kafka-host-inspector`, анализ логов — `kafka-log-investigator`, метрики и Jolokia MBean'ы — `kafka-metrics-investigator`.
allowed-tools: [Bash, Read, Write, Edit, Grep, Glob]
---

# Скилл инспекции MDB Kafka кластеров

Скилл-каталог для разбора состояния Kafka-кластеров под управлением mdb-data. Содержит
архитектуру кластера, формат хостов и каталог известных проблем. Конкретные операции
делегированы подчинённым скиллам.

⚠️ Скилл описывает **состояние процессов Kafka + Cruise Control** на уровне кластера
(запуск, регистрация в quorum, rscheck) и каталог известных проблем. Конкретные операции:
- **работа с хостами** (mcc ssh/scp, пути) — `kafka-host-inspector`
- **анализ логов** broker/controller/cruise — `kafka-log-investigator`
- **метрики, MBean'ы, диагностика "Broker is dead"** — `kafka-metrics-investigator`

Скилл НЕ покрывает: throughput / latency, настройки топиков / ACL,
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

## Подчинённые скиллы

Работа с хостами и логами вынесена в отдельные скиллы — вызывай их напрямую:

- **`kafka-host-inspector`** — подключение к хостам через `mcc ssh` + `expect`, особенности
  `mcc scp`, шаблоны выполнения команд, путеводитель по путям на хосте (логи, конфиги, SSL,
  systemd, rscheck, host_checker, prometheus, cruise-control).
- **`kafka-log-investigator`** — скачивание и анализ логов broker/controller/cruise-control
  (`/mnt/logs/dbms/`), что грепать в `kafka-broker.out.log` / `kafka-controller.out.log` /
  `cruise-control.err.log`, маркеры старта/ошибок.
- **`kafka-metrics-investigator`** — метрики (JMX 8080, Jolokia 7777, kafka-exporter 23569,
  share-group-lag-exporter 23570), Jolokia MBean'ы, диагностика "Broker is dead" через MBean'ы.
- **`kafka-reassign-partiotions`** — ручное перераспределение партиций через
  `kafka-reassign-partitions.sh` по заданной схеме размещения реплик. Предлагай, когда
  пользователь даёт **явную схему** (какие партиции на какие брокеры), хочет вывести брокер
  из кластера, видит дисбаланс дисковой нагрузки между ДЦ и хочет перекинуть партиции вручную.
  Не предлагай для автоматического ребаланса — это к Cruise Control (`commands/cruise_control_ops.md`).

## Структура скилла

- `SKILL.md` — этот файл.
- `commands/runbook.md` — дежурный ранбук: доступность кластера, рестарт, логи, порты,
  kafkactl, синхронизация kafka.sync, тайминги и ссылка на встречу.
- `commands/troubleshooting.md` — каталог типовых проблем из дежурного ранбука (создание
  топика, не могу подключиться, сэмпл сообщений, перевод на SASL_PLAINTEXT, место на
  брокерах / в логах, Connection timed out, зависшие таски, обновление версии,
  перераспределение партиций, переезд rc→hc, STARTING RESERVED, io/network треды,
  ребалансировка consumer group, удаление брокера, новый listener, JoinGroup INCONSISTENT_GROUP_PROTOCOL).
- `commands/cruise_control_ops.md` — операции с Cruise Control: диагностика (dead,
  RUNNING UNAVAILABLE, нет метрик), актуализация конфига, поднятие CC на кластере, перенос
  в другой ДЦ.
- `commands/administration.md` — рутинное администрирование: проверка видимости брокеров,
  удаление контроллера, unregister брокера, пользователи / топики / ACL / consumer groups
  (создание, проверка, удаление).
- `commands/known_issues.md` — каталог известных технических проблем (симптомы, причины,
  фиксы): Broker is dead, InvalidReplicationFactor, CruiseControlMetricsReporter и т.д.
- `history/` — краткие разборы реальных инцидентов (симптом + фикс + грабли). Полные разборы
  могут лежать в `kafka-reassign-partiotions/history/`. Перед диагностикой смотреть, нет ли
  похожего случая.

## Известные проблемы (кратко)

Подробности — `commands/known_issues.md`. Разбор типовых дежурных проблем —
`commands/troubleshooting.md`.

- **"Broker is dead" в UI** — rscheck/host_checker падает на MBean `kafka.server:type=raft-metrics/current-state`,
  удалённом в Kafka 4.x. Фикс — `kafka.server:name=BrokerState,type=KafkaServer`. Разбор —
  скилл `kafka-metrics-investigator` (`commands/diagnose_broker_dead.md`).
- **`only N broker(s) are registered`** — не все брокеры успели зарегистрироваться, либо controller
  quorum не собрался.
- **`<unresolved>` controller hostname** — норма в момент initialization, проблема если не резолвится
  через минуту.
- **CC не запускается** — `UnsupportedClassVersionError` (Java 11 vs 17). Фикс — обновить Java в
  `ubuntu20-mdb-cruisecontrol-base`.
- **CruiseControlMetricsReporter не подключается** — JAR несовместим с версией Kafka (CC 2.5.141
  vs Kafka 4.x требует 2.5.147+), либо auth-проблема. Отдельный случай: `ClassNotFoundException:
  CruiseControlMetricsReporter` — брокер падает при старте, JAR репортера отсутствует в образе.
  Фикс — поднять версию docker-образа Kafka. Детали — `known_issues.md`.
- **Broker не регистрируется в controller quorum** (INCALL-42685) — рассинхрон
  `controller.quorum.voters` при миграции ДЦ controller'ов. На broker-хостах voters обновили,
  а на выводимом controller-хосте `node.id` остался и больше не в voters → контроллер падает
  при старте (`node id XXXX must be included in the set of voters`), broker не может
  зарегистрироваться (`Shutting down because we were unable to register with the controller quorum`).
  Фикс — синхронизировать voters на всех хостах. Детали — `known_issues.md`.
- **Fenced брокер** — `FencedBrokerCount > 0` на controller-хосте (проверка через `kafka-metrics-investigator`).
- **Under-replicated partitions** — `UnderReplicatedPartitions > 0` на broker-хосте (проверка через `kafka-metrics-investigator`).
- **Offline partitions из-за удалённого брокера в Replicas** (MDBSUP-4166) — в Grafana
  `offline/under-repl/at-min-isr > 0`, все broker-хосты AVAILABLE, но `kafka-topics
  --unavailable-partitions` показывает партии с `Leader: none` и паттерном
  `Replicas: <dead_broker>,... Isr: <dead_broker>`. Хост удалён из mdb-data, но остался в
  metadata Kafka как preferred leader. Фикс — unclean leader election (`kafka-leader-election.sh
  --admin.config --election-type unclean --all-topic-partitions`) + reassign для убирания
  мёртвого broker id из Replicas (скилл `kafka-reassign-partiotions`). Детали — `known_issues.md`.

## Что НЕ покрывает скилл

- Throughput / latency / performance — к Prometheus/Grafana.
- Rebalance execution — только чтение состояния CC, не запуск (но в `troubleshooting.md`
  есть инструкция по `kafka-reassign-partitions.sh` и перераспределению через CC).
- KRaft log corruption — нужен `kafka-dump-log.sh`.
- Дисковое место / memory — к хостовым чекерам (но в `troubleshooting.md` есть разбор
  забившихся дисков и `-stray` партиций).
