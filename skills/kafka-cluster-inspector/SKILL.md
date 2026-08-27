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
- **работа с хостами** (mcc ssh/scp, пути) — [`mcc-host-worker`](../mcc-host-worker/SKILL.md)
  (база) + `kafka-host-inspector` (Kafka-специфика)
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

## Broker / controller IDs

Внутренние `broker.id` / `node.id` в Kafka-кластере — **5-значные**, не совпадают с порядковым номером в hostname. Формат: `<dc-prefix><host-index>`.

| Роль хоста                     | broker.id / node.id | Примеры                          |
|--------------------------------|---------------------|----------------------------------|
| broker в DC                    | 2`<dc>`0`<idx>`     | 1.broker.dc → 20001, 2.broker.dc → 20002 |
| broker в IC                    | 21`<idx>`           | 1.broker.ic → 21001              |
| broker в UC                    | 22`<idx>`           | 1.broker.uc → 22001              |
| broker в PC                    | 23`<idx>`           | 1.broker.pc → 23001, 2.broker.pc → 23002 |
| controller в DC                | 10`<idx>`           | 1.controller.dc → 10001          |
| controller в IC                | 11`<idx>`           | 1.controller.ic → 11001          |
| controller в UC                | 12`<idx>`           | 1.controller.uc → 12001          |

Для DC-префикса берётся код из таблицы `one_cloud_meta` / PMS (dc → `20000`, hc → `24000`, и т.д.) — но **точные значения нужно уточнять через `describeCluster` или `kafka-broker-api-versions.sh`**, если IDs нужны для `alterPartitionReassignments` / `unregisterBroker`. Передача неправильных IDs падает с `InvalidReplicaAssignmentException: The manual partition assignment includes broker X, but no such broker is registered`.

Узнать реальные IDs:
```bash
mcc --local -n infra ssh 1.broker.<cluster>.<dc>.one-infra.ru \
  'kafka-broker-api-versions.sh --bootstrap-server localhost:9092 --command-config /etc/kafka/kafka-console-consumer.properties 2>/dev/null | grep -oE "id: [0-9]+" | sort -u'
```

## Подчинённые скиллы

Работа с хостами и логами вынесена в отдельные скиллы — вызывай их напрямую:

- **`kafka-host-inspector`** — Kafka-специфика выполнения команд на хосте, путеводитель по
  путям (логи, конфиги, SSL, systemd, rscheck, host_checker, prometheus, cruise-control).
  Базовые паттерны `mcc ssh + expect`/`mcc scp` — в [`mcc-host-worker`](../mcc-host-worker/SKILL.md).
  **Содержит собственную `history/`** с инцидентами хостового уровня (диагностика через логи
  cruise-хоста, Porto-контейнер, cgroup и т.п.). Перед запуском диагностики кластера сверяйся
  с `kafka-host-inspector/history/` — если симптом совпадает, делегируй в дочерний скилл.
- **`kafka-log-investigator`** — скачивание и анализ логов broker/controller/cruise-control
  (`/mnt/logs/dbms/`), что грепать в `kafka-broker.out.log` / `kafka-controller.out.log` /
  `cruise-control.err.log`, маркеры старта/ошибок.
- **`kafka-metrics-investigator`** — метрики (JMX 8080, Jolokia 7777, kafka-exporter 23569,
  share-group-lag-exporter 23570), Jolokia MBean'ы, диагностика "Broker is dead" через MBean'ы.
- **`kafka-reassign-partitions`** — ручное перераспределение партиций через
  `kafka-reassign-partitions.sh` по заданной схеме размещения реплик. Предлагай, когда
  пользователь даёт **явную схему** (какие партиции на какие брокеры), хочет вывести брокер
  из кластера, видит дисбаланс дисковой нагрузки между ДЦ и хочет перекинуть партиции вручную.
  Не предлагай для автоматического ребаланса — это к Cruise Control (`commands/cruise_control_ops.md`).

## Оператор one-cloud-ops (операции вне Temporal)

Операции вида `get_kafka_downscale_broker_result` выполняет **оператор one-cloud-ops**, не
Temporal — по operationId в Temporal будет пусто. Диагностика через `mcc ops`, ручное
выполнение шагов задачи (reassign → unregister → withdraw → `mcc op_stop`) — в
[commands/one_cloud_ops.md](commands/one_cloud_ops.md). Поиск/закрытие самой операции в
прод-БД — скилл `jira-mdbsup-solver`.

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
- `commands/one_cloud_ops.md` — оператор one-cloud-ops (операции вне Temporal): диагностика
  `mcc ops`, что делает DownscaleKafkaBrokerTask, ручное выполнение downscale-broker,
  `mcc op_stop`, грабли заливки файлов/414/Java-classpath.
- `history/` — разборы реальных инцидентов и MDBSUP-операций кластерного уровня
  (modify/resize, downscale-broker, sysconfig, quorum, SCRAM; симптом + фикс + грабли).
  Полные разборы MDBSUP-кейсов живут здесь; в `jira-mdbsup-solver/history/` — только
  заглушки со ссылками сюда. Повторяющиеся паттерны в новых кейсах заменять ссылками на
  старые разборы, не дублировать. Полные разборы reassign-кейсов могут лежать в
  `kafka-reassign-partitions/history/`. Перед диагностикой смотреть, нет ли похожего случая.
- **`kafka-host-inspector/history/`** — разборы инцидентов **хостового уровня** (подключение к
  хосту, пути к логам/конфигам, Porto-контейнер, cgroup, специфика выполнения команд). Родительский
  скилл читает эту историю, чтобы понять, нужно ли делегировать в `kafka-host-inspector`. Если
  симптом совпадает с инцидентом оттуда (например, `NotEnoughValidWindowsException` CC → нужно
  грепать логи на cruise-хосте) — вызывай дочерний скилл, не разбирай вручную.

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
- **CC после пересоздания хоста долбит localhost:9092** (MDBSUP-4739) — конфиги pms лежат под
  неправильным hostname (`cruise-control.<cluster>.clouds` вместо `cruise.<cluster>.clouds`) →
  confp не рендерит `cruisecontrol.properties`, остаётся сток образа с
  `bootstrap.servers=localhost:9092`. Фикс — переложить конфиги в pms под правильный hostname,
  затем `confp --oneshot && systemctl restart cruise-control` (первый confp-прогон может упасть
  на vault-pki — повторить). Разбор — `history/MDBSUP-4739.md`.
- **CC в crash-loop: `cannot find the metrics reporter topic [__CruiseControlMetrics]`**
  (bilmigrated-datatransfer-kafka, 2026-08-21) — create_additional_service записал блок
  `metric.reporters` в PMS, но: (1) рендер на брокеры не дошёл — файлы старше PMS-изменения,
  (2) в PMS-переменной забыли импорт `{% import "/etc/misc/utils.j2" as utils -%}` → confp
  падает `UndefinedError: 'utils' is undefined`, весь рендер broker.properties блокируется.
  Фикс — импорт в PMS первой строкой, затем поочерёдно confp+restart на брокерах.
  Разбор — `history/bilmigrated_cc_crashloop_no_utils_import.md`.
- **`NotEnoughValidWindowsException` на свежеподнятом CC** (MDBSUP-4761) — remove_broker
  запущен до прогрева (нужно 5 окон по 5 мин после старта CC). Конфиг при этом валидный —
  сначала смотреть `GET /user_tasks` и `/state`: возможно, повтор операции уже прошёл успешно
  (в тикете — прошёл на следующий день, вмешательство не потребовалось). Разовый OOM в
  HTTP-Dispatcher в err.log — побочный эффект упавшего запроса, не падение CC.
  Разбор — `history/MDBSUP-4761.md`.
- **`NotEnoughValidWindowsException` при живом CC — молчит один брокер** (onemekafkaauth38,
  2026-08-27) — NumValidPartitions < 95% и недостающая доля ≈ 1/N брокеров: на одном хосте
  контейнер с битым cgroup/mountinfo → JDK NPE при `getProcessCpuLoad()` → репортер CC
  падает на CPU-метрике каждую минуту и молчит целиком. Рестарт не лечит. Фикс —
  `mcc migrate --relocate` шарда инстанса на другой миньон. Разбор —
  `history/2026-08-27_onemekafkaauth38-cc-notenoughvalidwindows-broken-container.md`.
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
- **Controller crash-loop: node.id-свалка** (I48592) — два симптома: `Stored node id X
  doesn't match previous node id Y in meta.properties` (конфликт диск vs конфиг) и
  `leader ... epoch N inconsistent with current leader empty and epoch 1` (дубликат
  node.id — id уже занят другим voter'ом). Корень — рассинхрон PMS: `kafka.layout`
  противоречит `kafka.controller.quorum` (node.id рендерится по позиции ДЦ в layout:
  `10000 + dc_id*1000 + instance_id`). Вайп дисков НЕ чинит, пока PMS неверен; «active»
  после вайпа ≠ в кворуме. Фикс — исправить layout в PMS → confp → вайп данных
  контроллера при конфликте meta.properties. Разбор — `history/I48592.md`.
- **Fenced брокер** — `FencedBrokerCount > 0` на controller-хосте (проверка через `kafka-metrics-investigator`).
- **Under-replicated partitions** — `UnderReplicatedPartitions > 0` на broker-хосте (проверка через `kafka-metrics-investigator`).
- **Offline partitions из-за удалённого брокера в Replicas** (MDBSUP-4166) — в Grafana
  `offline/under-repl/at-min-isr > 0`, все broker-хосты AVAILABLE, но `kafka-topics
  --unavailable-partitions` показывает партии с `Leader: none` и паттерном
  `Replicas: <dead_broker>,... Isr: <dead_broker>`. Хост удалён из mdb-data, но остался в
  metadata Kafka как preferred leader. Фикс — unclean leader election (`kafka-leader-election.sh
  --admin.config --election-type unclean --all-topic-partitions`) + reassign для убирания
  мёртвого broker id из Replicas (скилл `kafka-reassign-partitions`). Детали — `known_issues.md`.

## Что НЕ покрывает скилл

- Throughput / latency / performance — к Prometheus/Grafana.
- Rebalance execution — только чтение состояния CC, не запуск (но в `troubleshooting.md`
  есть инструкция по `kafka-reassign-partitions.sh` и перераспределению через CC).
- KRaft log corruption — нужен `kafka-dump-log.sh`.
- Дисковое место / memory — к хостовым чекерам (но в `troubleshooting.md` есть разбор
  забившихся дисков и `-stray` партиций).
