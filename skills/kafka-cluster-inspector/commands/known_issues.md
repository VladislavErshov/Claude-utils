# Известные проблемы Kafka-кластеров

Подробный разбор симптомов, причин и фиксов. В `SKILL.md` только краткие ссылки сюда.

## "Broker is dead" в UI mdb-data

**Симптом**: в UI mdb-data брокер отображается как `unknown` / `dead`, хотя процесс
`kafka-broker.service` запущен и брокер зарегистрировался в KRaft quorum.

**Причина**: rscheck `checkkafka.py` или host_checker `check_kafka.py` падает с исключением
при запросе несуществующего MBean'а. Любое исключение → `return "Broker is dead"`.

**Особенность Kafka 4.x**: MBean `kafka.server:type=raft-metrics/current-state` удалён на
broker-only хостах. rscheck использовал его в `is_broker()` для определения роли ноды.
На 4.x получаем `InstanceNotFoundException` → KeyError на `response["value"]` → "Broker is dead".

**Фикс**: см. скилл `kafka-metrics-investigator`, команда `diagnose_broker_dead.md` — использовать `kafka.server:name=BrokerState,type=KafkaServer`
вместо `raft-metrics/current-state`. MBean существует на broker-хосте и отсутствует на controller-хосте.

Подробный пошаговый разбор — скилл `kafka-metrics-investigator`, `commands/diagnose_broker_dead.md`.

## `only N broker(s) are registered` при создании topic

**Симптом** в `kafka-broker.out.log`:
```
InvalidReplicationFactorException: Unable to replicate the partition 3 time(s):
The target replication factor of 3 cannot be reached because only 2 broker(s) are registered
```

**Причины**:
- Один из брокеров не успел зарегистрироваться (новый кластер, race condition) — подождать.
- Controller quorum не собрался — проверить `kafka-controller.out.log` на всех controller-хостах.
- Часть брокеров имеют cordoned log directories — проверить `kafka-broker.err.log`.

**Что смотреть**:
- `grep -E "Transition from|registered broker|Kafka Server started" kafka-broker.out.log`
  на каждом broker-хосте — должен быть `Successfully registered broker XXXXX`.
- `grep -E "ControllerServer|registered|elected" kafka-controller.out.log`
  на каждом controller-хосте — должен быть elected leader.

## `<unresolved>` controller hostname в логе брокера

**Симптом**: `[RaftManager id=XXX] ... leaderEndpoints=Endpoints(endpoints={ListenerName(CONTROLLER)=1.controller.<cluster>.<dc>.one-infra.ru/<unresolved>:9093})`

**Важно**: `<unresolved>` в момент initialization — **норма**. KRaft пишет это до того, как
резолвнул DNS. Если через минуту брокер успешно регистрируется
(`Successfully registered broker XXXXX with broker epoch YYY`) — DNS работает, проблема в другом.

Если не резолвится и через минуту — реальная проблема DNS / неверный hostname в
`controller.quorum.voters`.

## Cruise Control не запускается

**Симптом** в `cruise-control.err.log`:
```
java.lang.UnsupportedClassVersionError: com/linkedin/kafka/cruisecontrol/KafkaCruiseControlMain
has been compiled by a more recent version of the Java Runtime (class file version 61.0),
this version of the Java Runtime only recognizes class file versions up to 55.0
```

**Причина**: CC собран под Java 17 (class file 61.0), а в образе CC Java 11 (class file 55.0).

**Class file version reference**:
- 55.0 = Java 11
- 61.0 = Java 17

**Фикс**: в `ubuntu20-mdb-cruisecontrol-base/rootfs/docker/build.d/10-install-java.sh` заменить
`openjdk-11-jdk` → `openjdk-17-jdk` и в Dockerfile `ENV JAVA_HOME` на
`/usr/lib/jvm/java-17-openjdk-amd64`.

**Дополнительно**: если в `cruise-control.out.log` только стартовые сообщения Jolokia
(`I> No access restrictor found`, `Jolokia: Agent started with URL ...`) и ничего больше —
CC main class даже не загрузился. Смотреть `err.log` на `UnsupportedClassVersionError` /
`LinkageError` / `ClassNotFoundException`.

## CruiseControlMetricsReporter не подключается к брокеру

**Симптом** в `kafka-broker.out.log` на broker-хосте:
```
WARN [Producer clientId=CruiseControlMetricsReporter] Connection to node -1
(1.broker.<cluster>.<dc>.one-infra.ru/<ip>:9092) could not be established. Node may not be available.
```

**Причины**:
- JAR metrics-reporter'а несовместим с версией Kafka. Kafka 4.x требует CC 2.5.147+, а в образе
  Kafka может быть старый JAR 2.5.141.
- Брокер ещё не стартовал (race при загрузке образа) — подождать.
- Auth-проблемы SASL/SSL — проверить `client.properties`.

**Маркер успешной регистрации репортера**:
```
INFO Starting Cruise Control metrics reporter with reporting interval of 60000 ms.
```

**Маркер падения репортера** (сразу после старта):
```
INFO App info kafka.admin.client for CruiseControlMetricsReporter unregistered
```

### `ClassNotFoundException: CruiseControlMetricsReporter` — брокер не стартует

**Симптом** в `kafka-broker.out.log`:
```
ERROR [main] BrokerServer - [BrokerServer id=XXXXX] Fatal error during broker startup. Prepare to shutdown
org.apache.kafka.common.KafkaException: Class com.linkedin.kafka.cruisecontrol.metricsreporter.CruiseControlMetricsReporter cannot be found
    at org.apache.kafka.common.config.AbstractConfig.getConfiguredInstance(AbstractConfig.java:394)
    at kafka.server.DynamicMetricReporterState.createReporters(DynamicBrokerConfig.scala:920)
    ...
Caused by: java.lang.ClassNotFoundException: com.linkedin.kafka.cruisecontrol.metricsreporter.CruiseControlMetricsReporter
```

Брокер стартует, регистрируется в KRaft quorum, доходит до `DynamicMetricReporterState.createReporters`
и падает → `systemctl status kafka-broker` = `failed (Result: exit-code)`. systemd рестартит в цикле.

**Отличие от предыдущего случая**: там репортер грузился, но не подключался к брокеру (WARN).
Здесь класс репортера **вообще отсутствует** в classpath — JAR не лежит ни в `/opt/kafka/libs/`,
ни в `/opt/kafka/dependant-libs/`. `find /opt /etc -name "*cruise*"` пуст.

**Причина**: docker-образ Kafka слишком старой версии (например `ubuntu20-kafka-3.8.0:1.1.4`).
В старых образах JAR `cruise-control-metrics-reporter-*.jar` либо не укладывался, либо лежал
по несовместимому пути. Конфиг `broker.properties` при этом ссылается на класс репортера
(`metric.reporters=com.linkedin.kafka.cruisecontrol.metricsreporter.CruiseControlMetricsReporter`
+ блок `cruise.control.metrics.reporter.*`), поэтому класс обязан быть в classpath.

**Фикс**: поднять версию docker-образа Kafka (`ubuntu20-kafka-3.8.0` или соответствующего
versioned-образа) на хостах кластера через modify-флоу mdb-data. В новой версии JAR
репортера должен присутствовать в `/opt/kafka/libs/`.

**Как проверить после деплоя**:
```bash
ls /opt/kafka/libs/ | grep -i cruise   # должен показать cruise-control-metrics-reporter-*.jar
systemctl is-active kafka-broker       # active
grep "Starting Cruise Control metrics reporter" /mnt/logs/dbms/kafka-broker.out.log
```

**Временный workaround** (если нельзяすぐ передеплоить): закомментировать в `broker.properties`
`metric.reporters` и весь блок `cruise.control.metrics.reporter.*`, рестартовать
`kafka-broker.service`. Минус — Cruise Control перестанет получать метрики с брокеров.

## Broker не регистрируется в controller quorum (рассинхрон `controller.quorum.voters`)

**Инцидент-референс**: INCALL-42685 (2026-07-23, кластер `kafka-spd-adtech-kafka`).

**Симптом** в `kafka-broker.out.log` — broker циклически падает каждые ~2 мин при старте:
```
ERROR [broker-XXXXX-lifecycle-manager] BrokerLifecycleManager - Shutting down because we were unable to register with the controller quorum.
ERROR [main] BrokerServer - Received a fatal error while waiting for the controller to acknowledge that we are caught up
ERROR [main] BrokerServer - Fatal error during broker startup. Prepare to shutdown
java.lang.RuntimeException: Received a fatal error while waiting for the controller to acknowledge that we are caught up
Caused by: java.util.concurrent.CancellationException
```
И в хвосте крутится без прогресса:
```
INFO [MetadataLoader id=XXXXX] initializeNewPublishers: the loader is still catching up because we still don't know the high water mark yet.
```

**В UI mdb-data** при этом часть controller-хостов может быть `UNAVAILABLE` или `unknown`,
а broker-хосты — `AVAILABLE`/`observer` (т.е. проблема не в самих брокерах).

**Причина**: рассинхрон `controller.quorum.voters` при миграции ДЦ controller'ов. На broker-хостах
voters уже обновили (добавили новый ДЦ, удалили старый), а на controller-хосте старого ДЦ
`node.id` остался, и его нет в voters → Kafka отказывается стартовать:
```
ERROR [main] Kafka$ - Exiting Kafka due to fatal exception
java.lang.IllegalArgumentException: requirement failed: If process.roles contains the 'controller' role,
the node id 11001 must be included in the set of voters controller.quorum.voters=Set(13001, 10001, 12001)
    at kafka.server.KafkaConfig.validateControllerQuorumVotersMustContainNodeIdForKRaftController$1(KafkaConfig.scala:1287)
```

Старый controller (не в voters) падает при старте → в UI `UNAVAILABLE`. При этом quorum из
оставшихся voters может собраться и выбрать лидера, но broker может упорно ломиться к
устаревшему лидеру и не получать metadata → падает по registration timeout.

**Что проверять**:
1. На проблемном broker-хосте:
   ```bash
   grep -E "controller.quorum.voters" /mnt/logs/dbms/kafka-broker.out.log | head -1
   grep -E "Shutting down because we were unable|still don't know the high water mark" /mnt/logs/dbms/kafka-broker.out.log | tail -5
   ```
2. На каждом controller-хосте:
   ```bash
   grep -E "node id .* must be included in the set of voters" /mnt/logs/dbms/kafka-controller.out.log | head -1
   grep -E "ControllerServer.*Transition|Kafka Server started|elected as leader" /mnt/logs/dbms/kafka-controller.out.log | tail -5
   ```
3. Сравнить `node.id` каждого controller'а с `controller.quorum.voters` на broker-хостах.
   Каждый controller с `process.roles=controller` обязан быть в voters.

**Фикс**: синхронизировать `controller.quorum.voters` на всех хостах (brokers + controllers)
так, чтобы все живые controller'ы были в voters, а выведенные из кворума — не были.
При миграции ДЦ controller'ов (типовой сценарий: `kc` → `ec`) обновить voters одновременно
на broker-хостах и на оставшихся controller-хостах; на выводимом controller-хосте либо
перевести `process.roles` в `broker`, либо погасить и удалить хост.

**Норма, не ошибка**: в `kafka-controller.out.log` записи
`NotControllerException: The active controller appears to be node XXXX` — это переизбрание
лидера кворума, не падение. `QuorumState` transitions `Leader → ResignedState → FollowerState`
тоже норма.

## Fenced брокер

**Симптом**: брокер зарегистрировался, но controller его "заборнил" (fenced). В UI может
помечаться как dead.

**Проверка** на controller-хосте:
```bash
curl -s 'http://localhost:7777/jolokia/read/kafka.controller:name=FencedBrokerCount,type=KafkaController'
```

Если `Value > 0` — есть fenced брокеры. Смотреть `kafka-controller.out.log` на предмет
`Fencing broker` событий.

## Under-replicated partitions

**Симптом**: rscheck возвращает статус "Has N under-replicated partitions" (не "Broker is dead").
UI может показывать warning.

**Проверка** на broker-хосте:
```bash
curl -s 'http://localhost:7777/jolokia/read/kafka.server:type=ReplicaManager,name=UnderReplicatedPartitions'
```

`Value > 0` — есть under-replicated партиции.

## Растущий follower lag при UnderReplicatedPartitions=0 (ранняя стадия)

**Симптом**: панель «Broker Max Lag» в Grafana (`kafka_server_replicafetchermanager_maxlag`)
растёт на конкретном брокере, но `UnderReplicatedPartitions=0`, `IsrShrinksPerSec=0`,
`NetworkProcessorAvgIdlePercent`/`RequestHandlerAvgIdlePercent` высокие (тред-пулы не
исчерпаны). Лаг не упирается в сеть/CPU/memory.

**Корень**: `num.replica.fetchers=1` (дефолт) — один fetcher-тред на пару (этот брокер →
лидер X) последовательно обслуживает все партиции лидера X. При высокой нагрузке на
топик fetcher не успевает обойти партию за `replica.fetch.wait.max.ms` — lag копится,
реплика на грани выпадения из ISR (`replica.lag.time.max.ms`, 30s по умолчанию).

**Проверка** (только через JMX exporter 8080, **не Jolokia** — MBean `ReplicaFetcherManager,name=MaxLag`
через 7777 отдаёт `InstanceNotFoundException`):
```bash
# Общий MaxLag по брокеру:
curl -s --max-time 15 http://localhost:8080/metrics | grep '^kafka_server_replicafetchermanager_maxlag'

# Per-partition лаг (какие партии отстают):
curl -s --max-time 15 http://localhost:8080/metrics \
  | grep '^kafka_server_fetcherlagmetrics_consumerlag' | grep -v ' 0.0$' | sort -k2 -n -r | head -20

# MinFetchRate — нижняя граница скорости fetcher'ов:
curl -s --max-time 15 http://localhost:8080/metrics | grep '^kafka_server_replicafetchermanager_minfetchrate'
```

**Фикс**: в pms `kafka.broker.properties` поднять `num.replica.fetchers` (с 1 до 4 — рабочий
случай, MDBSUP-4649), затем `confp --oneshot` + поочерёдный рестарт брокеров.

Подробности и грабли (Jolokia vs JMX exporter, нумерация fetcher-тредов, шум mdb-tos) —
`history/MDBSUP-4649.md`. Второй подтверждённый кейс — MDBSUP-4737 (`auction-realtime-adtech-kafka`,
2026-08-21): «застрявший remove broker» при видимых ~70 Мбит/с приняли за троттлинг CC, но
throttle был пуст, reassignment завершён, kc-брокеры уже освобождены (0 replicas) — URP был
хроническим lag'ом от `num.replica.fetchers=1`. Разбор-чеклист «как отличить троттлинг CC от
fetcher-бутылочного-горлышка» — `history/MDBSUP-4737.md`. Сопутствующие метрики и таблица типов лага —
`kafka-metrics-investigator/commands/check_metrics.md` → «Follower lag» и «Дедупликация лагов».

Если при этом ещё и min ISR пробит — статус "Has N partitions with min in-sync replicas" +
`rank=RANK_PREFAIL`. Проверить:
```bash
curl -s 'http://localhost:7777/jolokia/read/kafka.server:type=ReplicaManager,name=UnderMinIsrPartitionCount'
```

## Offline partitions из-за удалённого брокера в Replicas

**Инцидент-референс**: MDBSUP-4166 (2026-07-24, кластер `spfrclustermdb-oneme-kafka`).

**Симптом** в Grafana (кластерные метрики за range 1h):
- `offline partitions count` > 0 (в инциденте — 27)
- `under replicated` > 0 (14)
- `at min ISR` > 0 (14)

При этом в UI mdb-data **все broker-хосты `AVAILABLE` / `observer`** — проблемных хостов нет.
MBean `UnderReplicatedPartitions` / `UnderMinIsrPartitionCount` / `AtMinIsrPartitionCount`
на отдельных брокерах при этом **могут быть 0** — метрика локальная (только партиции, hosted на
данном брокере), а проблема кластерная.

**Диагноз через `kafka-topics --describe --unavailable-partitions`** (bootstrap-server = **FQDN**
брокера, не `localhost` — иначе `SslAuthenticationException: No subject alternative DNS name
matching localhost found`):

```
Topic: nnPlatformResultsLog  Partition: 3  Leader: none  Replicas: 22026,20019,21023  Isr: 22026
Topic: hashCalculationResult Partition: 6  Leader: none  Replicas: 22026,20012,21016  Isr: 22026
...
```

Паттерн: у всех offline партиций **один и тот же broker id** (здесь `22026`) в `Replicas`,
и **только он в `Isr`**. Этот broker:
- preferred leader для всех проблемных партиций;
- единственная ISR-реплика — другие реплики ранее выпали из ISR;
- **недоступен** (хост удалён из mdb-data, `mcc` через скилл [`mcc-host-worker`](../../mcc-host-worker/SKILL.md) не подключается с `NamespaceMissingException`).

Так как `unclean.leader.election.enable=false` (default в Kafka 3.x) и `min.insync.replicas=2`,
controller не может выбрать лидера из не-ISR реплик → партиции висят `Leader: none`.

**Отличие от "Fenced брокер"**: fenced брокер жив, но controller его временно изолировал.
Здесь брокер физически удалён из инфраструктуры, но остался в metadata Kafka как preferred
leader для части партиций.

### Фикс (два этапа)

#### Этап 1: Unclean leader election — вернуть лидера

```bash
sudo -u kafka /opt/kafka/bin/kafka-leader-election.sh \
  --bootstrap-server <broker-fqdn>:9092 \
  --admin.config /opt/kafka/config/client.properties \
  --election-type unclean \
  --all-topic-partitions
```

**Нюанс Kafka 3.8**: `kafka-leader-election.sh` принимает **`--admin.config`**, а не
`--command-config` (как `kafka-topics.sh`). С `--command-config` —
`UnrecognizedOptionException`.

`--all-topic-partitions` для `UNCLEAN` безопасно для здоровых партиций — election запускается
только там, где лидера нет.

**Маркер успеха**: `Successfully completed leader election (UNCLEAN) for partitions ...`.

**Важно**: unclean election может терять данные — сообщения, записанные на мёртвом брокере и
не успевшие реплицироваться, утеряны. Но партиции уже offline, данные и так недоступны.

#### Этап 2: Убрать мёртвый broker id из Replicas через reassign

После unclean election брокер остаётся в `Replicas` как мёртвая реплика (в Isr его уже нет).
Полный разбор — скилл `kafka-reassign-partitions`, `history/MDBSUP-4166.md`.

Кратко:
1. `kafka-topics --describe | grep "Replicas:.*<dead_broker_id>"` — собрать все партиции.
2. Сгенерировать `reassign.json`: заменить мёртвый broker id на живого брокера из того же ДЦ
   (для сохранения cross-DC схемы), не занятого в текущих Replicas этой партицы.
3. `kafka-reassign-partitions.sh --execute` **без `--throttle`** (с throttle может упасть с
   `TimeoutException` на `incrementalAlterConfigs` — см. ниже грабли).
4. `--verify` до `completed` для всех партиций.

Reassign проходит мгновенно, т.к. мёртвый брокер не содержал уникальных данных, а новые реплики
уже в ISR после unclean election — Kafka просто обновляет metadata.

### Грабли

1. **`localhost:9092` не работает** — SSL-сертификат не содержит `localhost` в SAN. Использовать
   FQDN брокера.
2. **`--under-min-isr` не поддерживается в Kafka 3.8** у `kafka-topics.sh`. Использовать
   `--at-min-isr-partitions` (ISR == min.insync.replicas, на грани) и
   `--under-replicated-partitions` (ISR < RF, включает under-min-isr как подмножество).
3. **`--command-config` vs `--admin.config`**: у `kafka-topics.sh` и `kafka-reassign-partitions.sh`
   — `--command-config`, у `kafka-leader-election.sh` — `--admin.config`.
4. **Timeout на `incrementalAlterConfigs` при `--throttle`**: `--execute --throttle N` вызывает
   `incrementalAlterConfigs` для установки throttle через controller. Если controller перегружен
   или metadata quorum медленный — `TimeoutException` за 60 сек. Reassign при этом не
   запускается. Решение — `--execute` без `--throttle`, либо повторить позже.
5. **`grep "^Topic:"` не матчит строки партиций** — перед `Topic:` в строках партиций есть
   табуляция. Использовать `grep "Replicas:.*<broker_id>"` или `grep -E "^\s*Topic:"`.
6. **tcl/expect + Python `[...]`**: `expect -c '...'` интерполирует `[...]` как tcl command
   substitution. List comprehensions ломаются. Решение — base64-кодировать python-скрипт
   локально, отправить через `echo '<b64>' | base64 -d > /tmp/script.py`.
   Подробнее про грабли Tcl/expect — [`mcc-host-worker/commands/pitfalls.md`](../../mcc-host-worker/commands/pitfalls.md).

### Похожий сценарий — Cruise Control не убирает мёртвый брокер

В инциденте MDBSUP-4166 на кластере был Cruise Control (видно по устаревшему throttle-config в
`oneme_antispam_pr_idsEntityFacts`), но он **не убрал** 22026 из Replicas автоматически.
`ReassigningPartitions` MBean = 0. Вероятно, CC не настроен на auto-removal dead brokers, либо
был отключён. Если CC есть, но проблема не устраняется — действовать вручную по этапу 2.
