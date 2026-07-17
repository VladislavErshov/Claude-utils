# Скачивание и анализ логов Kafka

## Скачать логи со всех хостов

Пользователь даёт список хостов. Пример:

```
BROKERS=(
  1.broker.<cluster>.hc.one-infra.ru
  1.broker.<cluster>.kc.one-infra.ru
  1.broker.<cluster>.pc.one-infra.ru
)
CONTROLLERS=(
  1.controller.<cluster>.hc.one-infra.ru
  1.controller.<cluster>.kc.one-infra.ru
  1.controller.<cluster>.pc.one-infra.ru
)
CRUISE=(
  1.cruise.<cluster>.hc.one-infra.ru
)
```

### Брокеры и контроллеры

```bash
mkdir -p ~/kafka_logs/brokers ~/kafka_logs/controllers

for H in "${BROKERS[@]}"; do
  D=~/kafka_logs/brokers/$H
  mkdir -p "$D"
  mcc scp "$H:/mnt/logs/dbms/" "$D/" 2>&1 | head -3
done

for H in "${CONTROLLERS[@]}"; do
  D=~/kafka_logs/controllers/$H
  mkdir -p "$D"
  mcc scp "$H:/mnt/logs/dbms/" "$D/" 2>&1 | head -3
done
```

### Cruise Control (если есть в кластере)

```bash
mkdir -p ~/kafka_logs/cruise
for H in "${CRUISE[@]}"; do
  D=~/kafka_logs/cruise/$H
  mkdir -p "$D"
  mcc scp "$H:/mnt/logs/dbms/" "$D/" 2>&1 | head -3
done
```

⚠️ Путь именно `/mnt/logs/dbms` (с 's' в `logs`). Опечатка `/mnt/log/dbms` даёт
`failed to read downloaded archive header: EOF`.

⚠️ `mcc scp` иногда падает с `SSL Handshake is not finished` — просто повторить
команду для проблемного хоста через 1-2 секунды.

## Структура скачанных логов

В каждой директории хоста:
- `kafka-broker.out.log` — stdout брокера (основной лог)
- `kafka-broker.err.log` — stderr брокера (warnings, ошибки)
- `kafka-controller.out.log` — stdout controller'а (или `KAFKA_ROLE is not set to CONTROLLER. Exiting.` на broker-хосте)
- `kafka-controller.err.log` — stderr controller'а
- `kafka-exporter.out.log`, `kafka-exporter.err.log` — kafka_exporter (consumer metrics)
- `cruise-control.out.log`, `cruise-control.err.log` — Cruise Control (на cruise-хосте)

## Что искать в kafka-broker.out.log

### Успешный старт брокера

```bash
grep -E "Transition from|registered broker|Kafka Server started" ~/kafka_logs/brokers/<host>/kafka-broker.out.log | head -10
```

Маркеры успеха:
- `[BrokerServer id=XXXXX] Transition from SHUTDOWN to STARTING`
- `[BrokerLifecycleManager id=XXXXX] Successfully registered broker XXXXX with broker epoch YYY`
- `[BrokerServer id=XXXXX] Transition from STARTING to STARTED`
- `[KafkaRaftServer nodeId=XXXXX] Kafka Server started`

### InvalidReplicationFactorException

```bash
grep -E "InvalidReplicationFactor|registered" ~/kafka_logs/brokers/<host>/kafka-broker.out.log | head -20
```

Маркер:
```
Unable to replicate the partition 3 time(s): The target replication factor of 3 cannot be reached
because only N broker(s) are registered
```

Это означает, что не все брокеры успели зарегистрироваться. `N` показывает сколько брокеров
кворум видит на момент попытки создать topic.

### `<unresolved>` controller hostname

```bash
grep "unresolved" ~/kafka_logs/brokers/<host>/kafka-broker.out.log | head -5
```

Если в логе только в момент initialization — нормально. Брокер позже успешно резолвит.
Если `Successfully registered broker` есть ниже — DNS работает.

### CruiseControlMetricsReporter не подключается

```bash
grep "CruiseControlMetricsReporter" ~/kafka_logs/brokers/<host>/kafka-broker.out.log | head -20
```

Маркеры проблем:
- `WARN [Producer clientId=CruiseControlMetricsReporter] Connection to node -1 (...) could not be established`
- `App info kafka.admin.client for CruiseControlMetricsReporter unregistered` (сразу после старта — репортер упал)

### KRaft quorum / voters

```bash
grep -E "Starting voters|VoterSet|Recorded new KRaft controller" ~/kafka_logs/brokers/<host>/kafka-broker.out.log | head -10
```

Покажет список voters (`[10001, 11001, 12001]`) и какого controller'а брокер выбрал лидером.

## Что искать в kafka-controller.out.log

### На broker-хосте (должно быть)

```
KAFKA_ROLE is not set to CONTROLLER. Exiting.
```

Это нормально — на broker-хосте controller-процесс не запускается. Лог `kafka-controller.out.log`
содержит только эту строку.

### На controller-хосте

```bash
grep -E "ControllerServer|registered|ActiveControllerCount|elected" ~/kafka_logs/controllers/<host>/kafka-controller.out.log | head -20
```

Маркеры:
- `[ControllerServer id=XXXXX] Transition from SHUTDOWN to STARTING`
- `[RaftManager id=XXXXX] ... elected as leader` — этот controller стал активным лидером
- `Kafka Server started`

## Что искать в cruise-control.err.log

### Java version mismatch

```bash
grep -E "UnsupportedClassVersionError|LinkageError|class file version" ~/kafka_logs/cruise/<host>/cruise-control.err.log | head -5
```

Маркер:
```
java.lang.UnsupportedClassVersionError: com/linkedin/kafka/cruisecontrol/KafkaCruiseControlMain
has been compiled by a more recent version of the Java Runtime (class file version 61.0),
this version of the Java Runtime only recognizes class file versions up to 55.0
```

Class file version:
- 55.0 = Java 11
- 61.0 = Java 17

Если CC собран под Java 17, а в образе Java 11 — обновить `openjdk-11-jdk` → `openjdk-17-jdk`
в `ubuntu20-mdb-cruisecontrol-base/rootfs/docker/build.d/10-install-java.sh`.

### CruiseControl.out.log — только Jolokia

```bash
cat ~/kafka_logs/cruise/<host>/cruise-control.out.log
```

Если в `out.log` только стартовые сообщения Jolokia (`I> No access restrictor found`,
`Jolokia: Agent started with URL ...`) и ничего больше — CC main class даже не загрузился.
Смотреть `err.log` на `UnsupportedClassVersionError` / `LinkageError` / `ClassNotFoundException`.

## Проверка после фикса

После пересборки образа и передеплоя — скачать логи заново и проверить:
1. В `kafka-broker.out.log` есть `Kafka Server started` И нет `InvalidReplicationFactorException` (или только в момент старта).
2. На cruise-хосте в `cruise-control.err.log` нет `UnsupportedClassVersionError`.
3. На cruise-хосте в `cruise-control.out.log` есть логи CC main (не только Jolokia).

## Очистка

```bash
rm -rf ~/kafka_logs
```
