# Администрирование Kafka

Рутинные операции администрирования: проверка видимости брокеров, удаление контроллеров,
unregister брокеров, управление пользователями / топиками / ACL / consumer groups.

Все скрипты лежат в `/opt/kafka/bin`. Для выполнения любого скрипта достаточно зайти на
любой **брокер**. Для VK-кластеров в `/opt/kafka/config/client.properties` добавить:

```properties
ssl.endpoint.identification.algorithm=
ssl.truststore.type=PEM
ssl.truststore.location=/opt/kafka/ssl/tls_ca.crt
```

## Перед началом

Все kafka-скрипты — в `/opt/kafka/bin`. Для выполнения любого из скриптов достаточно зайти
на любой **брокер**. Для VK-кластеров в `/opt/kafka/config/client.properties` добавить:

```properties
ssl.endpoint.identification.algorithm=
ssl.truststore.type=PEM
ssl.truststore.location=/opt/kafka/ssl/tls_ca.crt
```

## Как проверить, что брокеры видят друг друга

```bash
/opt/kafka/bin/kafka-metadata-quorum.sh --bootstrap-server $cloud_hostname:9092 \
  --command-config /opt/kafka/config/client.properties describe --status
```

Пример вывода:
```
ClusterId:              4acc7529-30a0-4cc4-b888-96e545b03a69
LeaderId:               10003
LeaderEpoch:            1
HighWatermark:          293860
MaxFollowerLag:         0
MaxFollowerLagTimeMs:   326
CurrentVoters:          [10001,10002,10003]   # здесь должны быть id всех контроллеров
CurrentObservers:       [20001,20002,20003]   # здесь должны быть id всех брокеров (достаточно сверить кол-во)
```

## Контроллеры

### Удалить контроллер

1. Идём в pms, правим проперти `kafka.controller.quorum`, удалив из списка удаляемый
   контроллер. **Важно!** Айдишники остальных не трогаем.
2. Делаем смену лидера контроллера, если он удаляемый:
   ```bash
   systemctl restart kafka-controller
   ```
3. Рестартим контроллеры и брокеры (можно таской `update-config` оператора). Лидер-контроллер
   рестартим **последним** среди контроллеров.
4. Удаляем контроллер из облака.

## Брокеры

### Unregister

```bash
/opt/kafka/bin/kafka-cluster.sh unregister --bootstrap-server $cloud_hostname:9092 \
  --config /opt/kafka/config/client.properties --id <broker-id>
```

## Пользователи

### Создание

Создать можно из UI, указав `username`, `password`, `acl`, `quota`.

Создать/изменить из консоли:
```bash
/opt/kafka/bin/kafka-configs.sh --bootstrap-server $cloud_hostname:9092 \
  --command-config /opt/kafka/config/client.properties --alter \
  --add-config 'SCRAM-SHA-256=[iterations=8192,password=<password>]' \
  --entity-type users --entity-name <username>
```

### Как можно проверить

```bash
/opt/kafka/bin/kafka-configs.sh --bootstrap-server $cloud_hostname:9092 \
  --command-config /opt/kafka/config/client.properties --describe --entity-type users | grep <username>
```

### Удалить

⚠️ **Не забыть также удалить из таблицы `users` в backstage.**

1. Удалить все связанные с пользователем ACL:
   ```bash
   /opt/kafka/bin/kafka-acls.sh --bootstrap-server $cloud_hostname:9092 \
     --command-config /opt/kafka/config/client.properties --remove \
     --allow-principal User:<USERNAME> --topic <topic-name>

   /opt/kafka/bin/kafka-acls.sh --bootstrap-server $cloud_hostname:9092 \
     --command-config /opt/kafka/config/client.properties --remove \
     --allow-principal User:<USERNAME> --group '*'
   ```

2. Удалить квоты пользователя:
   ```bash
   /opt/kafka/bin/kafka-configs.sh --bootstrap-server $cloud_hostname:9092 \
     --command-config /opt/kafka/config/client.properties --alter \
     --entity-type users --entity-name <USERNAME> --delete-config producer_byte_rate

   /opt/kafka/bin/kafka-configs.sh --bootstrap-server $cloud_hostname:9092 \
     --command-config /opt/kafka/config/client.properties --alter \
     --entity-type users --entity-name <USERNAME> --delete-config consumer_byte_rate
   ```
   Получение `Invalid config(s): producer_byte_rate` или `Invalid config(s): consumer_byte_rate`
   говорит о том, что квот нет и можно переходить к удалению.

3. Удалить пользователя:
   ```bash
   /opt/kafka/bin/kafka-configs.sh --bootstrap-server $cloud_hostname:9092 \
     --command-config /opt/kafka/config/client.properties --alter \
     --delete-config 'SCRAM-SHA-256' --entity-type users --entity-name <USERNAME>
   ```

## Топики

### Просмотреть все топики

```bash
/opt/kafka/bin/kafka-topics.sh --bootstrap-server $cloud_hostname:9092 \
  --command-config /opt/kafka/config/client.properties --list
```

### Просмотреть проблемные партиции

```bash
/opt/kafka/bin/kafka-topics.sh --bootstrap-server $cloud_hostname:9092 \
  --command-config /opt/kafka/config/client.properties --describe --unavailable-partitions
```

### Сделать UNCLEAN перевыборы (с потерей данных)

```bash
/opt/kafka/bin/kafka-leader-election.sh --bootstrap-server $cloud_hostname:9092 \
  --admin.config /opt/kafka/config/client.properties \
  --election-type UNCLEAN --topic <topic-name> --partition <partition-number>
```

### Создание

Создать можно из UI.

```bash
/opt/kafka/bin/kafka-topics.sh --create --bootstrap-server $cloud_hostname:9092 \
  --command-config /opt/kafka/config/client.properties \
  --replication-factor <replication-factor> \
  --partitions <number-of-partitions> \
  --topic <topic-name> \
  --config cleanup.policy=delete \
  --config retention.ms=86400000 \
  --config leader.replication.throttled.replicas='*' \
  --config follower.replication.throttled.replicas='*'
```

`replication.factor=3` ставим по умолчанию. `retention.ms` — настраиваемый (по умолчанию
1 день — `86400000`). Остальные `--config` скорее всего менять не придётся.

### Как изменить топик

⚠️ **Не забыть изменить значения настроек в БД:** таблица `databases`, поле `settings`.

Увеличить количество партиций (можно только увеличить):
```bash
/opt/kafka/bin/kafka-topics.sh --bootstrap-server $cloud_hostname:9092 \
  --command-config /opt/kafka/config/client.properties --alter --if-exists \
  --partitions <number-of-partitions> --topic <topic-name>
```

Изменить настройки конфигурации:
```bash
/opt/kafka/bin/kafka-configs.sh --bootstrap-server $cloud_hostname:9092 \
  --command-config /opt/kafka/config/client.properties --alter \
  --entity-type topics --entity-name <topic-name> \
  --add-config retention.ms=86400000,cleanup.policy=delete,...
```

### Как можно проверить

```bash
/opt/kafka/bin/kafka-topics.sh --bootstrap-server $cloud_hostname:9092 \
  --command-config /opt/kafka/config/client.properties --describe --topic <topic-name>
```

### Как удалить

⚠️ **Не забыть также удалить из таблицы `databases` в backstage.**

```bash
/opt/kafka/bin/kafka-topics.sh --bootstrap-server $cloud_hostname:9092 \
  --command-config /opt/kafka/config/client.properties --delete --topic <topic-name>
```

## ACL

### Создание

Создать можно из UI.

Чаще всего нужно навесить ACL на чтение/запись определённого топика. Подробное описание
доступных операций: https://docs.confluent.io/platform/current/security/authorization/acls/overview.html

```bash
/opt/kafka/bin/kafka-acls.sh --bootstrap-server $cloud_hostname:9092 \
  --command-config /opt/kafka/config/client.properties --add \
  --allow-host '*' --allow-principal User:<user> \
  --operation Read --operation Write --topic <topic-name>
```

Если пользователю нужно ACL на чтение — проверить, что есть ACL на `Group` (позволяет читать
топики, используя любой consumer id). Проверяем:
```bash
/opt/kafka/bin/kafka-acls.sh --bootstrap-server $cloud_hostname:9092 \
  --command-config /opt/kafka/config/client.properties --list --principal User:<user>
```
Если нет — выполнить:
```bash
/opt/kafka/bin/kafka-acls.sh --command-config /opt/kafka/config/client.properties \
  --bootstrap-server $cloud_hostname:9092 --add \
  --allow-host '*' --allow-principal User:<user> --operation Read --group '*'
```

Если пользователю нужно выдать права на создание топиков (включена настройка
`auto.create.topics.enable`):
```bash
/opt/kafka/bin/kafka-acls.sh --bootstrap-server $cloud_hostname:9092 \
  --command-config /opt/kafka/config/client.properties --add \
  --allow-host '*' --allow-principal User:<user> --operation Create --topic '*'
```

### Как можно проверить

```bash
/opt/kafka/bin/kafka-acls.sh --bootstrap-server $cloud_hostname:9092 \
  --command-config /opt/kafka/config/client.properties --list --principal User:<user>
```

### Как удалить

⚠️ **Не забыть также удалить из таблицы `permissions` в backstage.** После удаления
проверить на брокере, что удаление действительно произошло (командой выше).

```bash
/opt/kafka/bin/kafka-acls.sh --bootstrap-server $cloud_hostname:9092 \
  --command-config /opt/kafka/config/client.properties --remove \
  --allow-principal User:<USERNAME> --operation Read --topic <topic-name>
```

## Consumer groups

### Просмотр всех групп

```bash
/opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server $cloud_hostname:9092 \
  --command-config /opt/kafka/config/client.properties --list
```

### Проверяем состояние группы

Узнать текущее состояние группы и текущий lag:
```bash
/opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server $cloud_hostname:9092 \
  --command-config /opt/kafka/config/client.properties --describe --group <group>
```

Узнать state группы и текущего координатора:
```bash
/opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server $cloud_hostname:9092 \
  --command-config /opt/kafka/config/client.properties --describe --state --group <group>
```

### Как удалить

Получаем текущий state группы — для удаления должно быть `State: Empty; Members: 0`. Если
это не так — просим сначала остановить консьюмеров.
```bash
/opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server $cloud_hostname:9092 \
  --command-config /opt/kafka/config/client.properties --describe --state --group <group>
```

Удаление:
```bash
/opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server $cloud_hostname:9092 \
  --command-config /opt/kafka/config/client.properties --delete --group <group>
```
