# Разбор проблем Kafka

Каталог типовых проблем из дежурного ранбука. Базовые команды и доступность — `runbook.md`,
операции с Cruise Control — `cruise_control_ops.md`, администрирование топиков/ACL/users —
`administration.md`.

Базовые паттерны доступа к хостам и грабли Tcl/SSL —
скилл [`mcc-host-access`](../../mcc-host-access/SKILL.md). Специфичные для Kafka скрипты на expect
(например `runner.exp` для очистки `*-stray` партиций) — ниже.

Каталог известных инцидентов и технических багов (Broker is dead, InvalidReplicationFactor,
FencedBroker, CruiseControlMetricsReporter) — `known_issues.md`.

## Приходят с вопросом создания топика

Пользователи часто не знают, с какими параметрами создавать топик. Главное — подобрать
кол-во партиций. В UI нужно указать: название, кол-во партиций, фактор репликации,
retention (время хранения после записи).

### Расчёт количества партиций

Отталкиваемся от ожидаемого рейта записи. Учитываем, что размер 1 партиции не должен
превышать **50 ГБ** (значение зависит от размера кластера, см. ниже).

При ретеншене 1 сутки максимальный рейт в партицию:
```
50 * 1024 / (24ч * 3600с) ≈ 0.6 МБ/c
```
→ Если нужен рейт ~2 МБ/с — указываем 4 партиции.

### Рекомендации по размеру партиции

| Размер кластера | Размер партиции |
| --- | --- |
| Маленький (до 300–500 ГБ) | 5–20 ГБ |
| Средний (500 ГБ – 1 ТБ) | 20–40 ГБ |
| Большой (свыше 1 ТБ) | 40–60 ГБ, для очень больших — до 150 ГБ |

## Не могу подключиться

1. Проверить состояние кластера — `operator is fresh`, ноды живые.
2. Проверить сетевую доступность:
   ```bash
   telnet 1.broker.<cluster>.<dc>.one-infra.ru 9092
   ```
   (заменить на 1 из брокеров в `bootstrap.servers`).
3. Проверить, что конфиг подключения идентичен указанному в доке (там же наиболее частые
   проблемы).
4. Проверить версию SSL протокола. Должна быть `TLSv1.3`. Если используется другая:
   - либо попросить включить её на клиенте,
   - либо если клиент не поддерживает — зайти в pms брокера и контроллера, после
     `ssl.truststore.password` добавить `ssl.protocol=TLSv1.2` (или используемую клиентом),
     сделать рестарт кластера.
5. Если не помогает — в личку.

## Не могу читать топик

1. Проверить, что вообще подключение выполняется (см. предыдущий пункт).
2. Проверить ACL на чтение топика (в UI):
   ```
   aclOperation: Read
   resourceType: topic
   host: *
   permissionType: allow
   resourceName: <topicName>
   ```
3. Проверить ACL на consumer group (в UI):
   ```
   aclOperation: Read
   resourceType: group
   host: *
   permissionType: allow
   resourceName: *
   ```
   **Самая частая проблема — отсутствие ACL на группу.**

## Просят скинуть сэмпл сообщений

1. Зайти на любой из брокеров.
2. Для чтения нужен пользователь. Если уже есть юзер с правами на чтение топика —
   использовать его (пароль в волте). Иначе создать тестового юзера:

   ```bash
   /opt/kafka/bin/kafka-configs.sh --bootstrap-server $cloud_hostname:9092 \
     --command-config /opt/kafka/config/client.properties --alter \
     --add-config 'SCRAM-SHA-256=[iterations=8192,password=<password>]' \
     --entity-type users --entity-name test-user

   /opt/kafka/bin/kafka-acls.sh --bootstrap-server $cloud_hostname:9092 \
     --command-config /opt/kafka/config/client.properties --add \
     --allow-host '*' --allow-principal User:test-user --operation Read --topic <topic>

   /opt/kafka/bin/kafka-acls.sh --bootstrap-server $cloud_hostname:9092 \
     --command-config /opt/kafka/config/client.properties --add \
     --allow-host '*' --allow-principal User:test-user --operation Read --group '*'
   ```

3. Отредактировать `/opt/kafka/config/consumer.properties`:
   ```properties
   bootstrap.servers=<HOST>:9092
   security.protocol=SASL_SSL
   sasl.mechanism=SCRAM-SHA-256
   sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username="test-user" password="<password>";
   group.id=test-consumer-group
   ```

4. Выполнить (если сообщения отправлялись до текущего момента — добавить `--from-beginning`):
   ```bash
   /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server <HOST>:9092 \
     --consumer.config /opt/kafka/config/consumer.properties \
     --topic <topic> --max-messages 10
   ```
5. Отправить данные в лс.

## Перевести кластер на SASL_PLAINTEXT

По умолчанию на создаваемых кластерах включён SSL. Некоторые клиенты просят PLAINTEXT.

### Конфиг брокеров
- `listener.security.protocol.map` для всех listeners выставить в `SASL_PLAINTEXT`.
- Убрать строки `ssl.*` (их всего 4).
- Если есть cruise — в конце изменить:
  ```
  cruise.control.metrics.reporter.security.protocol=SASL_PLAINTEXT
  ```

### Конфиг контроллеров
- `listener.security.protocol.map` для всех listeners выставить в `SASL_PLAINTEXT`.
- Убрать строки `ssl.*` (их 4).

### Общее
- В конфиге `ssl.enabled` выставить `false`.
- В конфиге круиза (если есть): `security.protocol=SASL_PLAINTEXT`.
- В конфиге оператора добавить: `kafkaAdminSecurityProtocol=SASL_PLAINTEXT`.

Сделать рестарт кластера и круиза.

## Не работает экспортер на PLAINTEXT кластере

При PLAINTEXT-авторизации экспортер иногда пытается заходить по SSL. Заменить в
`/etc/systemctl/system/kafka-exporter.service` `ExecStartPre=...` и `ExecStart=...` на:

```ini
# ExecStartPre - удаляем
ExecStart=/bin/bash -c '/opt/kafka-exporter/kafka_exporter --kafka.server=$cloud_hostname:9092 --kafka.version=$KAFKA_VERSION --sasl.mechanism=plain --web.listen-address=:23569'
```

После:
```bash
systemctl daemon-reload
systemctl restart kafka-exporter
```

## Просят создать PLAIN пользователя

В базу таких пользователей **не добавляем**, чтобы не давать возможность изменить юзера —
все пользователи из UI это scram.

1. В конфиге через `;` добавить имя создаваемого пользователя.
2. В волте в директории `/users` создать пользователя по аналогии с другими.
3. Сделать рестарт кластера.
4. Навесить ACL на пользователя (см. `administration.md`).

## Кончилось место на брокерах

### Случай 1: прерывание ребаланса Cruise Control или reassign → остались `-stray` партиции

Перебрать хосты × ДЦ через скилл [`mcc-host-access`](../../mcc-host-access/SKILL.md)
(команда `ssh`). Шаблон хоста: `$i.broker.<cluster>.<dc>.one-infra.ru`
(`i=1..75`, `dc=hc,kc,pc`). На каждом хосте выполнить:

```bash
cd /mnt/data/log
du -sk *-stray 2>/dev/null | awk '{s+=$1} END {print s+0}'
rm -rf *-stray
```

Сделать рестарт хостов, на которых была ошибка (иначе память может долго не обновляться).

### Случай 2: в остальных случаях

Если место уже закончилось — брокеры умерли, таски оператора не запустятся. Таска ресайза
должна отработать. Если нет — руками добавить диск, затем на брокерах:
```bash
systemctl restart kafka-broker
```

Можно почистить файлы вручную, если докинуть диск нельзя:

1. Зайти по ssh на брокер.
2. `cd /mnt/data/log`
3. Смотрим наиболее заполненные партиции:
   ```bash
   du -sh * | sort -h
   ```
4. `cd <название партиции>`, например `cd oneme_core_OtherEvents-90`.
5. В UI смотрим `retention.ms` для этого топика (например `237600000`).
6. Перевести мс в мин: `237600000 мс = 3960 мин`.
7. Смотрим метаданные партиции, **результат сохранить**:
   ```bash
   cat partition.metadata
   ```
8. Удалить все файлы старше N минут:
   ```bash
   find . -type f -mmin +3960 -delete
   ```
9. `vim partition.metadata` — восстановить содержимое из шага 7.
10. `cat partition.metadata` — проверить сохранность.
11. `systemctl restart kafka-broker`.

### Выяснить причину забившихся дисков

- Некоторые выставляют retention в несколько суток/недель — узнать, зачем столь долгое
  хранение. При необходимости уменьшить `retention.ms` для топиков.
- Возросшая нагрузка — увеличить диск / кол-во брокеров.
- Если после увеличения диска ничего не происходит — посмотреть сеть на лидере
  контроллеров. Возможно, не хватает сети.

## Кончилось место в логах на брокерах

Скорее всего, docker-образ с багом логротейта.

Сначала чистим логи, перезапускаем кафку. С забитым диском логов кафка может не подняться
на новом образе. Зайти на хост через скилл [`mcc-host-access`](../../mcc-host-access/SKILL.md)
и выполнить:

```bash
systemctl stop kafka-broker
cd /mnt/logs/dbms
rm kafka-*
systemctl start kafka-broker
```

Далее — по инструкции с консьюмером метрик (проверить, что в манифесте есть нужные поля и
`kafka_exporter` в волте, см. «Перевести кластер на актуальную версию с consumer lag
метриками»). Если всё есть — запустить таск `kafka.update` на операторе. Актуальную версию
смотреть на https://mdb.kaizen.idzn.ru/dockerTags.

## Ошибки вида: Connection timed out

1. Зайти в мониторинг, проверить график ошибок.
2. Если всплесков нет — посмотреть графики рейта записи/чтения и latency. Если на графиках
   `Response Send Time` резкий всплеск — вероятнее всего проблема сетевая. Зайти на хост,
   проверить `telnet` до соседних брокеров / до сервиса клиента.
3. Сделать `describe` топика, посмотреть состояние партиций. Все должны иметь кол-во реплик
   в состоянии `insync == min.insync.replicas` (по умолчанию 2).
4. Возможно, на некоторых брокерах упираемся в лимиты сети. Возможен вариант, когда брокер
   лежал и после восстановления активно наливает данные — некоторые партиции могут
   переходить в `insync`. Почти все клиенты при записи ждут `ack` от всех insync реплик.
   Если достигнут лимит сети, подтверждение от реплики на данном брокере периодически не
   происходит. **Временно увеличить сеть.**

## Зависла таска создания пользователя

Зависает с ошибкой вида `cluster.metadata is not supported SCRAM`. Встречается редко,
в основном когда кластер не был создан из-за нехватки квоты.

Выполнить на одном из брокеров:
```bash
/opt/kafka/bin/kafka-features.sh --command-config /opt/kafka/config/client.properties \
  --bootstrap-server $cloud_hostname:9092 upgrade --metadata 3.8
```

## Перевести кластер на актуальную версию (с consumer lag метриками)

Актуальную версию смотреть на https://mdb.kaizen.idzn.ru/dockerTags — выбираем версию,
умеющую отдавать метрики consumer group.

Добавить в vault соответствующего кластера секрет `kafka_exporter`:
- ключ — `password`
- значение — сгенерировать пароль из 20 символов.

Пример: https://vault.idzn.io/ui/vault/secrets/zkv/kv/list/mdb/mdbdev/kafka/fix-bug-mdbdev-kafka.mdbdev.db.production.mdb.prod/

**Первым делом обновить контроллеры до новой версии!**

В манифесте брокеров (контроллеры оставить те же):

1. Добавить в `env`:
   ```yaml
   env:
     prometheus_metrics_cfg=/metrics:8080;/metrics:23569
   ```
2. Убрать из `env`:
   ```yaml
   prometheus_port=8080
   prometheus_location=/metrics
   ```
3. Добавить порт:
   ```yaml
   '23569': lan,tcp
   ```

## Перераспределить партиции после добавления новых брокеров

Со всеми новыми кластерами уже создаётся Cruise Control — отправлять перераспределять
через него.

Для перераспределения нужен **запас диска** — кафка копирует все данные партиций на новые
брокеры. Можно проанализировать график Log size. Возможно, придётся перераспределять
поочерёдно для топиков, а не все сразу.

### Пошагово

1. Выполнить `kafka-metadata-quorum.sh` (см. `administration.md`), чтобы получить
   информацию о кластере. Сохранить id брокеров (будут в `CurrentObservers`).

2. Создать `topics.json` (перечислить топики для перераспределения). Если
   перераспределяем все топики — добавить также `__consumer_offsets`:
   ```json
   {
     "topics": [{"topic": "topicName1"},{"topic": "topicName2"}],
     "version": 1
   }
   ```

3. Сгенерировать план:
   ```bash
   /opt/kafka/bin/kafka-reassign-partitions.sh --bootstrap-server $cloud_hostname:9092 \
     --command-config /opt/kafka/config/client.properties \
     --topics-to-move-json-file topics.json \
     --broker-list "тут все id брокеров через запятую" --generate
   ```
   Сохранить результат в `reassign.json`.

4. Выполнить с throttle (bytes/sec; в примере 100 МБ/c). Если на кластере уже запущена
   балансировка — чтобы добавить партиции к ребалансировке, добавить флаг `--additional`:
   ```bash
   /opt/kafka/bin/kafka-reassign-partitions.sh --bootstrap-server $cloud_hostname:9092 \
     --command-config /opt/kafka/config/client.properties \
     --reassignment-json-file reassign.json --throttle 104857600 --execute
   ```

## Переезд кластеров дзена из rc в hc

По конфигам ориентироваться на пример кластера `events`.

1. `kafka.isWanCluster` для hc — `false`, для остальных дц — `true`.

2. В `kafka.broker.properties` для hc меняем строки:
   ```
   listeners = INTERNAL://:9092,WAN://:9093
   advertised.listeners = INTERNAL://{{ env('cloud_hostname') }}:9092,WAN://{{ env('cloud_hostname') }}:9093
   ```

3. В манифесте в hc убираем `wan`, открываем порт 9093:
   ```
   '9093': lan,tcp,started
   ```

## Брокер зависает в STARTING RESERVED (или контроллеры показывают fenced brokers)

При старте брокер запрашивает метаданные у контроллера. Если в это время превышены лимиты
сети — старт может занять долгое время. Смотреть на:

- сеть **out** на контроллере-лидере. Если в 100% — поднять.
- сеть **in** на брокере. Если в 100% — поднять.

## Метрики по io/network тредам (когда плохо идёт репликация)

Если наблюдается лаг репликации, а с лимитами сети всё ок — проверить, не упирается ли
брокер в количество io/network/replication тредов.

Под «рестартом» ниже понимается:
```bash
confp --oneshot
systemctl restart kafka-broker
```

### 1. Средний процент времени, когда I/O потоки бездействуют

Если значение < 30% — потоки перегружены. Увеличить `num.io.threads` (изменить в конфиге
брокера в pms и сделать рестарт).

Рекомендация:
```
num.io.threads = min(количество_ядер * 2, общее_число_партиций / 100)
```

```bash
curl http://localhost:7777/jolokia/read/kafka.network:type=SocketServer,name=NetworkProcessorAvgIdlePercent | jq
```

### 2. Загруженность потоков, обрабатывающих запросы

Если значение < 30% — потоки перегружены. Увеличить `num.network.threads`. Варианты:

- изменить в конфиге брокера и сделать рестарт;
- если брокер в prefail — можно выполнить (увеличивать только x2 от текущего):
  ```bash
  /opt/kafka/bin/kafka-configs.sh --bootstrap-server $cloud_hostname:9092 \
    --command-config /opt/kafka/config/client.properties \
    --entity-type brokers --entity-name <broker_id> \
    --alter --add-config num.network.threads=
  ```

```bash
curl http://localhost:7777/jolokia/read/kafka.server:type=KafkaRequestHandlerPool,name=RequestHandlerAvgIdlePercent | jq
```

### 3. UnderReplicatedPartitions и AtMinISRReplicas

Если значения высокие, при этом лаг на брокерах растёт (дашборд Brokers info → max lag) —
увеличить `num.replica.fetchers` (изменить в конфиге брокера в pms и сделать рестарт).

## На кластере ошибки ребалансировки consumer group

Обычно встречается после проблем с кластером/топиком. Чаще всего — затуп в координаторе
группы.

1. Заходим на любой брокер, смотрим состояние проблемной группы:
   ```bash
   /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server $cloud_hostname:9092 \
     --command-config /opt/kafka/config/client.properties --describe --state --group <group>
   ```
2. Скорее всего увидим `group is rebalancing` — смотрим на текущего координатора.
3. Делаем стоп этого брокера-координатора, ждём смены координатора на другой брокер.
4. Смотрим, ушли ли ошибки ребалансировки. Возвращаем брокер.
5. Может помочь остановить текущего лидер-контроллера, чтобы он сменился на соседний.
6. Важно: на контроллерах сеть не должна быть в полку — может быть проблема в передаче
   метаданных.

Если не помогло — могут быть проблемы на клиентах:
- Консьюмер по дефолту настроен на таймаут поллинга в 300 секунд. Если не вызван очередной
  `poll()` за этот промежуток — консьюмер считается выбывшим, инициируется перебалансировка.
- Может помочь увеличить на консьюмере `maxPollInterval`, например до 900 секунд (15 минут).

## На брокерах забита сеть / много партиций в состоянии under min.isr

Сопоставить с разборами:
- `[P2] I23174: На go сервисах перестали писаться логи`
- `INCALL-13067: На всех брокерах забита сеть, много партиций в min.isr или under replication`

## Застряло удаление брокера

1. Определяем id удаляемого брокера. Смотрим `kafka.layout`, id определяется по формуле:
   ```
   node.id = 2*10000 + dc_id*1000 + instance_id
   ```
   Например, если `kafka.layout=hc,kc,pc` и удаляемый брокер в `kc` под номером 5:
   `node.id = 20000 + 1*1000 + 5 = 21005`.

2. Выполняем:
   ```bash
   /opt/kafka/bin/kafka-topics.sh --bootstrap-server $cloud_hostname:9092 \
     --command-config /opt/kafka/config/client.properties --describe | grep <id>
   ```

   2.1. Если греп пустой — можно просто остановить таск downscale и поправить манифест
       сервиса и шарда, уменьшив количество реплик на 1. Затем выполнить unregister:
       ```bash
       /opt/kafka/bin/kafka-cluster.sh unregister --bootstrap-server $cloud_hostname:9092 \
         --config /opt/kafka/config/client.properties --id <id>
       ```

   2.2. Если не пустой — проверить, запущено ли перераспределение партиций с этого брокера.
        Если нет — запустить через круиз. Если запущено — дождаться завершения.

## Добавить нового listener (например, SASL_PLAINTEXT)

### 1. В pms `kafka.broker.properties`

В `listeners=INTERNAL://:9092` добавляем нужный (имя любое осмысленное) через запятую
(если кластер в infra — следующий за ним WAN из шаблона можно стереть). Порт должен быть
новый, неиспользуемый:
```
listeners = INTERNAL://:9092,PLAINTEXT://:9093
```

В `advertised.listeners=INTERNAL://{{ env('cloud_hostname') }}:9092` также через запятую
указываем нужный с тем же именем (wan аналогично можно стереть):
```
advertised.listeners = INTERNAL://{{ env('cloud_hostname') }}:9092,PLAINTEXT://{{ env('cloud_hostname') }}:9093
```

Обновить `listener.security.protocol.map` — добавить через запятую в формате
`<имя listener>:<security protocol>`:
```
listener.security.protocol.map=CONTROLLER:SASL_SSL,INTERNAL:SASL_SSL,WAN:SASL_SSL,PLAINTEXT:SASL_PLAINTEXT
```

### 2. В pms `kafka.controller.properties`

Повторить последний шаг (добавить в `listener.security.protocol.map`).

### 3. Рестарт

- Поочередный рестарт контроллеров.
- Поочередный рестарт брокеров, добавив в манифест порт для нового listener:
  ```
  '9093': lan,tcp
  ```

## Брокер лежит

Часто проблема в неправильной конфигурации.

## В кластере пытались создать Cruise Control

Проверить, что в конфиге брокера (`broker.properties`) закомментированы строки, относящиеся
к Cruise Control.

## Ошибки, способы проверки и решения

### `JoinGroup: INCONSISTENT_GROUP_PROTOCOL`

1. Просмотреть все группы.
2. Если есть зависшие в ребалансе — попросить остановить консумеров.
3. Если после этого остаются участники — перезагрузить брокер через
   `systemctl restart kafka-broker`.
