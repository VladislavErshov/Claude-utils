# MDBSUP-4166: Вывод удалённого broker 22026 из Replicas на spfrclustermdb-oneme-kafka

**Дата**: 2026-07-24
**Кластер**: `spfrclustermdb-oneme-kafka`
**Брокеры**: hc=21001-21025, kc=20001-20025, pc=22001-22025 (75 брокеров); 22026 (`26.broker...pc`) удалён из mdb-data, но оставался в metadata Kafka
**Версия Kafka**: 3.8.0 (KRaft)
**RF**: 3 (1 реплика на ДЦ), `min.insync.replicas=2`
**Исполнитель**: дежурный + Claude

## Контекст

В Grafana-дашборде для `9.broker.spfrclustermdb-oneme-kafka.pc.one-infra.ru` (broker id 22009) пользователь увидел:
- offline partitions = 27 (за range 1h)
- under replicated = 14
- at min ISR = 14

Прямая проверка через `kafka-topics --describe --unavailable-partitions` через FQDN `9.broker...pc:9092` показала **9 партиций** с `Leader: none` (актуально на момент разбора; 27 — max за час). Все 9 имели одинаковый паттерн:

```
Topic: nnPlatformResultsLog  Partition: 3  Leader: none  Replicas: 22026,20019,21023  Isr: 22026
Topic: hashCalculationResult Partition: 6  Leader: none  Replicas: 22026,20012,21016  Isr: 22026
... (ещё 7 партиций)
```

**Диагноз**: broker 22026 (`26.broker...pc`) был preferred leader и **единственной ISR-репликой** для этих партиций, но стал недоступен (хост удалён из mdb-data, mcc не подключался — `NamespaceMissingException`). Поскольку `unclean.leader.election.enable=false` (default в Kafka 3.x) и `min.insync.replicas=2`, controller не мог выбрать лидера из не-ISR реплик → партиции повисли offline.

## Часть 1: Unclean leader election (восстановление доступности)

Перед reassign потребовалось вернуть партициям лидера — иначе reassign бесполезен.

### Команда

```bash
sudo -u kafka /opt/kafka/bin/kafka-leader-election.sh \
  --bootstrap-server 9.broker.spfrclustermdb-oneme-kafka.pc.one-infra.ru:9092 \
  --admin.config /opt/kafka/config/client.properties \
  --election-type unclean \
  --all-topic-partitions
```

`--all-topic-partitions` для `UNCLEAN` безопасно для здоровых партиций — election запускается только там, где лидера нет.

### Нюанс синтаксиса Kafka 3.8

`kafka-leader-election.sh` принимает **`--admin.config`**, а не `--command-config` (как `kafka-topics.sh`). С `--command-config` падает с `UnrecognizedOptionException`.

### Результат

```
Successfully completed leader election (UNCLEAN) for partitions
spamAnalyzerReplication-nausermessages-2, hashCalculationResult-6,
idsNNPlatformTasks-9, oneme_antispam_pr_idsPhones-41, nnPlatformResultsLog-3,
oneme_antispam_nnBotTextClassificationResponse-10, IdsSearchSpam2-16,
oneme_antispam_nnAntiphishingClassificationRequest-13,
spamAnalyzerReplication-nachatmessages-4
```

Все 9 партиций получили лидера. После: `--unavailable-partitions` = 0, `--under-replicated-partitions` = 0, `--at-min-isr-partitions` = 0.

**Важно**: unclean election может терять данные — сообщения, записанные на 22026 и не успевшие реплицироваться, утеряны. Но партиции уже были offline, данные и так были недоступны.

## Часть 2: Вывод 22026 из Replicas через reassign

После unclean election 22026 остался в `Replicas` 23 партиций как мёртвая реплика (в Isr его уже не было). Надо убрать его из Replicas, заменив на живого pc-брокера.

### Сбор партиций

```bash
sudo -u kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server $BS --command-config $CFG --describe 2>/dev/null \
  | grep "Replicas:.*22026" > /tmp/with_22026.txt
```

**Грабли**: `grep "^Topic:"` не матчит строки партиций — перед `Topic:` есть табуляция. Надо `grep "Replicas:.*22026"` или `grep -E "^\s*Topic:"`.

Найдено **23 партиции** с 22026 в Replicas.

### Генерация reassign.json

Python-скрипт `/tmp/gen_reassign.py` (загружали на хост через base64, т.к. `expect` + tcl ломается на `[...]` в python-коде):

```python
pc_brokers = [22001 + i for i in range(25)]  # 22001-22025, без 22026
for line in lines:
    m = re.search(r"Topic:\s(\S+)\s+Partition:\s(\d+)\s+Leader:\s(\S+)\s+Replicas:\s([\d,]+)\s+Isr:\s([\d,]+)", line)
    ...
    candidates = [b for b in pc_brokers if b not in replicas and b != 22026]
    new_broker = candidates[partition % len(candidates)]  # round-robin по partition
    new_replicas = [new_broker if r == 22026 else r for r in replicas]
```

Стратегия:
- Заменяем только 22026, остальные реплики не трогаем.
- Новый брокер — из того же ДЦ (pc=22xxx), чтобы сохранить cross-DC схему «1 реплика на ДЦ».
- Round-robin по partition number для балансировки нагрузки между pc-брокерами.
- Исключаем брокеров, уже занятых в текущих Replicas этой партиции.

### Список замен (23 партиции)

| Partition | Было | Стало |
|---|---|---|
| nnPlatformResultsLog-3 | 22026,20019,21023 | 22004,20019,21023 |
| idsNNPlatformScores-5 | 20005,21025,22026 | 20005,21025,22006 |
| nnPhotosPornClassificationResult-15 | 20023,21008,22026 | 20023,21008,22016 |
| hashCalculationInput-13 | 20003,21002,22026 | 20003,21002,22014 |
| oneme_antispam_pr_idsPhones-15 | 20007,21016,22026 | 20007,21016,22016 |
| oneme_antispam_pr_idsPhones-41 | 22026,20008,21017 | 22017,20008,21017 |
| oneme_antispam_pr_idsPhones-67 | 21018,22026,20009 | 21018,22018,20009 |
| hashCalculationResult-6 | 22026,20012,21016 | 22007,20012,21016 |
| oneme_antispam_nnAntiphishingClassificationResponse-25 | 21003,22026,20015 | 21003,22001,20015 |
| oneme_antispam_nnAntiphishingClassificationResponse-51 | 20016,21004,22026 | 20016,21004,22002 |
| oneme_antispam_nnAntiphishingClassificationRequest-13 | 22026,20008,21021 | 22014,20008,21021 |
| oneme_antispam_nnAntiphishingClassificationRequest-39 | 21022,22026,20009 | 21022,22015,20009 |
| oneme_antispam_nnAntiphishingClassificationRequest-65 | 20010,21023,22026 | 20010,21023,22016 |
| spamAnalyzerReplication-nausermessages-2 | 22026,20014,21009 | 22003,20014,21009 |
| onemeBotUpdates-2 | 21005,22026,20025 | 21005,22003,20025 |
| spamAnalyzerReplication-nachatmessages-4 | 22026,20001,21009 | 22005,20001,21009 |
| NotificationActionStatisticsWrapper-7 | 20011,21011,22026 | 20011,21011,22008 |
| oneme_antispam_nnBotTextClassificationResponse-10 | 22026,20023,21014 | 22011,20023,21014 |
| IdsSearchSpam2-16 | 22026,20003,21013 | 22017,20003,21013 |
| IdsSearchSpam_test2-4 | 21025,22026,20004 | 21025,22005,20004 |
| nnPhotosPornClassificationInput-14 | 20011,21011,22026 | 20011,21011,22015 |
| idsNNPlatformTasks-9 | 22026,20020,21017 | 22010,20020,21017 |
| oneme_antispam_nnBotTextClassificationRequest-3 | 20001,21009,22026 | 20001,21009,22004 |

### Выполнение

**Попытка 1** — `--execute --throttle 104857600` упала с timeout:

```
Error: org.apache.kafka.common.errors.TimeoutException:
  Timed out waiting for a node assignment. Call: incrementalAlterConfigs
at ...ReassignPartitionsCommand.modifyInterBrokerThrottle(...)
```

`incrementalAlterConfigs` (установка throttle через controller) не завершилась за 60 сек (default api timeout). Reassign не запустился. При последующем `--verify` Kafka автоматически очистила throttle.

**Попытка 2** — `--execute` **без `--throttle`**:

```bash
sudo -u kafka /opt/kafka/bin/kafka-reassign-partitions.sh \
  --bootstrap-server 9.broker.spfrclustermdb-oneme-kafka.pc.one-infra.ru:9092 \
  --command-config /opt/kafka/config/client.properties \
  --reassignment-json-file /tmp/reassign.json \
  --execute
```

```
Successfully started partition reassignments for IdsSearchSpam2-16,...
```

`--verify` сразу (через секунду) показал `completed` для всех 23 партиций. Reassign прошёл мгновенно, т.к.:
1. 22026 был недоступен — его реплика не содержала уникальных данных.
2. Новые реплики (22xxx) уже были в ISR после unclean election — синхронизация не требовалась.
3. Kafka просто обновила metadata в KRaft log.

Throttle чистить не пришлось — `--execute` без `--throttle` не ставит throttle-конфиги.

### Финальная проверка

```
22026 в Replicas: 0   (было 23)
Unavailable: 0
Under-replicated: 0
At-min-isr: 0
```

22026 полностью выведен из кластера. Осталось одно упоминание 22026 в устаревшем throttle-config топика `oneme_antispam_pr_idsEntityFacts` (`64:22026`) — артефакт от предыдущего reassign, не критичен.

## Грабли и нюансы

### 1. SSL/localhost в bootstrap-server

`kafka-topics.sh --bootstrap-server localhost:9092` падает с `SslAuthenticationException: No subject alternative DNS name matching localhost found`. Сертификат брокера не содержит `localhost` в SAN. **Решение**: использовать FQDN брокера (`9.broker.spfrclustermdb-oneme-kafka.pc.one-infra.ru:9092`).

### 2. `--under-min-isr` не поддерживается в Kafka 3.8

У `kafka-topics.sh` во FreeBSD нет флага `--under-min-isr` (был в старых версиях). Вместо него:
- `--at-min-isr-partitions` — партиции, где ISR size == min.insync.replicas (на грани).
- `--under-replicated-partitions` — партиции, где ISR < RF (включая under-min-isr как подмножество).

### 3. Tcl/expect + Python с `[...]`

`expect -c '...'` интерполирует `[...]` как tcl command substitution. Python-код с list comprehensions `[x for x in ...]` ломается. **Решение**: закодировать python-скрипт в base64 локально, отправить через `echo '<b64>' | base64 -d > /tmp/script.py`.

### 4. Tcl/expect + `$VAR` в shell-скриптах

`send "cat > /tmp/x.sh << EOF\nBS=...\n... \$BS ...\nEOF"` — tcl интерполирует `$BS` как tcl-переменную. **Решение**: экранировать как `\$BS` внутри send-строки.

### 5. `grep "^Topic:"` не матчит строки партиций

В выводе `kafka-topics --describe` перед `Topic:` в строках партиций есть табуляция. `grep "^Topic:"` матчит только заголовки топиков. **Решение**: `grep -E "^\s*Topic:"` или `grep "Replicas:.*<broker_id>"`.

### 6. `--command-config` vs `--admin.config`

В Kafka 3.8:
- `kafka-topics.sh` — `--command-config`
- `kafka-leader-election.sh` — `--admin.config`
- `kafka-reassign-partitions.sh` — `--command-config`

У разных тулзов разные имена флага для одного и того же. Проверять через `--help`.

### 7. Timeout на `incrementalAlterConfigs` при `--throttle`

`--execute --throttle N` вызывает `incrementalAlterConfigs` для установки throttle на топики/брокеры. Если controller перегружен или metadata quorum медленный, операция падает с `TimeoutException` за 60 сек. **Решение**: либо повторить позже, либо запустить `--execute` без `--throttle` (если сеть/диски не под угрозой перегрузки).

## Что не сделано (на будущее)

1. **Устаревший throttle-config** в `oneme_antispam_pr_idsEntityFacts` содержит `64:22026`. Можно почистить через `kafka-configs.sh --alter --entity-type topics --entity-name oneme_antispam_pr_idsEntityFacts --delete-config leader.replication.throttled.replicas,follower.replication.throttled.replicas`, но это затронет весь throttle (не только 22026). Безопасно, если других активных reassign на этом топике нет.
2. **Причина удаления 22026** из mdb-data не выяснялась — это к администраторам mdb-data / OneCloud.
3. **Cruise Control** на кластере, видимо, есть (на это указывал throttle-config в `oneme_antispam_pr_idsEntityFacts`), но он не убрал 22026 автоматически. Возможно, CC не настроен на auto-removal dead brokers, или был отключён.
