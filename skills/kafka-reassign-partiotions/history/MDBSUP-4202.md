# MDBSUP-4202: Вывод broker 24001 (dc) из кластера vkmyvkteamprod-cfs-kafka

**Дата**: 2026-07-27
**Кластер**: `vkmyvkteamprod-cfs-kafka`
**Брокеры**: 5 шт. — kc=21001, pc=22001, rc=23001, dc=24001, ec=25001
**Версия Kafka**: 3.8.0 (KRaft)
**RF**: 3
**Исполнитель**: дежурный + Claude

## Контекст

Хост `1.broker.vkmyvkteamprod-cfs-kafka.dc.one-infra.ru` (broker id 24001) нужно вывести
из кластера — перенести все его партиции на оставшиеся брокеры. На момент старта:

- 5 брокеров в кластере (21001, 22001, 23001, 24001, 25001).
- Данные только на 3: 21001 (kc), 23001 (rc), 24001 (dc) — по 60 реплик на каждом.
- 22001 (pc) и 25001 (ec) — пустые, под добавление для миграции.
- Controller quorum: 11001@kc, 15001@ec, 12001@pc (3 контроллера в разных ДЦ).

`1.broker...ec` (25001) в mcc instances показывал `availability: RESERVED`, но через
`kafka-broker-api-versions.sh` брокер зарегистрирован и отвечает.

## Стратегия

Пользователь выбрал round-robin 30/30 между 22001 (pc) и 25001 (ec), без throttle
(по аналогии с MDBSUP-4166, где `--execute --throttle` падал с `TimeoutException`
на `incrementalAlterConfigs`).

Каждая партиция после reassign:
- Было: `[21001, 23001, 24001]` (RF=3, 1 реплика на ДЦ: kc/rc/dc)
- Стало: `[21001, 23001, 22001]` или `[21001, 23001, 25001]` (RF=3, 1 реплика на ДЦ: kc/rc/pc или kc/rc/ec)

Cross-DC redundancy сохранена: каждая партиция имеет по 1 реплике в 3 разных ДЦ.

## Часть 1: Сбор информации

### Broker ID хоста

```bash
mcc --local sshexec -n infra 1.broker.vkmyvkteamprod-cfs-kafka.dc.one-infra.ru \
  "grep -E '^(node|broker)\.id|^process\.roles|^controller\.quorum\.voters' \
   /opt/kafka/config/broker.properties"
```

```
process.roles=broker
node.id=24001
controller.quorum.voters=11001@1.controller.vkmyvkteamprod-cfs-kafka.kc.one-infra.ru:9093,15001@1.controller.vkmyvkteamprod-cfs-kafka.ec.one-infra.ru:9093,12001@1.controller.vkmyvkteamprod-cfs-kafka.pc.one-infra.ru:9093
```

**Важно**: конфиг лежит в `/opt/kafka/config/broker.properties` (KRaft-режим), а не в
`server.properties` (где только `broker.id=0` — это ZooKeeper-mode legacy). Для KRaft
смотреть `broker.properties`.

### Список всех брокеров и их DC

`kafka-broker-api-versions.sh` дал 5 broker IDs: 21001, 22001, 23001, 24001, 25001.
DC каждого брокера нашли перебором через sshexec:

```bash
for dc in hc pc uc kc ec dc rc ic nc zc sc cc bc ac; do
  mcc --local sshexec -n infra 1.broker.vkmyvkteamprod-cfs-kafka.${dc}.one-infra.ru \
    "grep -E '^node\.id' /opt/kafka/config/broker.properties 2>/dev/null"
done
```

Результат:
| broker id | DC | FQDN |
|---|---|---|
| 21001 | kc | 1.broker.vkmyvkteamprod-cfs-kafka.kc.one-infra.ru |
| 22001 | pc | 1.broker.vkmyvkteamprod-cfs-kafka.pc.one-infra.ru |
| 23001 | rc | 1.broker.vkmyvkteamprod-cfs-kafka.rc.one-infra.ru |
| 24001 | dc | 1.broker.vkmyvkteamprod-cfs-kafka.dc.one-infra.ru |
| 25001 | ec | 1.broker.vkmyvkteamprod-cfs-kafka.ec.one-infra.ru |

### Партиции с 24001 в Replicas

```bash
sudo -u kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server 1.broker.vkmyvkteamprod-cfs-kafka.dc.one-infra.ru:9092 \
  --command-config /opt/kafka/config/client.properties \
  --describe 2>/dev/null | grep "Replicas:.*24001"
```

60 партиций: 50×`__consumer_offsets`, 3×`bell.request`, 3×`bell.upsert_agents`,
1×`notify.email`, 3×`services_yt_logs`.

В `bell.upsert_agents-2` брокер 24001 — preferred leader (`Replicas: 24001,21001,23001`).

## Часть 2: Генерация reassign.json

Python-скрипт `/tmp/gen_reassign.py` парсит `kafka-topics --describe`, сортирует партиции
по `(topic, partition)` и заменяет 24001 на 22001/25001 round-robin по индексу:

```python
CANDIDATES = [22001, 25001]  # pc, ec
new_broker = CANDIDATES[idx % len(CANDIDATES)]
new_replicas = [new_broker if r == OLD_BROKER else r for r in replicas]
```

Порядок реплик и preferred leader сохраняются (где 24001 был лидером — новый брокер
становится preferred leader, Kafka триггерит leader election).

Распределение: 30 партиций → 22001 (pc), 30 → 25001 (ec).

## Часть 3: Загрузка reassign.json на хост

`mcc scp /tmp/reassign.json <host>:/tmp/` промолчал и **не загрузил** файл. `mcc sshexec`
с полным base64 (17 КБ) упал с `414 URI Too Long`.

Рабочий вариант — `mcc ssh` + expect + base64 чанками по 4 КБ:

```python
# Локально: разбить base64 на чанки
base64 < /tmp/reassign.json | tr -d '\n' > /tmp/reassign.json.b64
split -b 4000 /tmp/reassign.json.b64 /tmp/reassign_chunk_
```

```tcl
# expect-скрипт, сгенерированный Python-ом со встроенными чанками
set timeout 90
spawn mcc --local ssh 1.broker.vkmyvkteamprod-cfs-kafka.dc.one-infra.ru
expect "/# "
send "rm -f /tmp/r.b64 /tmp/reassign.json\r"
expect "/# "
send "printf %s '<chunk_1>' >> /tmp/r.b64\r"
expect "/# "
# ...ещё 4 чанка...
send "base64 -d /tmp/r.b64 > /tmp/reassign.json && wc -c /tmp/reassign.json && echo ===DONE===\r"
expect "===DONE==="
```

Загружено 13071 байт, JSON валиден (`python3 -c 'import json; ...'` подтвердил 60 партиций).

## Часть 4: Выполнение и верификация

### --execute без throttle

```bash
sudo -u kafka /opt/kafka/bin/kafka-reassign-partitions.sh \
  --bootstrap-server 1.broker.vkmyvkteamprod-cfs-kafka.dc.one-infra.ru:9092 \
  --command-config /opt/kafka/config/client.properties \
  --reassignment-json-file /tmp/reassign.json \
  --execute
```

```
Successfully started partition reassignments for __consumer_offsets-0,...,services_yt_logs-2
```

Запущено 60 партиций. Без throttle (как в MDBSUP-4166).

### --verify

```bash
sudo -u kafka /opt/kafka/bin/kafka-reassign-partitions.sh \
  --bootstrap-server ... --command-config ... \
  --reassignment-json-file /tmp/reassign.json --verify
```

Все 60 партиций = `completed` через несколько секунд. Reassign прошёл быстро, т.к.:

1. Топики небольшие (внутренний `__consumer_offsets` + 4 пользовательских).
2. Новые реплики 22001/25001 сразу подключились и синхронизировались с 21001/23001.
3. Сеть между ДЦ не узкая (200 Mbit/sec на брокер).

### Финальная проверка

```bash
# 24001 в Replicas: должно быть 0
kafka-topics --describe | grep -c "Replicas:.*24001"
# → 0

# Гистограмма реплик по брокерам
kafka-topics --describe | grep -oE "Replicas: [0-9,]+" | grep -oE "[0-9]+" \
  | sort -n | uniq -c | sort -rn
# →   60 23001
#     60 21001
#     30 25001
#     30 22001

# Under-replicated partitions
kafka-topics --describe --under-replicated-partitions | grep -c "^Topic:"
# → 0
```

24001 полностью выведен из кластера. Throttle чистить не пришлось — `--execute` без
`--throttle` не ставит throttle-конфиги.

## Грабли и нюансы

### 1. `mcc sshexec` обрывает сессию при длинных INFO-логах

При запуске `kafka-configs.sh --describe` через `mcc sshexec` сессия падала с
`*** Connection closed by remote host ***` после вывода 30+ строк INFO-логов log4j.
**Решение**: фильтровать через `2>/dev/null` + `grep`, либо использовать `expect + mcc ssh`
с sentinel-маркером (`echo ===DONE===`).

### 2. `mcc sshexec` не принимает base64 > 4 КБ

`echo '<base64 17KB>' | base64 -d > /tmp/file` через `mcc sshexec` падает с
`414 URI Too Long` — sshexec кодирует команду в URL.

**Решение**: разбить base64 на чанки по 4 КБ и отправлять через `mcc ssh + expect + printf`.

### 3. `mcc scp` молча не копирует

`mcc scp /tmp/reassign.json <host>:/tmp/` вернулся без ошибок, но файл на хосте не
появился. Возможно, зависит от состояния minion. **Решение**: не полагаться на scp
для критичных загрузок, использовать expect + base64.

### 4. Tcl/expect интерполирует `[...]` и `$VAR`

Внутри `send "..."` в expect:
- `[0-9,]+` в grep-паттерне → `invalid command name "0-9,"`. Экранировать как `\[0-9,\]+`.
- `$VAR` → подставится как tcl-переменная. Экранировать `\$VAR`.

### 5. Конфиг KRaft-брокера — `broker.properties`, не `server.properties`

`/opt/kafka/config/server.properties` содержит `broker.id=0` (legacy ZK-mode).
Реальный конфиг KRaft-брокера — `/opt/kafka/config/broker.properties` с `node.id=24001`,
`process.roles=broker`, `controller.quorum.voters=...`.

### 6. `wc -l` в выводе `--under-replicated-partitions` считает INFO-логи

`kafka-topics --describe --under-replicated-partitions 2>/dev/null | wc -l` дал 83
(хотя партиций всего 60). Это потому что log4j INFO-логи пишутся в stdout при некоторых
флагах. **Решение**: считать только строки партиций через `grep -c "^Topic:"` (дал 0).

### 7. `availability: RESERVED` в mcc instances ≠ брокер мёртв

`1.broker...ec` (25001) в `mcc instances` показывал `availability: RESERVED` (known
issue `STARTING RESERVED` из скилла kafka-cluster-inspector). Но через
`kafka-broker-api-versions.sh` брокер зарегистрирован и работает, reassign на него
прошёл успешно. RESERVED — это mdb-data состояние, не Kafka.

## Что не сделано (на будущее)

1. **Хост 24001 не удалён из mdb-data**. Reassign убрал 24001 из Replicas, но хост всё
   ещё числится в кластере. Для полного вывода нужно удалить через mdb-data API/UI.
2. **Проверка leader balance**. После reassign leader'ы могли сместиться на 22001/25001
   (особенно `bell.upsert_agents-2`, где 24001 был preferred leader). Если есть
   дисбаланс — запустить `kafka-leader-election.sh --election-type preferred` для
   возврата к preferred leader'ам.
3. **Cruise Control** на кластере, видимо, отсутствует — он не убрал 24001 автоматически
   и не перебалансировал новые реплики. Возможно, CC не настроен.
