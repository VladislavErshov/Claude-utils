# Разбор проблем Kafka

**Канон — Confluence «Дежурство MDB: Kafka», секции «Проблемы» и «Ошибки, способы
проверки и решения»** (SSOT): https://confluence.vk.team/pages/viewpage.action?pageId=1348619075

Покрывает: создание топика (расчёт партиций), не могу подключиться/читать, сэмпл сообщений,
перевод на SASL_PLAINTEXT, PLAIN-пользователь, место на брокерах (включая полный скрипт
`clean_up.sh` для `-stray` партиций) и в логах, Connection timed out, зависшая таска
создания пользователя, обновление версии, перераспределение партиций, переезд rc→hc,
STARTING RESERVED, io/network треды, ребалансировка consumer group + JoinGroup, under
min.isr, застряло удаление брокера, новый listener, брокер лежит. Вики живая — править там.

Каталог известных технических багов (Broker is dead, InvalidReplicationFactor, FencedBroker,
CruiseControlMetricsReporter) — `known_issues.md`. Базовые паттерны доступа к хостам и
грабли Tcl/SSL — скилл [`mcc-host-worker`](../../mcc-host-worker/SKILL.md).

## Наши дополнения к вики

### Как правильно смотреть репликацию/реассигн (проверено на I53179, adtech-mdb8553-kafka)

**Кластерный статус репликации — `kafka-topics --describe`, НЕ Jolokia-метрики по одному брокеру:**

```bash
# с любого живого брокера, bootstrap — FQDN (localhost:9092 не работает — нет в SAN):
/opt/kafka/bin/kafka-topics.sh --bootstrap-server <broker-fqdn>:9092 \
  --command-config /opt/kafka/config/client.properties \
  --describe --under-replicated-partitions   # ISR < RF, включает under-min-isr
/opt/kafka/bin/kafka-topics.sh --bootstrap-server <broker-fqdn>:9092 \
  --command-config /opt/kafka/config/client.properties \
  --describe --at-min-isr-partitions         # ISR == min.insync.replicas (на грани)
```

Чтение результата:
- `Adding Replicas: ... Removing Replicas: ...` в строке партиции = **активный реассигн**;
- в URP-партиции сравнить `Replicas` vs `Isr` — выпавший broker id = отстающий фолловер;
  если один id выпадает из многих партиций — узкое место этот брокер, а не партиции;
- `kafka-configs --entity-type topics --entity-name <topic> --describe | grep throttle` —
  наличие `leader/follower.replication.throttled.replicas` = CC-ребаланс с троттлингом;
  сам троттл — `kafka-configs --entity-type brokers --entity-default --describe | grep throttled.rate`
  (байты/с; 104857600 = 100 МиБ/с);
- `Broker Max Lag` в Grafana = `kafka.server_replicafetchermanager_maxlag` =
  Jolokia `kafka.server:type=ReplicaFetcherManager,name=MaxLag,clientId=Replica` —
  отставание брокера КАК ФОЛЛОВЕРА. URP при этом может быть 0 на его лидерских партициях.
  Рост = фетчеры не успевают (диск/CPU/троттлинг ребаланса).

**Грабли:**
- `--bootstrap-server localhost:9092` → AdminClient виснет в `TimeoutException: fetchMetadata`
  (SSL SAN без localhost). Только FQDN.
- **Рабочий конфиг брокера — `/opt/kafka/config/broker.properties`** (process cmdline:
  `kafka.Kafka /opt/kafka/config/broker.properties`). НЕ путать с
  `/opt/kafka/config/server.properties` — это стоковый дефолт образа со вводящими в
  заблуждение значениями (там нет `num.replica.fetchers`, хотя в broker.properties их 8).
  Источник правды для тюнинга: PMS `kafka.broker.properties` → шаблон
  `broker.properties.j2` → рендер `broker.properties` (сверка — скилл `kafka-config-inspector`).
- Несколько `--topic A --topic B` в одной команде в Kafka 3.8 НЕ работает (возвращает пусто) —
  описывать кластер через `--under-replicated-partitions`/`--at-min-isr-partitions` или по одному топику.
- В stdout прилетают INFO-логи AdminClient — парсить `grep -a "Partition:" | grep -av INFO`.
- `grep "^Topic:"` не матчит (перед Topic табуляция) — `grep -E "^\s*Topic:"`.
- CC REST в MDB — порт **9000** (не 9090): `http://127.0.0.1:9000/kafkacruisecontrol/state?substates=executor`
  и `/user_tasks` прямо на cruise-хосте; может отвечать минуты — таймаут не меньше 90.
  Полезно в ExecutorState: `finishedDataMovement` (МБ), `completedPartitionMovement`,
  `pendingPartitionMovement`.
- broker.id ↔ хост: 200xx=1-й ДЦ, 210xx=2-й, 220xx=3-й (точную карту ДЦ давать через
  `kafka-broker-api-versions.sh ... | grep -oE "^[^(]+\(id: [0-9]+"`).
- Сложные скрипты на хост — только base64: локально `base64 < script.py`, на хосте
  `echo '<b64>' | base64 -d > /tmp/x.py && python3 /tmp/x.py` (Tcl/expect ломает кавычки и `[...]`).

**Диски при лаге репликации — проверять на лагающих брокерах (I53179, adtech-mdb8553):**

```bash
iostat -xz 1 2 | grep -E "Device|nvme|sd[a-z]"   # первый замер — с аптайма, смотреть ВТОРОЙ
uptime                                            # load при пустых дисках = CPU/соседи
KP=$(pgrep -f kafka.Kafka); pidstat -d -p $KP 1 2 # записи java (чтения через sendfile НЕ видны)
lsblk -o NAME,SIZE,TYPE,MOUNTPOINT               # JBOD-карта + поиск аномалий
```

Интерпретация:
- `%util ~100%` на data-диске приёмника бэкфилла — физический предел: nvme не может
  одновременно писать перелив и отдавать consumer-чтения. Параметры брокера не помогут —
  снижать точечный `follower.replication.throttled.rate` на этом брокере или ждать;
- read-storm (десятки тысяч r/s, rareq-sz ~3-4KB, r_await низкий, util 70-90%) — отдача
  фетчей мимо page cache (холодные сегменты для реассигна / лагающие консьюмеры);
  лечится завершением перелива, не конфигами;
- high load при пустых дисках — CPU-контеншн (соседние контейнеры/потоки), не I/O;
- проверить, что диск вообще тот: `lsblk` — данные Kafka на nvme JBOD; диск без маунтпоинта
  с ненулевым util — аномалия вне kafka-пути, разбирать отдельно;
- **noisy neighbor — главная скрытая причина «диск не справляется»** (кейс I53179,
  adtech-mdb8553): iostat показывает 99% util и ГБ/с записи, а продюсеры пишут единицы МБ/с.
  Доказательство — сравнить запись УСТРОЙСТВА (iostat, host-wide) с записью КОНТЕЙНЕРА
  (cgroup blkio, host-wide devices видны изнутри):
  ```bash
  iostat -x 1 2 | grep nvme                        # wkB/s устройства
  F=/sys/fs/cgroup/blkio/blkio.throttle.io_service_bytes
  A=$(grep Write $F | grep -vE "Sync|Total|Read|Discard|Async" | tr "\n" " "); sleep 3
  B=$(grep Write $F | grep -vE "Sync|Total|Read|Discard|Async" | tr "\n" " ")  # дельта/3 = B/s контейнера
  pidstat -d -p $(pgrep -f kafka.Kafka) 1 2        # пишет ли java
  ```
  Если устройство пишет ~1 ГБ/с, а контейнер ~КБ/с — сосед по миньону льёт в общий диск.
  Чтения соседа тоже давят (r_await растёт у всех). Решение — `mcc migrate --relocate`
  инстанса на другой миньон (канон и грабли: `mcc-host-worker/commands/migrate.md`;
  полный разбор: `history/I53179-2026-09-18-adtech-mdb8553-noisy-neighbor-disk.md`).
  Для миграции брокера: проверить партиции с ISR=2, где он в ISR (упадут в ISR=1 →
  с min.isr=2 продюсеры получат ошибки на время переезда); после — `hostname`,
  `df -h /mnt/data` (данные на месте), `Kafka Server started`, MaxLag начал падать,
  и на новом миньоне повторить blkio-дельту (соседа не должно быть);
- вариант того же корня: **load 100+ при пустых дисках** — CPU-сосед по миньону
  (наблюдалось на 12.pc/12.uc в I53179); лечится тем же переездом, диагностика —
  `top` внутри контейнера показывает только контейнерные процессы, а load/%Cpu — host-wide;
- RF=2 партиции при живом реассигне руками НЕ чинить: один reassign-таск на кластер,
  ручной reassign конфликтует с исполнением CC (сначала пауза/завершение CC, потом reassign).

### Очистка `-stray` партиций без clean_up.sh (mcc-перебор)

Перебрать хосты × ДЦ через [`mcc-host-worker`](../../mcc-host-worker/SKILL.md) (команда
`ssh`). Шаблон хоста: `$i.broker.<cluster>.<dc>.one-infra.ru` (`i=1..75`, `dc=hc,kc,pc`).
На каждом хосте:

```bash
cd /mnt/data/log
du -sk *-stray 2>/dev/null | awk '{s+=$1} END {print s+0}'
rm -rf *-stray
```

Сделать рестарт хостов, на которых была ошибка (иначе память может долго не обновляться).
Полный скрипт с expect-раннером и параллелизмом — в вики, секция «Кончилось место на
брокерах».

⚠️ **После `rm -rf *-stray` сверять `df -h`, а не `du`**: брокер может держать утёкшие
fd на stray-сегменты (deleted-but-open, сотни ГБ) — rm место не освобождает. Проверка
`lsof -nP +L1 | grep deleted`; освобождение — рестарт `kafka-broker` либо truncate
утёкших fd через `/proc/<pid>/fd/*` (приём I49678). Разбор —
`history/MDBSUP-5279-2026-09-10-extdbpu-stray-fd-leak.md`.

### Кончилось место в логах

Зайти на хост через [`mcc-host-worker`](../../mcc-host-worker/SKILL.md). Сначала чистим
логи, перезапускаем кафку — с забитым диском логов кафка может не подняться на новом образе.
