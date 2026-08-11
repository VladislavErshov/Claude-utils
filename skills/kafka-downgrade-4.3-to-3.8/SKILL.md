---
name: kafka-downgrade-4.3-to-3.8
description: Даунгрейд MDB Kafka кластера с 4.3 до 3.8 (KRaft) с сохранением бизнес-данных и офсетов consumer-групп. Прямой даунгрейд metadata.version невозможен — метод состоит в сбросе KRaft-метаданных, ре-формате кластера и переименовании 4.3-папок из stray обратно в canonical с переписыванием partition.metadata новым topic_id. .log файлы остаются на диске, не копируются через mcc scp. Бэкап скачивается только KRaft metadata с контроллеров (для отката). Процедура без остановки кластера перед бэкапом — бэкап KRaft meta-log делается на живом кластере. Используй когда нужно откатить кластер с 4.3 (включая share groups KIP-932) до 3.8. Docker-образ переключает пользователь через изменение манифеста хоста в админке облака (НЕ mdb-data, НЕ PMS напрямую). Work с хостами — `kafka-host-inspector` + `mcc-host-access`, анализ логов — `kafka-log-investigator`.
allowed-tools: [Bash, Read, Write, Edit, Grep, Glob]
---

# Даунгрейд MDB Kafka 4.3 → 3.8 (KRaft, in-place, без остановки перед бэкапом)

Скилл описывает процедуру отката MDB Kafka кластера с 4.3 до 3.8 с сохранением
бизнес-данных и офсетов consumer-групп. Прямой даунгрейд `metadata.version`
невозможен — метод состоит в сбросе KRaft-метаданных, ре-формате кластера и
переименовании 4.3-папок из `<name>.<uuid>-stray` обратно в каноничные с
переписыванием `partition.metadata` новым topic_id.

⚠️ **Бизнес-данные сохраняются НЕ ВСЕГДА — есть проблема с разметкой timestamps.** После rename stray → canonical и старта брокеров Kafka 3.8 запускает log recovery. Без `.snapshot` (мы его удалили на Этапе 6) Kafka НЕ восстанавливает `largestRecordTimestamp` в `LogSegment` → считает = 0 (т.е. `1970-01-01 00:00:00 UTC`). При `retention.ms=604800000` (default 7 дней) retention видит сегменты с timestamp=0 как устаревшие → удаляет через ~30 сек после старта (грабля #18).

**Два метода работы с этой проблемой:**

- **Метод A (рекомендуется, основной) — удалить `.index`/`.timeindex`/`.snapshot`/`leader-epoch-checkpoint` на Этапе 6.** Тогда Kafka не может загрузить segment metadata из индексов и **вынуждена сделать полный log recovery**: построчно сканирует `.log` и восстанавливает `maxTimestampSoFar` из реальных records. После этого `largestRecordTimestamp` = реальный timestamp последней записи, retention не срабатывает, `retention.ms=-1` НЕ нужен. Минус — полный log recovery при старте (секунды/минуты для больших segment-ов). Подтверждено на `test-downgrade5` 2026-08-11.

- **Метод B (ускоренный) — оставить `.index`/`.timeindex`, удалить только `.snapshot`, поставить `retention.ms=-1`.** Брокер стартует быстрее (без полного сканирования `.log`), но `largestRecordTimestamp` остаётся = 0 и retention отключается навсегда. Возвращать `retention.ms` обратно нельзя — retention снова удалит segments с timestamp=0. Применять только если Method A слишком медленный (прод-кластер с большими segment-ами). Если Method B применён реактивно (после срабатывания грабли #18) — восстановить `.log` с непострадавшего брокера через `recover_log.sh` (Этап 8).

## Когда применять

- Кластер обновлён до 4.3 (включая share groups KIP-932), нужно откатиться до 3.8.
- Broker/cluster IDs можно не сохранять (тестовый кластер).
- Docker-образ Kafka переключает пользователь через изменение манифеста хоста в админке облака (НЕ mdb-data, НЕ PMS напрямую).
- ⚠️ Откат ограничен: KRaft восстанавливается из бэкапа, но `.log` на брокерах остаются в перестроенном состоянии. Если нужен гарантированный откат на 4.3 с данными — делай полный бэкап `log.dirs` до начала процедуры.

## Архитектура решения

KRaft-only, broker и controller на разных хостах. Сброс metadata → ре-формат →
`--create` топиков → Kafka сама переименовывает 4.3-папки в `<name>.<uuid>-stray`
→ мы переименовываем stray → canonical + переписываем `partition.metadata`.

**Бэкап KRaft meta-log делается на живом кластере** (без остановки) с controller-follower. KRaft meta-log append-only — truncated segment при восстановлении игнорируется Kafka (безопасно), snapshot'ы создаются atomically через rename. Контейнер пересоздаётся при переключении docker-образа (Этап 2) — отдельная остановка не нужна.

**Топик `__share_group_*` не переносится** (3.8 не знает share-протокол KIP-932).
**CC-топики (`__CruiseControlMetrics*`, `__KafkaCruiseControl*`) удаляем перед
стартом** — грабля #17: `CorruptRecordException` при старте 3.8.

## Хосты

```
{1,2,3,...}.broker.<cluster>.<dc>.one-infra.ru     — Kafka broker
{1,2,3,...}.controller.<cluster>.<dc>.one-infra.ru — KRaft controller
```

⚠️ **Cruise-хост (`1.cruise.<cluster>.<dc>.one-infra.ru`) НЕ ТРОГАТЬ.** Не запускать/не проверять cruise-control.service, не включать cruise-хост в host-check. Если пользователь упоминает cruise-хост в списке — игнорировать его на протяжении всей процедуры.

Список хостов даёт пользователь. Доступ через `mcc --local -n infra sshexec -n infra <fqdn> "<cmd>"` и `mcc scp` (подробнее — `mcc-host-access`).

### Хелпер `mcc_retry` — auto-retry для SSL "Too early" (грабля #6)

`mcc sshexec`/`mcc scp` периодически падают с `Failed to setup tunnel: Too early. SSL Handshake is not finished` —особенно при частых вызовах на 6 хостах. На каждом даунгрейде это 5-10 retry с ручным `sleep 10-15`. Хелпер автоматизирует retry (3 попытки, backoff 8 сек) и фильтрует `Connection closed by remote host`:

```bash
# Залить на машину оператора или выполнять в текущей сессии
mcc_retry() {
    # mcc_retry <host> <cmd...>  — для sshexec
    # mcc_retry scp <src> <host:dst>  — для scp (первый аргумент = "scp")
    local mode="$1"
    if [ "$mode" = "scp" ]; then
        shift
        local src="$1" dst="$2"
        for i in 1 2 3; do
            local out
            out=$(mcc scp "$src" "$dst" 2>&1)
            if ! echo "$out" | grep -q 'Too early. SSL Handshake is not finished'; then
                echo "$out" | grep -v 'Connection closed'
                return 0
            fi
            echo "RETRY $i/3 (SSL Too early), sleeping 8s..." >&2
            sleep 8
        done
        return 1
    fi
    # sshexec mode: mcc_retry <host> <cmd...>
    local host="$1"; shift
    local cmd="$*"
    for i in 1 2 3; do
        local out
        out=$(mcc --local -n infra sshexec -n infra "$host" "$cmd" 2>&1)
        if ! echo "$out" | grep -q 'Too early. SSL Handshake is not finished'; then
            echo "$out" | grep -v 'Connection closed'
            return 0
        fi
        echo "RETRY $i/3 (SSL Too early), sleeping 8s..." >&2
        sleep 8
    done
    return 1
}
```

Использование:
```bash
mcc_retry 1.broker.<cluster>.<dc>.one-infra.ru "systemctl start kafka-broker.service; systemctl is-active kafka-broker.service"
mcc_retry scp ~/local/file.txt 1.broker.<cluster>.<dc>.one-infra.ru:/mnt/data/
```

⚠️ Хелпер проверяет именно строку `Too early. SSL Handshake is not finished` — другие ошибки (host not found, command failed) не триггерят retry. `grep -v 'Connection closed'` подавляет шум от mcc после успешного выполнения.

## Пути на хосте (MDB-специфика)

- `log.dirs=/mnt/data/log` — данные топиков (брокеры)
- `/mnt/data/metadata/__cluster_metadata-0/` — KRaft meta-log (контроллеры, отдельно от `log.dirs`)
- `/opt/kafka/config/{broker,controller,client}.properties` — отрендерено confp
- `/opt/kafka/bin/` — `kafka-storage.sh`, `kafka-topics.sh`, `kafka-features.sh`, `kafka-metadata-quorum.sh`, `kafka-consumer-groups.sh`, `kafka-dump-log.sh`, `kafka-get-offsets.sh`
- `/mnt/logs/dbms/kafka-{broker,controller}.out.log` — логи
- Systemd: `kafka-broker.service`, `kafka-controller.service`, `cruise-control.service`

⚠️ **Не путать с vanilla Kafka**: пути `/var/lib/kafka/data` и `/etc/kafka/server.properties` НЕ MDB. Ручная подмена бинарников бессмысленна — `confp --oneshot` вернёт всё обратно.

## Этапы процедуры

### 0. Сбор состояния 4.3 (только чтение)

На любом broker-хосте:

```bash
export KAFKA_HEAP_OPTS='-Xmx512m'  # иначе OOM в kafka CLI
B=1.broker.<cluster>.<dc>.one-infra.ru:9092
# ВАЖНО: --command-config передавать как отдельные аргументы, НЕ через $CF=... (грабля #16)

/opt/kafka/bin/kafka-topics.sh --bootstrap-server $B --command-config /opt/kafka/config/client.properties --describe > /tmp/topics_structure.txt
/opt/kafka/bin/kafka-configs.sh --bootstrap-server $B --command-config /opt/kafka/config/client.properties --entity-type topics --describe > /tmp/topics_configs.txt
/opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server $B --command-config /opt/kafka/config/client.properties --all-groups --describe > /tmp/consumer_groups.txt
/opt/kafka/bin/kafka-features.sh --bootstrap-server $B --command-config /opt/kafka/config/client.properties describe > /tmp/features.txt
/opt/kafka/bin/kafka-topics.sh --bootstrap-server $B --command-config /opt/kafka/config/client.properties --list | grep '^__share_group' > /tmp/share_group_topics.txt
cat /mnt/data/log/meta.properties > /tmp/meta_properties.txt

# SCRAM users и ACLs — теряются при format KRaft, нужно воссоздать после даунгрейда
/opt/kafka/bin/kafka-configs.sh --bootstrap-server $B --command-config /opt/kafka/config/client.properties --entity-type users --describe > /tmp/users_scram.txt 2>&1 || true
/opt/kafka/bin/kafka-acls.sh --bootstrap-server $B --command-config /opt/kafka/config/client.properties --list > /tmp/acls.txt 2>&1 || true

# Упаковать все 8 дампов в один tar (скачивать одной командой, меньше SSL retry)
cd /tmp && tar -czf /mnt/data/state_4_3.tar.gz topics_structure.txt topics_configs.txt consumer_groups.txt features.txt share_group_topics.txt meta_properties.txt users_scram.txt acls.txt
```

Скачать `state_4_3.tar.gz` на машину оператора одной командой `mcc scp` (mcc автоматически распакует — в каталоге назначения появятся 8 .txt файлов). Не качать файлы по одному — это 8 попыток SSL handshake, каждая может поймать "Too early" (грабля #6).

⚠️ `mcc scp` не работает с `/tmp` (грабля #7) — поэтому tar собирается в `/mnt/data/`.

**Verify**: список share-group топиков может быть пустым (если share groups не использовались) — это норма. Записать cluster.id из `meta_properties.txt` — понадобится на Этапе 3.

⚠️ **SCRAM users и ACLs хранятся в KRaft metadata и теряются при format (Этап 3).** Если `users_scram.txt` пустой или `kafka-configs` упал (кластер уже down после Этапа 2) — вытащить из бэкапа KRaft meta-log (Этап 1) через `kafka-dump-log.sh --files <meta-log>.log --cluster-metadata-decoder | grep -E 'USER_SCRAM_CREDENTIAL_RECORD|ACCESS_CONTROL_ENTRY_RECORD'`. Для каждого user взять ПОСЛЕДНЮЮ версию record (по offset) — более старые содержат устаревшие salt/key. Поля SCRAM: `mechanism` (1=SCRAM-SHA-256, 2=SCRAM-SHA-512), `salt`, `storedKey`→`stored_key`, `serverKey`→`server_key`, `iterations`. Поля ACL: `resourceType` (2=TOPIC, 3=GROUP, 4=CLUSTER, 5=TRANSACTIONAL_ID), `patternType` (3=LITERAL), `operation` (3=READ, 4=WRITE, 8=DESCRIBE, 10=DESCRIBE_CONFIGS), `permissionType` (3=ALLOW). Супер-пользователь `super` прописан в broker.properties через mdb-data секреты — его воссоздавать не нужно. Восстановление — на Этапе 7.

### 1. Бэкап KRaft metadata с контроллеров на живом кластере (для отката)

Бэкап KRaft meta-log с контроллеров без остановки кластера. `.log` файлы брокеров НЕ бэкапим — они остаются на диске.

⚠️ Бэкапить с controller-follower (не leader), чтобы снизить активность записи во время tar. KRaft meta-log append-only — truncated segment при восстановлении игнорируется Kafka (безопасно), snapshot'ы создаются atomically через rename.

```bash
mkdir -p ~/kafka_4.3_backup/controller-<dc>/

# На каждом controller-хосте (3 шт): tar KRaft meta-log + meta-файлы
mcc --local -n infra sshexec -n infra <controller-fqdn> \
  "cd /mnt/data && tar -czf /mnt/data/kraft_meta.tar.gz \
     metadata/__cluster_metadata-0 \
     metadata/meta.properties metadata/bootstrap.checkpoint \
     log/meta.properties log/bootstrap.checkpoint \
     2>/dev/null || true"

mcc scp <controller-fqdn>:/mnt/data/kraft_meta.tar.gz ~/kafka_4.3_backup/controller-<dc>/
```

⚠️ `mcc scp` при скачивании tar.gz распаковывает его в каталог назначения — в `controller-<dc>/` появятся `metadata/` и `log/` с содержимым архива. Это нормально.

**Verify**: 3 архива скачаны, в каждом есть `metadata/__cluster_metadata-0/` + `meta.properties` + `bootstrap.checkpoint`.

### 2. Переключение docker-образа на 3.8 (пользователь)

**Пауза в процедуре.** Пользователь переключает docker-образ через изменение манифеста хоста в админке облака (НЕ mdb-data, НЕ PMS напрямую) на всех 6 хостах. Актуальный тег смотреть в https://mdb.kaizen.idzn.ru/dockerTags (например `ubuntu20-kafka-3.8.0`).

После переключения конфиги перерендерятся через `confp --oneshot` — из `broker.properties` исчезнут 4.3-специфичные настройки (`group.coordinator.rebalance.protocols=...,share`, `unstable.api.versions.enable=true`).

⚠️ **Откат docker-образа ломает рендеринг конфигов.** После переключения образа на 3.8 `confp-init.service` НЕ запускается автоматически — конфиги `/opt/kafka/config/{broker,controller}.properties` и `/etc/sysconfig/kafka` НЕ отрендерятся, `systemctl start kafka-*.service` падает с `Job for kafka-*.service failed because of unavailable resources or another system error` (systemd не находит EnvironmentFile/ExecStartPre). Симптом: `/opt/kafka/config/broker.properties: No such file or directory`. **Фикс: `systemctl start confp-init.service` на всех 6 хостах вручную.** После этого конфиги появляются.

⚠️ Контейнер может пересоздаться после неудачного старта — `confp --oneshot` нужно повторять перед каждым стартом.

⚠️ После переключения хосты могут быть недоступны 30-60 секунд (container is not found / minion not scheduling). Подождать и повторить.

⚠️ Отдельная остановка кластера перед переключением НЕ нужна — контейнер пересоздаётся при переключении образа, старые kafka-broker/controller процессы убиваются.

**Verify** после переключения — на любом broker-хосте проверить версию:
```bash
/opt/kafka/bin/kafka-topics.sh --version  # должно показать 3.8.0
```

⚠️ Конфиги `/opt/kafka/config/{broker,controller}.properties` могут отсутствовать до Этапа 4 — `confp-init.service` не запускается автоматически после переключения образа. Это НЕ мешает: на Этапе 4 перед format делается `systemctl restart confp-init.service` и конфиги рендерятся. Проверять confp на Этапе 2 не нужно.

### 3. Очистка и format KRaft под 3.8

Объединённый этап: остановка сервисов → удаление KRaft meta-файлов → format. Выполняется одним проходом на каждом хосте, без отдельного verify между clean и format.

⚠️ **НЕ генерируй новый cluster ID.** Используй **старый cluster ID** из 4.3 `meta.properties` (сохранён в `/tmp/meta_properties.txt` на Этапе 0). В env хоста прописан `KAFKA_CLUSTER_ID=<старый UUID>` (см. граблю #14), и этот UUID надо использовать при format.

⚠️ **Пути meta-файлов на broker и controller хостах РАЗНЫЕ.** Проверьте перед удалением, где именно лежат `__cluster_metadata-0`, `meta.properties`, `bootstrap.checkpoint`:
- **Broker-хосты**: KRaft meta-log `__cluster_metadata-0/` живёт **внутри `log.dirs`** (`/mnt/data/log/__cluster_metadata-0`). Папки `/mnt/data/metadata/` на брокерах НЕТ. Файлы `meta.properties`, `bootstrap.checkpoint` — в `/mnt/data/log/`.
- **Controller-хосты**: KRaft meta-log `__cluster_metadata-0/` живёт в `/mnt/data/metadata/__cluster_metadata-0`. НО `meta.properties` и `bootstrap.checkpoint` могут лежать **и в `/mnt/data/metadata/`, и в `/mnt/data/log/`** (у контроллера `log.dirs=/mnt/data/log` и Kafka пишет туда meta-файлы при формате). **Удалять из обоих мест**.

⚠️ **На broker-хостах topic-папки НЕ ТРОГАЕМ.** `test-*`, `__consumer_offsets-*` с 4.3 `.log`/`.index`/`.timeindex`/`.snapshot`/`partition.metadata` остаются на диске — Kafka при `--create` (Этап 6) переименует их в stray, а мы на Этапе 7 переименуем обратно.

**На broker-хостах** (3 шт) — одним скриптом:
```bash
CLUSTER_ID=$KAFKA_CLUSTER_ID  # из env хоста, совпадает с 4.3 meta.properties

# 1. Остановить сервисы (иначе работающий процесс пересоздаёт meta-файлы после rm)
systemctl stop kafka-broker.service kafka-controller.service 2>/dev/null

# 2. Рендер конфигов (после переключения образа confp-init не запускается сам)
systemctl restart confp-init.service; sleep 3

# 3. Удалить KRaft meta-файлы (topic-папки НЕ ТРОГАЕМ)
rm -rf /mnt/data/log/__cluster_metadata-0 /mnt/data/log/__cluster_metadata-0.snapshot*
rm -f /mnt/data/log/meta.properties /mnt/data/log/bootstrap.checkpoint
rm -f /mnt/data/log/.kafka_cleanshutdown /mnt/data/log/*-checkpoint
# 4.3-специфичные топики (грабля #17 + 3.8 не знает KIP-932)
rm -rf /mnt/data/log/__share_group_* /mnt/data/log/__CruiseControlMetrics* /mnt/data/log/__KafkaCruiseControl*

# 4. Format под 3.8 со старым cluster ID
export KAFKA_HEAP_OPTS='-Xmx512m'
/opt/kafka/bin/kafka-storage.sh format --ignore-formatted -t "$CLUSTER_ID" -c /opt/kafka/config/broker.properties

# 5. Verify
grep -E 'cluster.id|node.id' /mnt/data/log/meta.properties
```

**На controller-хостах** (3 шт) — аналогично, но пути другие:
```bash
CLUSTER_ID=$KAFKA_CLUSTER_ID

systemctl stop kafka-broker.service kafka-controller.service 2>/dev/null
systemctl restart confp-init.service; sleep 3

# Удалить meta-файлы из ОБОИХ мест (metadata/ и log/)
rm -rf /mnt/data/metadata/__cluster_metadata-0 /mnt/data/metadata/__cluster_metadata-0.snapshot*
rm -f /mnt/data/metadata/meta.properties /mnt/data/metadata/bootstrap.checkpoint
rm -f /mnt/data/log/meta.properties /mnt/data/log/bootstrap.checkpoint

/opt/kafka/bin/kafka-storage.sh format --ignore-formatted -t "$CLUSTER_ID" -c /opt/kafka/config/controller.properties

# Verify — Kafka форматирует обе директории
grep -E 'cluster.id|node.id' /mnt/data/metadata/meta.properties /mnt/data/log/meta.properties
```

⚠️ Если `kafka-storage.sh format` падает с `NoSuchFileException: /opt/kafka/config/broker.properties` — `confp-init.service` не отрендерил конфиги. Перезапусти `systemctl restart confp-init.service` (не `start`, а именно `restart`) и повтори format.

⚠️ Флаг `--ignore-formatted` важен — без него pre-start скрипт падает при повторном формате. Если pre-start падает с `KAFKA_ROLE is not set` — переменная уже правильно прописана в env хоста, **не трогать `/etc/sysconfig/kafka`** (по пользовательской инструкции).

### 4. Запуск пустого кластера 3.8

```bash
# Контроллеры (3 шт)
systemctl start kafka-controller.service
# Дождаться quorum leader election
tail -f /mnt/logs/dbms/kafka-controller.out.log | grep -E 'the leader is|Transition'
```

⚠️ Если контроллеры падают с `Job for kafka-controller.service failed because of unavailable resources` — грабля #13: `confp-init.service` не запущен. Фикс: `systemctl restart confp-init.service; sleep 5; systemctl restart kafka-controller.service`.

```bash
# Брокеры (3 шт)
systemctl start kafka-broker.service
tail -f /mnt/logs/dbms/kafka-broker.out.log | grep -E 'Successfully registered broker|Kafka Server started'

# Проверка quorum (с broker-хоста, не с controller — SSL на 9093 ломает AdminClient)
export KAFKA_HEAP_OPTS='-Xmx512m'
/opt/kafka/bin/kafka-metadata-quorum.sh --bootstrap-server <broker-fqdn>:9092 \
  --command-config /opt/kafka/config/client.properties describe --status
```

**Verify**: `kafka-metadata-quorum.sh ... describe --status` показывает `LeaderId`, `CurrentVoters: [10001,12001,11001]` (3 контроллера), `CurrentObservers: [20001,22001,21001]` (3 брокера).

### 5. Создать все топики сразу (включая `__consumer_offsets`)

`__consumer_offsets` создаём сразу вместе с бизнес-топиками. Kafka при `--create` создаст каноничные папки с пустым `.log` + новым topic_id, а 4.3-папки переименует в `<name>.<uuid>-stray` (т.к. `partition.metadata` содержит OLD topic_id).

⚠️ **`retention.ms=-1` НЕ ставить по умолчанию.** Создавай топики с теми же конфигами, что были в 4.3 (из дампа `topics_configs.txt`). Method A (удаление `.index`/`.timeindex`/`.snapshot` на Этапе 6) заставляет Kafka делать полный log recovery и восстанавливать `largestRecordTimestamp` из records — грабля #18 НЕ срабатывает, `retention.ms=-1` не нужен. Подтверждено на `test-downgrade5` 2026-08-11 (run3). Реактивный фикс — см. Этап 8: если после старта `kafka-consumer-groups --describe` не показывает часть partitions ИЛИ `ls -la /mnt/data/log/<topic>-<N>/*.log` показывает 0 байт — поставить `retention.ms=-1` через `kafka-configs --alter --add-config retention.ms=-1` и восстановить `.log` с непострадавшего брокера через `recover_log.sh`.

⚠️ `__consumer_offsets` имеет `cleanup.policy=compact` — retention по времени на нём не работает в любом случае.

```bash
B=<broker-fqdn>:9092
export KAFKA_HEAP_OPTS='-Xmx512m'

# Бизнес-топики из дампа topics_structure.txt — с теми же конфигами что в 4.3
/opt/kafka/bin/kafka-topics.sh --bootstrap-server $B --command-config /opt/kafka/config/client.properties \
  --create --topic test --partitions 3 --replication-factor 3 \
  --config min.insync.replicas=1 --config segment.bytes=1073741824
# ...повторить для каждого бизнес-топика из дампа, БЕЗ retention.ms=-1

# __consumer_offsets — сразу, без двухфазности
/opt/kafka/bin/kafka-topics.sh --bootstrap-server $B --command-config /opt/kafka/config/client.properties \
  --create --topic __consumer_offsets --partitions 50 --replication-factor 3 \
  --config cleanup.policy=compact --config compression.type=producer \
  --config min.insync.replicas=1 --config segment.bytes=104857600

# Собрать новые topic_id
/opt/kafka/bin/kafka-topics.sh --bootstrap-server $B --command-config /opt/kafka/config/client.properties \
  --describe | grep '^Topic:' > /tmp/new_topics.txt
# Скачать new_topics.txt на машину оператора, затем залить на все 3 broker-хоста в /mnt/data/new_topics.txt
```

⚠️ `__share_group_*` НЕ создавать — 3.8 не поддерживает. `__KafkaCruiseControl*`/`__CruiseControlMetrics` Kafka создаст сама при старте Cruise Control.

⚠️ Между `--create` и остановкой брокеров (Этап 6) минимизировать задержку — координатор `__consumer_offsets` может писать state в каноничные папки. Это не страшно (мы их всё равно заменим stray-папками), но лишняя активность не нужна.

**Verify**: `kafka-topics --describe` показывает все топики с NEW topic_id. На broker-хостах `ls /mnt/data/log/ | grep stray | wc -l` показывает количество stray-директорий = сумме partition count всех 4.3-топиков (для test + __consumer_offsets = 3 + 50 = 53).

### 6. Переименование stray → canonical, починить `partition.metadata`

Главная операция процедуры. На каждом broker-хосте (после остановки брокеров).

**Метод A (основной, без retention.ms=-1):** удаляем `.index`/`.timeindex`/`.snapshot`/`leader-epoch-checkpoint` — оставляем только `.log` + `partition.metadata`. Kafka при старте делает полный log recovery и восстанавливает `largestRecordTimestamp` из records (см. граблю #18).

```bash
systemctl stop kafka-broker.service

DATA_DIR=/mnt/data/log
TIDS_FILE=/mnt/data/new_topics.txt  # с машины оператора (формат: "topic tid" построчно)

get_tid() {
    awk -v t="$1" '$1==t {print $2; exit}' "$TIDS_FILE"
}

# 1. stray → canonical
find "$DATA_DIR" -maxdepth 1 -type d -name '*-stray' | while read s; do
    b=$(basename "$s")
    c=$(echo "$b" | sed -E 's/\.[^.]+-stray$//')
    target="$DATA_DIR/$c"
    [ -d "$target" ] && rm -rf "$target"
    mv "$s" "$target"
done

# 2. Для каждой topic-папки: переписать partition.metadata + удалить индексы
for t in test test2 test3 __consumer_offsets; do
    TID=$(get_tid "$t")
    [ -z "$TID" ] && { echo "WARN: no tid for $t"; continue; }
    for d in "$DATA_DIR"/${t}-*; do
        [ -d "$d" ] || continue
        # ВАРИАНТ 1 (основной): удалить индексы, оставить только .log + partition.metadata
        rm -f "$d"/*.index "$d"/*.timeindex "$d"/*.snapshot "$d"/leader-epoch-checkpoint "$d"/*-checkpoint
        printf "version: 0\ntopic_id: %s\n" "$TID" > "$d/partition.metadata"
        chown kafka:kafka "$d/partition.metadata" 2>/dev/null || true
    done
    echo "FIXED: $t -> tid=$TID"
done

echo "stray remaining: $(find $DATA_DIR -maxdepth 1 -type d -name '*-stray' | wc -l)"
```

⚠️ **Метод A (основной): удаляем `.index`/`.timeindex`/`.snapshot`/`leader-epoch-checkpoint` — оставляем только `.log` + `partition.metadata`.** Kafka при старте делает полный log recovery, сканирует `.log` и восстанавливает `largestRecordTimestamp` из реальных records. retention.ms=-1 НЕ нужен, retention можно оставить default. Минус — старт брокера дольше (секунды/минуты для больших segment-ов). Подтверждено на `test-downgrade5` 2026-08-11.

⚠️ **Метод B (ускоренный, только если Method A слишком медленный): оставить `.index`/`.timeindex`, удалить только `.snapshot`.** В этом случае `largestRecordTimestamp` НЕ восстанавливается (= 0), и нужно ставить `retention.ms=-1` на все бизнес-топики (Этап 5), иначе retention удалит segments через 30 сек после старта. `retention.ms` обратно вернуть нельзя — см. граблю #18. В скрипте выше закомментировать строку `rm -f "$d"/*.index "$d"/*.timeindex ...` (оставить только `rm -f "$d"/*.snapshot "$d"/leader-epoch-checkpoint "$d"/*-checkpoint`).

⚠️ **`.log` — НЕ ТРОГАТЬ в обоих методах.** Records от 4.3 остаются на месте, Kafka их прочитает при log recovery.

⚠️ Длинные скрипты через `mcc sshexec` отваливаются (грабля #5). Лучше залить скрипт файлом через `mcc scp` на каждый broker-хост, затем выполнить `bash /mnt/data/rename_stray.sh`.

**Verify** на каждом broker-хосте:
```bash
# Нет оставшихся stray
find /mnt/data/log -maxdepth 1 -type d -name '*-stray'  # должен вернуть пусто

# Метод A: в каждой topic-папке есть .log + partition.metadata, нет .index/.timeindex/.snapshot
ls /mnt/data/log/test-0/
cat /mnt/data/log/test-0/partition.metadata  # version: 0 + topic_id
```

### 7. Старт брокеров и восстановление SCRAM users и ACLs

Сначала стартовать брокеров (контроллеры уже active с Этапа 5):

```bash
# На broker-хостах (3 шт)
systemctl start kafka-broker.service
tail -f /mnt/logs/dbms/kafka-broker.out.log | grep -E 'Successfully registered broker|Kafka Server started'
```

Затем воссоздать SCRAM users и ACLs — они хранятся в KRaft metadata и потеряны при format (Этап 4). Воссоздать из дампа, собранного на Этапе 0.

⚠️ **В Kafka 3.8 `kafka-configs.sh --add-config 'SCRAM-SHA-256=[salt=...,stored_key=...,server_key=...,iterations=...]'` НЕ работает** —returns `Invalid credential property`. Поддерживается только формат с паролем: `SCRAM-SHA-256=[password=<plain>,iterations=8192]`. Поэтому для восстановления SCRAM user нужно знать его пароль (salt/key/serverKey из дампа KRaft meta-log нельзя использовать для восстановления через CLI). Если пароль неизвестен — создать с новым и сообщить пользователю.

```bash
B=<broker-fqdn>:9092
export KAFKA_HEAP_OPTS='-Xmx512m'

# Для каждого user из дампа (нужен пароль — узнать у пользователя):
/opt/kafka/bin/kafka-configs.sh --bootstrap-server $B --command-config /opt/kafka/config/client.properties \
  --entity-type users --entity-name <user-name> --alter \
  --add-config 'SCRAM-SHA-256=[password=<plain-password>,iterations=8192]'

# Для каждого ACL:
# resourceType: 2=TOPIC, 3=GROUP, 4=CLUSTER, 5=TRANSACTIONAL_ID
# operation: 3=READ, 4=WRITE, 8=DESCRIBE, 10=DESCRIBE_CONFIGS
/opt/kafka/bin/kafka-acls.sh --bootstrap-server $B --command-config /opt/kafka/config/client.properties \
  --add --allow-principal User:<name> --operation <OP> --topic '*' --resource-pattern-type LITERAL
```

⚠️ `super` user прописан в `broker.properties` через mdb-data секреты — его воссоздавать НЕ нужно. Воссоздавать только user-created SCRAM users (например `test-user`) и их ACLs. `kafka_exporter` ACLs обычно восстанавливаются автоматически через mdb-data при старте `kafka-exporter.service` — проверь, и только если отсутствуют, добавь вручную.

### 8. Verify

Финальная проверка кластера — включая SCRAM users и ACLs (требуется, чтобы users были восстановлены на Этапе 7).

⚠️ Kafka CLI выводит ~50 строк `AdminClientConfig values:` в stdout перед полезным выводом. Фильтруем через `grep -vE` — можно вынести в функцию или писать inline. Шаблон фильтра:
```bash
FILTER='grep -vE "AdminClientConfig|INFO|^\s|sasl|ssl|metric|retry|reconnect|request|metadata|auto|connections|default|enable|receive|send|socket"'
# Использование: <kafka-cli-cmd> 2>/dev/null | $FILTER
```

**🚨 СРАЗУ после старта брокеров (Этап 7) — обязательная проверка `.log` размеров на всех 3 брокерах.** Грабля #18 (retention удаляет `.log` с `largestRecordTimestamp=0`) недетерминирована — может сработать на 0, 1, 2 или 3 брокерах. На каждом broker-хосте:
```bash
# Для каждого бизнес-топика из дампа topics_structure.txt:
for d in /mnt/data/log/test-* /mnt/data/log/test2-*; do
    f=$(ls "$d"/*.log 2>/dev/null | head -1)
    [ -n "$f" ] && echo "$d: $(stat -c%s "$f") bytes"
done
```
Если хотя бы на одном брокере `.log` = 0 байт для partition-а, где в 4.3 были records (свериться с `consumer_groups.txt` → LOG-END-OFFSET > 0) — **грабля #18 сработала**. Немедленно:
1. Поставить `retention.ms=-1` на пострадавший топик через `kafka-configs --alter --add-config retention.ms=-1` (остановит дальнейшее удаление).
2. Найти брокер-источник (где `.log` не 0 байт — обычно 1 из 3 успевает сохранить данные).
3. Запустить `recover_log.sh` (см. ниже) для восстановления `.log`/`.index`/`.timeindex` с брокера-источника на пострадавшие.
4. После восстановления — `kafka-dump-log.sh --files .../<topic>-<N>/*.log` должен показать records.

```bash
# Проверить офсеты бизнес-топиков
/opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server <fqdn>:9092 \
  --command-config /opt/kafka/config/client.properties --topic test 2>/dev/null | \
  grep -vE 'AdminClientConfig|INFO|^\s'
# Должно показать test:0:<N> test:1:<N> test:2:<N> с офсетами из 4.3
# ⚠️ ВАЖНО: kafka-get-offsets показывает log-end-offset (starting offset пустого .log файла),
# а НЕ количество records. Если .log обнулён (грабля #18), offset будет как в 4.3, но records нет.
# Реальная проверка — kafka-dump-log.sh --files .../test-X/*.log | head (должны быть records).

# Проверить офсеты consumer-groups
/opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server <fqdn>:9092 \
  --command-config /opt/kafka/config/client.properties --all-groups --describe 2>/dev/null | \
  grep -vE 'AdminClientConfig|INFO|^\s'
# Сравнить с ~/kafka_4.3_backup/etap0_state/consumer_groups.txt (собран на Этапе 0)

# Проверить, что нет stray
find /mnt/data/log -maxdepth 1 -type d -name '*-stray'  # должен вернуть пусто

# Проверить SCRAM users и ACLs
/opt/kafka/bin/kafka-configs.sh --bootstrap-server <fqdn>:9092 \
  --command-config /opt/kafka/config/client.properties --entity-type users --describe 2>/dev/null | \
  grep -vE 'AdminClientConfig|INFO|^\s'
# Должны присутствовать все user-created SCRAM users из дампа Этапа 0
/opt/kafka/bin/kafka-acls.sh --bootstrap-server <fqdn>:9092 \
  --command-config /opt/kafka/config/client.properties --list 2>/dev/null | \
  grep -vE 'AdminClientConfig|INFO|^\s'
# ACLs для test-user и kafka_exporter должны присутствовать
```

⚠️ Если после старта `kafka-get-offsets` показывает офсеты как в 4.3, но `kafka-consumer-groups --describe` не показывает часть partitions — Kafka 3.8 обнулили `.log` при log recovery (грабля #18). Проверить через `kafka-dump-log.sh --files /mnt/data/log/test-<N>/*.log` — если `0000000000000000<offset>.log` пустой (0 байт), данные потеряны безвозвратно.

⚠️ Если `__consumer_offsets-<N>` содержит records от 4.3 (offset commits с value schema 4.3), Kafka 3.8 group coordinator может не загрузить group metadata → `consumer-groups --describe` не покажет partitions. `--reset-offsets --to-offset N` записывает новый commit, но group coordinator может его не прочитать из-за 4.3 records в начале `.log`. Фикс — удалить `__consumer_offsets-<N>` на всех 3 брокерах (с остановкой) и сделать reset заново.

#### recover_log.sh — восстановление `.log` с одного брокера на другие

При срабатывании грабли #18 (часть брокеров имеет `.log` = 0 байт). Скрипт скачивает `.log`/`.index`/`.timeindex`/`leader-epoch-checkpoint` с брокера-источника и заливает на пострадавшие брокеры. Залить на broker-хосты и запускать оттуда.

```bash
#!/bin/bash
# Usage на broker-хосте-источнике: recover_log.sh <partition> <dst_broker1> <dst_broker2>
# Например: recover_log.sh test-1 1.broker.cluster.hc.one-infra.ru 1.broker.cluster.pc.one-infra.ru
# Скрипт копирует файлы с текущего хоста (источника) на dst_broker-ы через mcc scp.
# На dst_broker-ах сервис kafka-broker.service должен быть ОСТАНОВЛЕН перед запуском.

set -e
PARTITION="$1"
shift
DST_BROKERS="$@"
DATA_DIR=/mnt/data/log

if [ -z "$PARTITION" ] || [ -z "$DST_BROKERS" ]; then
    echo "Usage: recover_log.sh <partition> <dst_broker1> [dst_broker2 ...]"
    echo "Run on broker-source (where .log is intact)"
    exit 1
fi

SRC_DIR="$DATA_DIR/$PARTITION"
if [ ! -d "$SRC_DIR" ]; then
    echo "ERROR: $SRC_DIR not found"
    exit 1
fi

# Упаковать файлы в tar (mcc scp распаковывает автоматически)
TMP_TAR="/mnt/data/recover_${PARTITION}.tar.gz"
tar -czf "$TMP_TAR" -C "$DATA_DIR" "$PARTITION"
echo "Created $TMP_TAR"

# Залить на каждый dst_broker
for dst in $DST_BROKERS; do
    echo "=== uploading to $dst ==="
    # На dst: удалить пустые .log/.index/.timeindex/.snapshot, оставить partition.metadata
    mcc scp "$TMP_TAR" "$dst:/mnt/data/" 2>&1 | tail -1
    mcc --local -n infra sshexec -n infra "$dst" "
        cd /mnt/data/log/$PARTITION && \
        rm -f *.log *.index *.timeindex *.snapshot leader-epoch-checkpoint && \
        tar -xzf /mnt/data/recover_${PARTITION}.tar.gz -C /mnt/data/log/ && \
        chown kafka:kafka /mnt/data/log/$PARTITION/* && \
        ls -la /mnt/data/log/$PARTITION/
    " 2>&1 | grep -v 'Connection closed'
done

rm -f "$TMP_TAR"
echo "DONE. Start kafka-broker.service on dst brokers and verify with kafka-dump-log.sh."
```

⚠️ Перед запуском `recover_log.sh` — поставить `retention.ms=-1` на топик, иначе retention удалит восстановленные `.log` снова.

### 9. Восстановление инфраструктуры

⚠️ **Cruise Control (cruise-хост) НЕ ТРОГАТЬ.** Не запускать `cruise-control.service`, не проверять его статус, не включать в host-check. Если cruise-хост недоступен (minion not running) — игнорировать, на даунгрейд это не влияет. CC-топики в Kafka (`__CruiseControlMetrics`, `__KafkaCruiseControl*`) Kafka создаст сама при старте, либо они останутся отсутствующими — это нормально.

- `share-group-lag-exporter` — больше не нужен (KIP-932 не поддерживается в 3.8), остановить и disable на всех broker-хостах:
  ```bash
  systemctl stop share-group-lag-exporter.service
  systemctl disable share-group-lag-exporter.service
  ```
- Дочерние сервисы после рестарта хостов НЕ запускаются автоматически (грабля #15). Запустить вручную:
  ```bash
  # На broker-хостах:
  systemctl start kafka-exporter.service rscheck@kafka.service vector.service rsyslog.service systemd-journald.service

  # На controller-хостах (kafka-exporter НЕ нужен — падает с "dependency failed"):
  systemctl start rscheck@kafka.service vector.service rsyslog.service systemd-journald.service
  ```

- ⚠️ **КРИТИЧНО: `host-check.service` + `host-check.timer` на всех 6 хостах** (3 broker + 3 controller). Без них mdb-health показывает `role=unknown, status=unknown` в UI облака (грабля #23). `host-check.service` — oneshot, отправляет репорт role/status на `https://health.mdb.one-infra.ru/api/mdb-health/host/`; `host-check.timer` запускает его периодически. После рестарта хостов оба inactive. Запустить на всех 6 хостах:
  ```bash
  systemctl start host-check.service host-check.timer
  # Verify: в логе должны быть свежие записи
  tail -3 /mnt/logs/system/host-checker.log
  # Должно быть: "status":"AVAILABLE","role":"observer" (broker) / "leader"/"follower" (controller)
  ```
  `host-check.service` после старта станет `inactive` (oneshot отработал) — это нормально. `host-check.timer` = `active`. UI облака обновится через 1-2 минуты.

**Финальная verify**:
- `kafka-topics --describe` — все партиции имеют Leader и полный Isr
- `kafka-topics --describe --under-replicated-partitions` — пусто
- `kafka-broker-api-versions.sh` — 3 брокера, версия 3.8.0
- `kafka-consumer-groups.sh --all-groups --describe` — офсеты совпадают с бэкапом
- В UI облака кластер AVAILABLE, все хосты AVAILABLE

**Эталонный список сервисов** (пример с `test-downgrade3` после восстановления):

На broker-хосте (`systemctl list-units --type=service --state=running,exited`):
```
confp-init.service               loaded active exited  Run confp as oneshot service
dbus.service                     loaded active running D-Bus System Message Bus
import-environment.service       loaded active exited  Import environment from pid 1
kafka-broker.service             loaded active running Apache Kafka broker
kafka-exporter.service           loaded active running Kafka exporter
network-wait-online.service      loaded active exited  Wait for network to be configured
rscheck@kafka.service            loaded active running RSCheck kafka service
rsyslog.service                  loaded active running System Logging Service
share-group-lag-exporter.service loaded active running Kafka share group lag exporter (KIP-932)
systemd-journald.service         loaded active running Journal Service
systemd-remount-fs.service       loaded active exited  Remount Root and Kernel File Systems
systemd-tmpfiles-setup.service   loaded active exited  Create Volatile Files and Directories
vector.service                   loaded active running Vector service for producing logs from files to kafka
```
`kafka-controller.service` на broker-хосте должен быть `failed` — это нормально (брокер и контроллер на разных хостах).

На controller-хосте:
```
confp-init.service               loaded active exited  Run confp as oneshot service
dbus.service                     loaded active running D-Bus System Message Bus
import-environment.service       loaded active exited  Import environment from pid 1
kafka-controller.service         loaded active running Apache Kafka controller
network-wait-online.service      loaded active exited  Wait for network to be configured
rscheck@kafka.service            loaded active running RSCheck kafka service
rsyslog.service                  loaded active running System Logging Service
share-group-lag-exporter.service loaded active running Kafka share group lag exporter (KIP-932)
systemd-journald.service         loaded active running Journal Service
systemd-remount-fs.service       loaded active exited  Remount Root and Kernel File Systems
systemd-tmpfiles-setup.service   loaded active exited  Create Volatile Files and Directories
vector.service                   loaded active running Vector service for producing logs from files to kafka
```
`kafka-broker.service` на controller-хосте должен быть `failed` — это нормально. `kafka-exporter.service` на controller-хосте отсутствует (`Unit kafka-exporter.service not found`) — тоже нормально, он там не нужен.

⚠️ По скиллу `share-group-lag-exporter` должен быть disabled (KIP-932 не поддерживается в 3.8). Но если он running и не мешает — можно оставить. На controller-хосте он работает как exporter, но без KIP-932 partitions будет показывать 0 — это OK.

## Критические грабли (найдены в реальной процедуре)

1. **Stray partitions — главная проблема.** Kafka помечает папку как `<name>.<uuid>-stray` если `partition.metadata` отсутствует или topic_id не совпадает с metadata кластера. Решение: создать `partition.metadata` вручную с правильным topic_id, переименовать stray → каноничное. Индексы (`.index`/`.timeindex`/`.snapshot`/`leader-epoch-checkpoint`) удалять — это Method A (Этап 6), заставляет Kafka делать полный log recovery и восстанавливать `largestRecordTimestamp` из records (фикс грабли #18). `.log` НЕ ТРОГАТЬ.

2. **`partition.metadata` нельзя просто удалить** (как пишут в vanilla-инструкциях) — Kafka всё равно сделает stray, потому что в `.log` остаются producer state с старым topic_id. Нужно именно ПОДСТАВИТЬ правильный topic_id.

3. **Topic id новый у каждого топика** в новом кластере 3.8. Узнавать через `kafka-topics --describe` после `--create`. В этой процедуре `__consumer_offsets` создаётся вместе с бизнес-топиками (не двухфазно) — Kafka переименует 4.3-папки в stray, мы вернём обратно.

4. **`__share_group_*` не переносить** — 3.8 не знает share-протокол KIP-932. Удалить с broker-хостов на Этапе 3.

5. **`mcc sshexec` отваливается по "Connection closed by remote host"** на длинных скриптах. Решение: писать операцию как compact one-liner через `for ... done` или `find ... -exec`, или заливать скрипт файлом через `mcc scp` и выполнять `bash /mnt/data/script.sh`.

6. **SSL Handshake "Too early"** при `mcc sshexec` — повторить через 10-15 секунд, само проходит.

7. **`mcc scp` на /tmp падает с EOF** — лить данные в `/mnt/data/` (там больше места и нет лимитов jail). Также: `mcc scp src host:/mnt/data/name.tar.gz` создаёт **директорию** `name.tar.gz/`, а не файл. Лить в `host:/mnt/data/` (каталог), mcc сам положит файл с именем source. При скачивании `mcc scp host:/mnt/data/file.tar.gz ./` — архив может автоматически распаковаться в каталог назначения (поведение mdb-specific).

8. **`kafka CLI` падает с OOM** — `export KAFKA_HEAP_OPTS='-Xmx512m'` перед запуском.

9. **`--bootstrap-server localhost:9092` падает с SSL handshake failed** — использовать FQDN (`1.broker.<cluster>.<dc>.one-infra.ru:9092`), потому что `advertised.listeners` указывает на FQDN и клиент переключается на него после начального handshake.

10. **`KAFKA_ROLE is not set` на pre-start** — не трогать `/etc/sysconfig/kafka` (по пользовательской инструкции роли уже правильно прописаны в env хоста). Проблема в чём-то другом, обычно в том что `confp --oneshot` не отрендерил конфиги.

11. **`confp --oneshot`** может перерендерить конфиги в любой момент. Если процедура затягивается — действовать быстро и повторять `confp --oneshot` перед каждым стартом.

12. **Контейнер пересоздаётся после неудачного старта**, правки пропадают — повторять `confp --oneshot` перед каждым стартом.

13. **После рестарта хостов `confp-init.service` нужно запускать вручную** — он не auto-start (static enabled, но не запускается при буте). Без него НЕ рендерятся: `/etc/sysconfig/kafka`, `/opt/kafka/scripts/pre-start-*.sh`, `/opt/kafka/config/{broker,controller}.properties`. Симптом: `systemctl start kafka-broker.service` падает с `Job for kafka-broker.service failed because of unavailable resources or another system error` (systemd не может найти ExecStartPre / EnvironmentFile). Фикс: `systemctl start confp-init.service` на всех 6 хостах. После этого рендерятся все нужные файлы и сервисы стартуют. **Также срабатывает после переключения docker-образа** (Этап 2) — контейнер пересоздаётся, но confp-init автоматически не запускается; без ручного старта `/opt/kafka/config/broker.properties: No such file or directory`.

14. **`KAFKA_CLUSTER_ID` в env хоста** — это UUID (например `032674a5-bce0-4359-92a5-e91c2d9bb805`), а не Kafka cluster ID от `kafka-storage.sh random-uuid` (base64, 22 символа). Это РАЗНЫЕ значения, и `kafka-storage.sh` принимает оба формата. С `--ignore-formatted` (а pre-start использует именно этот флаг) совпадение не проверяется — Kafka стартует с cluster.id из `meta.properties`. Менять `cluster.id` в `meta.properties` под env UUID НЕ нужно.

15. **После рестарта хостов дочерние сервисы НЕ запускаются автоматически.** На работающем хосте должны быть running: `kafka-broker` (или `kafka-controller`), `kafka-exporter` (только на брокерах), `rscheck@kafka`, `rsyslog`, `systemd-journald`, `vector`. После рестарта хостов + `confp-init.service` стартует только `kafka-broker.service`/`kafka-controller.service`, остальное остаётся inactive. Симптом: `systemctl list-units --type=service` показывает неполный список по сравнению с обычным хостом. Фикс — вручную на каждом хосте (см. Этап 10). На cruise-хосте `kafka-exporter.service` отсутствует (`Unit kafka-exporter.service not found`) — это нормально. На controller-хостах `kafka-exporter.service` падает с `A dependency job for kafka-exporter.service failed` — тоже нормально, он там не нужен.

16. **`CF=--command-config /opt/kafka/config/client.properties` без кавычек ломает bash.** Bash интерпретирует как `CF=--command-config` (переменная) + попытку выполнить `/opt/kafka/config/client.properties` как команду → `Permission denied`. Правильно: передавать `--command-config /opt/kafka/config/client.properties` как отдельные аргументы.

17. **CC-топики от 4.3 (`__CruiseControlMetrics-*`, `__KafkaCruiseControl*`) вызывают `CorruptRecordException` при старте Kafka 3.8.** Симптом: `Found record size 0 smaller than minimum record overhead (14) in file /mnt/data/log/__CruiseControlMetrics-2/00000000000000000000.log`, брокер падает. Фикс — удалить все CC-директории на всех брокерах перед стартом (Этап 3), Kafka 3.8 создаст их заново при запуске Cruise Control.

18. **🚨 RETENTION удаляет восстановленные `.log` от 4.3.** После rename stray → canonical и старта брокеров Kafka 3.8 запускает log recovery. Без `.snapshot` (мы его удалили на Этапе 6) Kafka 3.8 НЕ восстанавливает `largestRecordTimestamp` в `LogSegment` из индексов → считает = 0 (т.е. `1970-01-01 00:00:00 UTC`). При `retention.ms=604800000` (default 7 дней) retention видит сегменты с timestamp=0 как устаревшие → удаляет через ~30 сек после старта.

    **Почему 1970 год:** `largestRecordTimestamp` в `LogSegment` хранится в памяти и персистится через `.snapshot` файл (producer state). Если `.snapshot` удалён — Kafka 3.8 при log recovery **не сканирует `.log` автоматически**, а считает `largestRecordTimestamp=0`. Простого пути «исправить» `largestRecordTimestamp=0` нет — `kafka-dump-log` читает, но не пишет; патч `.timeindex` вручную опасен (бинарный формат).

    **Два метода работы (см. Этап 6):**

    - **Method A (основной, рекомендуется) — удалить `.index`/`.timeindex`/`.snapshot`/`leader-epoch-checkpoint` на Этапе 6.** Тогда Kafka не может загрузить segment metadata из индексов и **вынуждена сделать полный log recovery**: построчно сканирует `.log` и восстанавливает `maxTimestampSoFar` из реальных records (`CreateTime` каждого record). После этого `largestRecordTimestamp` = реальный timestamp последней записи, retention не срабатывает, `retention.ms=-1` НЕ нужен. Минус — старт брокера дольше (секунды/минуты для больших segment-ов 1 GB). Подтверждено на `test-downgrade5` 2026-08-11 (run3): грабля #18 НЕ сработала, `.log` сохранены на всех 3 брокерах.

    - **Method B (ускоренный, только если Method A слишком медленный) — оставить `.index`/`.timeindex`, удалить только `.snapshot`, поставить `retention.ms=-1`.** Брокер стартует быстрее (без полного сканирования `.log`), но `largestRecordTimestamp` остаётся = 0 и retention отключается навсегда. Возвращать `retention.ms` обратно нельзя — retention снова удалит segments с timestamp=0. Применять только на проде с большими segment-ами, где Method A слишком медленный.

    **Если Method B применён реактивно (грабля #18 уже сработала на части брокеров):** восстановить `.log`/`.index`/`.timeindex` с непострадавшего брокера через `recover_log.sh` (Этап 8). Обязательно поставить `retention.ms=-1` ДО восстановления, иначе retention удалит восстановленные `.log` снова.

    **Кластер `test-downgrade7` (2026-08-11, run1 — БЕЗ Method A, сработала грабля #18):** `test-*`/`test2-*`/`test3-*` `.log` обнулены на broker.kc и broker.pc (2 из 3 брокеров). На broker.hc данные сохранились (retention не успел сработать). Восстановлено: `retention.ms=-1` + `recover_log.sh` с broker.hc на broker.kc/pc. `consumer-group3/test3:1` потеряны records 0..157 (LOG-END-OFFSET уже сместился на 158 до восстановления).

    **Кластер `test-downgrade3-mdbdev-kafka` (2026-08-09):** `test-2` (172 records от 4.3) обнулён retention через 30 сек после старта. Восстановлено копированием `.log` + `.index` + `.timeindex` + `leader-epoch-checkpoint` с broker kc + `retention.ms=-1` на топике `test`.

    **Кластер `test-downgrade5` (2026-08-11, run3 — Method A, УСПЕХ без retention.ms=-1):** грабля #18 НЕ сработала. Удалены `.index`/`.timeindex`/`.snapshot`/`leader-epoch-checkpoint` на Этапе 6 → Kafka сделала полный log recovery → `largestRecordTimestamp` восстановлен из records → retention не сработал. `kafka-dump-log` подтвердил records с реальными `CreateTime` (2026-08-06), не 1970.

    **Симптом потери:** `kafka-get-offsets --topic test` показывает `test:2:172` (как в 4.3), но `ls -la /mnt/data/log/test-2/` показывает `00000000000000000172.log` размером 0 байт. `kafka-get-offsets` показывает log-end-offset (starting offset файла), а НЕ количество records. Реальная проверка — `kafka-dump-log.sh --files .../test-2/*.log` (должны быть records) или `ls -la` (размер .log > 0).

19. **`kafka-consumer-groups.sh --reset-offsets --to-offset N` НЕ работает, если N > log end offset.** После обнуления `.log` (грабля #18) log end offset = 0 (или starting offset пустого файла), и reset к офсету из бэкапа выдаёт «New offset (149) is higher than latest offset for topic partition test-1. Value will be set to 0». Единственный путь восстановить офсеты — поднять `.log` файлы `__consumer_offsets` с сохранёнными `.index`/`.timeindex` от 4.3.

20. **`kafka-get-offsets` показывает log-end-offset, а НЕ количество records.** После обнуления `.log` (грабля #18) Kafka создаёт новый пустой файл `0000000000000000<N>.log` где N = старый log-end-offset. `kafka-get-offsets` показывает N (как в 4.3), но actual records = 0. Реальная проверка — `kafka-dump-log.sh --files .../*.log` (должны быть records) или `ls -la` (размер .log должен быть > 0).

21. **`__consumer_offsets-<N>` с 4.3 records ломает group coordinator.** Если в `__consumer_offsets-<N>` есть records от 4.3 (offset commits с value schema 4.3), Kafka 3.8 group coordinator не может десериализовать value и не загружает group metadata. `--reset-offsets --to-offset N` записывает новый commit (в новый segment), но group coordinator не читает его из-за 4.3 records в начале `.log`. Симптом: `consumer-groups --describe` показывает не все partitions. Фикс — удалить `__consumer_offsets-<N>` на всех 3 брокерах (с остановкой) и сделать reset заново.

22. **🚨 После рестарта хоста `confp-init.service` пересоздаёт `bootstrap.checkpoint` с metadata.version от 4.3 (feature level 30).** Kafka 3.8 не знает про feature level 30 → падает при старте с `java.lang.IllegalArgumentException: No MetadataVersion with feature level 30` (stack: `BootstrapDirectory.readFromBinaryFile → KafkaRaftServer.initializeLogDirs`). Симптом: `systemctl start kafka-broker.service` (или controller) — `Main process exited, code=exited, status=1/FAILURE`, в `kafka-broker.out.log` стек с `MetadataVersion.fromFeatureLevel`. Фикс — удалить `bootstrap.checkpoint` и стартовать заново:
    ```bash
    # На broker-хосте:
    rm -f /mnt/data/log/bootstrap.checkpoint
    systemctl restart kafka-broker.service
    # На controller-хосте:
    rm -f /mnt/data/metadata/bootstrap.checkpoint /mnt/data/log/bootstrap.checkpoint
    systemctl restart kafka-controller.service
    ```
    Kafka 3.8 при старте создаст новый `bootstrap.checkpoint` с правильной metadata.version (3.8-IV0 = feature level 25). Срабатывать может не при первом рестарте — наблюдается на `test-downgrade3` после рестарта broker.kc и controller.kc.

23. **После рестарта хостов `host-check.service` и `host-check.timer` не активны → mdb-health показывает `role=unknown, status=unknown` в UI.** Симптом: в UI облака все хосты кластера показывают `unknown` для роли и статуса, хотя `kafka-broker.service`/`kafka-controller.service` active. Причина: `host-check.service` (oneshot, запускается по `host-check.timer`) не был активирован после рестарта → host_checker скрипт не отправляет репорты на `https://health.mdb.one-infra.ru/api/mdb-health/host/` → в Redis mdb-health нет свежих данных role/status → UI показывает unknown. Проверка: `tail /mnt/logs/system/host-checker.log` — если последняя запись старая (или файл отсутствует), host-check не работает. Фикс — на всех 7 хостах (3 broker + 3 controller + 1 cruise):
    ```bash
    systemctl start host-check.service host-check.timer
    ```
    `host-check.service` после старта станет `inactive` (это oneshot — запустился, отправил репорт, завершился), `host-check.timer` = `active` (будет запускать сервис периодически). В логе `host-checker.log` должны появиться свежие записи с `"status":"AVAILABLE","role":"observer"` (broker) / `"role":"leader"`/`"follower"` (controller). UI обновится через ~1-2 минуты.

## Откат (если что-то пошло не так)

Откат ограничен — `.log` файлы на брокерах не имеют бэкапа. Если процедура зашла далеко (Этап 7 уже переименовал stray → canonical), бизнес-данные в каноничных папках с `partition.metadata` от 3.8. Возврат на 4.3 потребует либо:

**а) Откат до Этапа 3** (KRaft уже очищен, но `.log` ещё в 4.3-структуре):
1. Остановить сервисы.
2. На controller-хостах: `rm -rf /mnt/data/metadata/*` и развернуть `kraft_meta.tar.gz` из `~/kafka_4.3_backup/controller-<dc>/`.
3. На broker-хостах: `rm -f /mnt/data/log/meta.properties /mnt/data/log/bootstrap.checkpoint` (если появились от format), topic-папки НЕ ТРОГАТЬ.
4. Переключить docker-образ на 4.3.
5. `systemctl start confp-init.service` на всех 6 хостах.
6. Запустить контроллеры → брокеры. 4.3 поднимется с исходными `.log` и `partition.metadata`.

**б) Откат после Этапа 6** (stray уже переименованы в canonical, `partition.metadata` переписан):
1. Остановить сервисы.
2. На controller-хостах: восстановить `kraft_meta.tar.gz`.
3. На broker-хостах: `partition.metadata` в topic-папках содержит 3.8 topic_id. Для 4.3 это mismatch → папки уйдут в stray. Нужно либо восстановить OLD `partition.metadata` (нет бэкапа — только если снять с другого 4.3-кластера), либо accept что 4.3 поднимется с пустыми топиками.
4. Переключить docker-образ на 4.3, `confp-init`, старт.

⚠️ **Если нужен гарантированный откат на 4.3 с данными — делай полный бэкап `log.dirs` до начала процедуры.** Эта процедура экономит время за счёт риска.

## Справочные скиллы

- `kafka-cluster-inspector` — архитектура, формат хостов, каталог проблем
- `kafka-host-inspector` — путеводитель по путям на хосте
- `mcc-host-access` — паттерны `mcc ssh`/`mcc scp` с expect
- `kafka-log-investigator` — что грепать в логах
- `kafka-metrics-investigator` — проверка через MBean/JMX

## История процедур

В папке `history/` рядом с этим скиллом лежат отчёты о реальных даунгрейдах (по файлу на кластер/раунт). Перед запуском новой процедуры полезно свериться с похожим кейсом оттуда — особенно с тем, сработала ли грабля #18 (retention удаляет `.log`) на свежем кластере.

- `test-downgrade5_run1.md` — первый даунгрейд 2026-08-09; данные сохранены через `retention.ms=-1`
- `test-downgrade5_run2.md` — второй даунгрейд 2026-08-09; эксперимент с `retention.ms=604800000` — грабля #18 НЕ сработала на свежем кластере
- `test-downgrade7.md` — даунгрейд 2026-08-10; данные сохранены без `retention.ms=-1`; впервые восстановлены SCRAM users и ACLs
- `test-downgrade6.md` — даунгрейд 2026-08-10; данные сохранены без `retention.ms=-1`; кластер без cruise-хоста (процедура не трогала cruise)
- `test-downgrade6_run2.md` — повторный даунгрейд 2026-08-10; грабля #18 сработала на 2 из 3 брокеров (hc, pc), данные восстановлены копированием .log с broker.kc; подтверждение что грабля #18 недетерминирована
- `test-downgrade7_run2.md` — повторный даунгрейд 2026-08-11; грабля #18 сработала на broker.kc и broker.pc (2 из 3), данные восстановлены через `retention.ms=-1` + `recover_log.sh` с broker.hc; потеряны records 0..157 в `consumer-group3/test3:1`
- `test-downgrade5_run3.md` — даунгрейд 2026-08-11; **Method A (удаление `.index`/`.timeindex`/`.snapshot`)** — грабля #18 НЕ сработала, retention.ms=-1 НЕ нужен; `kafka-dump-log` подтвердил records с реальными `CreateTime` (2026-08-06), не 1970

## Чек-лист результата

- [ ] 3 брокера active, 3 контроллера в quorum
- [ ] `kafka-topics --describe` — все партиции с Leader и полным Isr
- [ ] `--under-replicated-partitions` — пусто
- [ ] `kafka-broker-api-versions.sh` — 3 брокера, версия 3.8.0
- [ ] `kafka-consumer-groups --all-groups --describe` — офсеты совпадают с бэкапом
- [ ] `kafka-configs --entity-type users --describe` — все SCRAM users из дампа Этапа 0 восстановлены
- [ ] `kafka-acls --list` — все ACLs из дампа Этапа 0 восстановлены
- [ ] `kafka-dump-log.sh --files .../test-<N>/*.log` — records присутствуют (не 0 байт)
- [ ] `.log` размеры > 0 на ВСЕХ 3 брокерах для partition-ов с records (грабля #18 — обязательная проверка сразу после старта)
- [ ] 0 stray директорий в `/mnt/data/log/`
- [ ] Все сервисы running: `kafka-broker`/`kafka-controller`, `kafka-exporter` (только брокеры), `rscheck@kafka`, `vector`, `rsyslog`, `systemd-journald`
- [ ] `host-check.timer` active на всех 6 хостах (3 broker + 3 controller), в `/mnt/logs/system/host-checker.log` свежие репорты с role/status
- [ ] В UI облака кластер AVAILABLE, все хосты AVAILABLE с корректными ролями (broker=observer, controller=leader/follower). Cruise-хост не проверяем.

⚠️ **Бизнес-данные сохраняются НЕ ВСЕГДА — есть проблема с разметкой timestamps (грабля #18).** По умолчанию применяется **Method A** (удаление `.index`/`.timeindex`/`.snapshot` на Этапе 6) — Kafka делает полный log recovery, `largestRecordTimestamp` восстанавливается из records, `retention.ms=-1` НЕ нужен. Подтверждено на `test-downgrade5` 2026-08-11 (run3). Если Method A применить нельзя (прод с большими segment-ами, слишком медленный старт) — применяется Method B (оставить `.index`/`.timeindex`, `retention.ms=-1` навсегда). Реактивный фикс при срабатывании грабли #18 — `recover_log.sh` с непострадавшего брокера (Этап 8).
