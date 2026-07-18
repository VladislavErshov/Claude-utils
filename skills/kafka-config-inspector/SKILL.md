---
name: kafka-config-inspector
description: Инспекция конфиг-файлов Kafka-хостов — сверка PMS-переменных (pms.cloud.vk.team API) с отрендеренными конфиг-файлами на хостах (broker.properties, controller.properties, cruisecontrol.properties, capacity.json, sysconfig, jaas.conf, log4j.properties, tools-log4j.properties). Список хостов берётся из БД pg_backstage_plugin_mdb, файлы читаются через mcc scp. Используй когда нужно проверить, что PMS-API значения физически применились в /opt/kafka/config/ после modify-флоу. Скилл проверяет только property-файлы — он НЕ проверяет здоровье кластера (ISR, replication, partition balance и т.п.).
allowed-tools: [Bash, Read, Write, Edit, Grep, Glob]
---

# Скилл инспекции конфиг-файлов Kafka-хостов

Скилл сверяет property-файлы Kafka-хостов с двух сторон:
1. **PMS-API** (`pms.cloud.vk.team`) — что записано в PMS-переменные (`kafka.broker.properties`,
   `kafka.controller.properties`, `kafka.cruisecontrol.*`, `kafka.sysconfig`, `kafka.soc.audit.enabled` и т.д.)
2. **Конфиг-файлы на хостах** — что физически лежит в `/opt/kafka/config/` и `/opt/cruise-control/config/`,
   отрендеренное из PMS-шаблонов modify-флоу mdb-processing.

⚠️ Скилл проверяет **только конфиг-файлы**. Он НЕ инспектирует состояние кластера как целого:
брокеры в ISR, replication factor, partition balance, leader election, consumer lag — всё это
за пределами области действия. Только сверка «PMS-API ↔ отрендеренный файл на хосте».

Список хостов берётся из локальной БД `pg_backstage_plugin_mdb` (`host_state` по
`cluster_id`). Файлы скачиваются через `mcc scp -n infra`.

## Когда применять

- После modify-флоу — убедиться что PMS-API получил значения **и** что они
  отрендерились в файлы на хостах (например, `KAFKA_HEAP_OPTS` в `/opt/kafka/config/sysconfig`
  совпадает с `kafka.sysconfig` в PMS-API).
- При разборе «почему broker игнорирует новую конфигурацию» — PMS-API может быть
  обновлён, а файлы на хосте не перерисованы (или наоборот — файлы есть, PMS пустой).
- Для инспекции любого состояния Kafka-кластера без запуска modify.

## Что нужно

- **mTLS-сертификаты** в `~/.mccloud/` (`client.cert`, `client.key`, `ca.crt`) — для
  PMS-API.
- **mcc** (`/Users/vl.ershov/Documents/mcc/mcc`, есть в PATH) — для доступа к хостам.
  Всегда `mcc --local` (`-l`), чтобы mcc не тянул свежую версию с мастера на каждый вызов.
  Для чтения файлов используем `mcc scp`. Для выполнения команд на хосте — `mcc ssh` + `expect`
  (`mcc ssh` не принимает command как аргумент, но обёртка `expect` работает — см.
  `kafka-cluster-inspector/commands/run_commands.md`). `mcc sshexec` таймаутит к cloud-ops
  узлам (TLS handshake timeout) — не использовать.
- **Локальная БД** `pg_backstage_plugin_mdb` в docker-контейнере `pg_backstage_plugin_mdb`
  (порт 6434) — для списка хостов.

## Шаг 1: получить хосты кластера

```bash
docker exec pg_backstage_plugin_mdb psql -U dev -d backstage_plugin_mdb -tA -c \
  "SELECT host, params->>'dc' AS dc FROM host_state
   WHERE cluster_id='<CLUSTER_ID>' ORDER BY host;"
```

Хосты имеют вид (числовой префикс — порядковый номер компонента в DC **может быть любым**):
- `<N>.broker.<cluster-name>.<dc>.one-infra.ru` — broker. Брокеров в кластере может быть
  **произвольное количество** (1, 2, 3, …) — число определяется конфигурацией кластера.
- `<N>.controller.<cluster-name>.<dc>.one-infra.ru` — controller (KRaft)
- `1.cruise.<cluster-name>.<dc>.one-infra.ru` — cruise-control

При выборке хостов из БД фильтруй по префиксу `LIKE '%.broker.%'` / `'%.controller.%'` /
`'%.cruise.%'`, а не по конкретной цифре — иначе пропустишь 3-й, 4-й и т.д. брокеров.

## Шаг 2: PMS-API значения

Используй готовый скрипт `pms-read.sh` или напрямую:

```bash
# Все известные Kafka PMS-переменные для хоста (19 штук, см. KNOWN_PROPERTIES в pms-read.sh):
~/.claude/skills/kafka-config-inspector/bin/pms-read.sh <host>

# Одна переменная:
~/.claude/skills/kafka-config-inspector/bin/pms-read.sh <host> kafka.sysconfig

# Несколько ключевых для modify-флоу:
~/.claude/skills/kafka-config-inspector/bin/pms-read.sh <host> kafka.soc.audit.enabled
~/.claude/skills/kafka-config-inspector/bin/pms-read.sh <host> kafka.broker.properties
~/.claude/skills/kafka-config-inspector/bin/pms-read.sh <host> kafka.controller.properties
~/.claude/skills/kafka-config-inspector/bin/pms-read.sh <host> kafka.cruisecontrol.properties
```

⚠️ PMS-API — **read-only**. Менять PMS-файлы через `POST /api/conf/update.do` /
`DELETE /api/conf/delete.do` запрещено. PMS модифицируется только modify-флоу
mdb-processing.

## Шаг 3: файлы на хостах (через mcc scp)

⚠️ **Destination — всегда директория, не путь к файлу.** `mcc scp` кладёт скачанное
внутрь указанной локальной директории (она должна существовать, `mkdir -p`). Если указать
путь к файлу — падает с `failed to open destination directory ...: no such file or directory`.

⚠️ **Namespace**: на некоторых кластерах `mcc scp` падает с `NamespaceMissingException` —
тогда добавь `-n infra`. На dev-кластерах (mcc v0.29.0) scp обычно работает и без флага.

### Скачать конфиги broker/controller (файлы в `/opt/kafka/config/` + `/etc/sysconfig/kafka`)

⚠️ `sysconfig` рендерится в **`/etc/sysconfig/kafka`**, НЕ в `/opt/kafka/config/sysconfig`
(см. `docker-images/ubuntu20-kafka-base/rootfs/etc/confp/resources.d/kafka.yml:38-39`).
Качать отдельно, потому что вне `/opt/kafka/config/`.

```bash
HOST=1.broker.test-resize-mdbdev-kafka.dc.one-infra.ru
mkdir -p /tmp/kafka-inspect/$HOST

# Вся директория /opt/kafka/config/ — одним tarball (обходит баг mcc с файлами без расширения)
mcc --local scp -n infra "$HOST:/opt/kafka/config/" /tmp/kafka-inspect/$HOST/ 2>&1 | tail -5

# sysconfig — отдельно (он вне /opt/kafka/config/). Destination — директория, не файл!
mkdir -p /tmp/kafka-inspect/$HOST/sysconfig
mcc --local scp -n infra "$HOST:/etc/sysconfig/kafka" /tmp/kafka-inspect/$HOST/sysconfig/ 2>&1 | tail -3
```

### Скачать конфиги cruise-control (файлы в `/opt/cruise-control/config/` + `/etc/sysconfig/cruise-control`)

⚠️ Cruise-control файлы лежат в **`/opt/cruise-control/config/`**, НЕ в `/opt/kafka/config/`
(см. `docker-images/ubuntu20-mdb-cruisecontrol/rootfs/etc/confp/resources.d/cruise-control.yml`).
`cruisecontrol-sysconfig` рендерится в **`/etc/sysconfig/cruise-control`**.

```bash
HOST=1.cruise.test-resize-mdbdev-kafka.dc.one-infra.ru
mkdir -p /tmp/kafka-inspect/$HOST

mcc --local scp -n infra "$HOST:/opt/cruise-control/config/" /tmp/kafka-inspect/$HOST/ 2>&1 | tail -5
mkdir -p /tmp/kafka-inspect/$HOST/sysconfig
mcc --local scp -n infra "$HOST:/etc/sysconfig/cruise-control" /tmp/kafka-inspect/$HOST/sysconfig/ 2>&1 | tail -3
```

⚠️ `mcc scp` одиночного файла иногда падает с `failed to read downloaded archive
header: EOF` — это баг mcc для файлов без расширения (например `sysconfig`,
`jaas.conf`). Если упало — качай всю директорию целиком (там mcc отдаёт tarball и
распаковывает сам). Для `sysconfig` принципиально качать с правильного пути
(`/etc/sysconfig/kafka`), не из `/opt/kafka/config/`.

### Структура путей по типу хоста

Пути подтверждены по `docker-images/ubuntu20-kafka-base/rootfs/etc/confp/resources.d/kafka.yml`
и `docker-images/ubuntu20-mdb-cruisecontrol/rootfs/etc/confp/resources.d/cruise-control.yml`.

| Тип хоста (по FQDN)                                | Директория для свойств | sysconfig-путь | Какие файлы проверять |
|----------------------------------------------------|---|---|---|
| `<N>.broker.*` (N — любой, брокеров может быть ≥1) | `/opt/kafka/config/` | `/etc/sysconfig/kafka` | `broker.properties`, `log4j.properties`, `tools-log4j.properties`, `jaas.conf`, `client.properties` |
| `<N>.controller.*`                                 | `/opt/kafka/config/` | `/etc/sysconfig/kafka` | `controller.properties`, `log4j.properties`, `tools-log4j.properties`, `jaas.conf`, `client.properties` |
| `1.cruise.*`                                       | `/opt/cruise-control/config/` | `/etc/sysconfig/cruise-control` | `cruisecontrol.properties`, `capacity.json`, `log4j.properties`, `cruise_control_jaas.conf` |

### Файлы для проверки по типу хоста

Хосты в `host_state` имеют префикс `<N>.broker.<name>...` / `<N>.controller.<name>...` /
`<N>.cruise.<name>...` — это определяет какие файлы на нём надо смотреть. `N` — порядковый
номер компонента, может быть любым (1, 2, 3, …); брокеров в кластере может быть больше двух.

**Broker-хост** (`<N>.broker.*`, N — любой) — директория `/opt/kafka/config/` + `/etc/sysconfig/kafka`:

| Файл | PMS-переменная | Что проверять |
|---|---|---|
| `/opt/kafka/config/broker.properties` | `kafka.broker.properties` | `num.io.threads`, `compression.type`, `num.network.threads`, и т.п. из `brokerConfig.config` modify-запроса |
| `/opt/kafka/config/log4j.properties` | `kafka.log4j.properties` | log4j appender config |
| `/opt/kafka/config/tools-log4j.properties` | `kafka.tools.log4j.properties` | log4j для CLI-утилит |
| `/opt/kafka/config/jaas.conf` | `kafka.users` (j2-шаблон `jaas.conf.j2`) | SASL auth: блок `KafkaServer` (users + vault-пароли) |
| `/opt/kafka/config/client.properties` | (j2-шаблон, не PMS) | client config для admin-утилит |
| `/etc/sysconfig/kafka` | `kafka.sysconfig` | `KAFKA_HEAP_OPTS` (broker heap size), `KAFKA_OPTS` (tosAgent javaagent если `tosAgent=true`) |

**Controller-хост** (`<N>.controller.*`) — те же пути, что у broker, но `controller.properties` вместо `broker.properties`:

| Файл | PMS-переменная | Что проверять |
|---|---|---|
| `/opt/kafka/config/controller.properties` | `kafka.controller.properties` | параметры из `controllerConfig.config` modify-запроса (`num.io.threads` и т.п.) |
| `/opt/kafka/config/log4j.properties` | `kafka.log4j.properties` | — |
| `/opt/kafka/config/tools-log4j.properties` | `kafka.tools.log4j.properties` | — |
| `/opt/kafka/config/jaas.conf` | `kafka.users` (j2-шаблон `jaas.conf.j2`) | SASL auth (как у broker) |
| `/opt/kafka/config/client.properties` | (j2-шаблон) | — |
| `/etc/sysconfig/kafka` | `kafka.sysconfig` | `KAFKA_HEAP_OPTS` (controller heap = `controllerJvmHeapSizeMb` из modify) |

**Cruise-control хост** (`1.cruise.*`) — директория `/opt/cruise-control/config/` + `/etc/sysconfig/cruise-control`:

| Файл | PMS-переменная | Что проверять |
|---|---|---|
| `/opt/cruise-control/config/cruisecontrol.properties` | `kafka.cruisecontrol.properties` | `bootstrap.servers`, `security.protocol`, auto-rebalance, replication.throttle |
| `/opt/cruise-control/config/capacity.json` | `kafka.cruisecontrol.capacity.json` | disk/nw capacity |
| `/opt/cruise-control/config/log4j.properties` | `kafka.cruisecontrol.log4j.properties` | log4j для cruise |
| `/opt/cruise-control/config/cruise_control_jaas.conf` | `kafka.cruisecontrol.jaas.conf` | SASL auth для cruise-control |
| `/etc/sysconfig/cruise-control` | `kafka.cruisecontrol.sysconfig` | `KAFKA_HEAP_OPTS` для cruise (должен быть `cruiseControl.jvmHeapSizeMb` из modify) |

### SOC audit appender (socKafkaAppender) в `log4j.properties`

В `log4j.properties` на broker/controller-хостах рендерится **SOC audit appender** —
`log4j.appender.socKafkaAppender` (KafkaLog4jAppender), который шлёт SOC-события аудита
(request logger, authorizer logger, network Selector) в отдельный Kafka-топик. Этот appender
в народе называется «socLogger».

Шаблон `log4j.properties` лежит в соседнем проекте **backstage**:
`plugins/mdb-backend/src/task/manifest/templates/kafka-log-config` (j2-шаблон с `pms(...)`
вызовами). Для cruise-control есть отдельный шаблон `kafka-cruise-control-log-config` —
в нём SOC appender-а **нет**, только обычные log4j-loggers.

Шаблон `kafka-log-config` рендерит в `log4j.properties` SOC-блок по флагу из PMS:

| PMS-переменная | Куда рендерится | Default в шаблоне |
|---|---|---|
| `kafka.soc.audit.enabled` | Включает блок `socKafkaAppender` и SOC-loggers (`{% if pms('kafka.soc.audit.enabled', "false") == "true" %}`). Если `false` — весь блок отсутствует. | `"false"` |

Остальные параметры appender-а (`brokerList`, `topic`, `user`, `password`) заданы в шаблоне
жёстко и через PMS не управляются — сверять их с PMS-API не нужно.

Что проверять в `/opt/kafka/config/log4j.properties` на broker/controller:
- Если PMS `kafka.soc.audit.enabled=true` → в файле **должен быть** блок
  `log4j.appender.socKafkaAppender=org.apache.kafka.log4jappender.KafkaLog4jAppender`
  и SOC-loggers (`log4j.logger.kafka.request.logger=TRACE, socKafkaAppender`,
  `log4j.logger.kafka.authorizer.logger=DEBUG, socKafkaAppender`,
  `log4j.logger.org.apache.kafka.common.network.Selector=INFO, stdout, socKafkaAppender`).
- Если PMS `kafka.soc.audit.enabled=false` (или `<NOT_SET>`) → блока `socKafkaAppender`
  в файле быть **не должно**, `Selector` без `socKafkaAppender` в appenderRefs.

⚠️ Шаблон `kafka-log-config` также вставляет `cluster_id:{{ env('MDB_CLUSTER_ID') }}` в
ConversionPattern SOC-appender-а — можно сверять что cluster_id в log4j совпадает с
фактическим cluster_id кластера.

### Файлы, которые НЕ рендерятся через confp

- `kafka.layout`, `kafka.controller.quorum`, `kafka.isWanCluster`,
  `kafka.ssl.enabled`, `kafka.hostInfo.pushUrl`, `zen.kafka.vaultRoot`,
  `kafka.keystore/truststore.password.vault.path` — также потребляются скриптами
  pre-start, не рендерятся в статические файлы. Только скрипт `pms-read.sh`.

### Дополнительные файлы (могут быть на хостах)

- `/opt/kafka/config/keystore` / `truststore` — SSL артефакты (если `kafka.ssl.enabled=true`), генерируются `create_keystore.sh`
- `/opt/kafka/scripts/pre-start-kafka-broker.sh` / `pre-start-kafka-controller.sh` — сами скрипты pre-start
- `/etc/rscheck/kafka.conf` — rscheck config (мониторинг)

### Что сравнивать PMS-API ↔ файл

| PMS-API | Файл | Совпадение |
|---|---|---|
| `kafka.sysconfig` (KAFKA_HEAP_OPTS) | `/etc/sysconfig/kafka` (KAFKA_HEAP_OPTS) | heap size точно совпадает |
| `kafka.sysconfig` (KAFKA_OPTS) | `/etc/sysconfig/kafka` (KAFKA_OPTS) | tos-agent javaagent присутствует ⇔ `tosAgent=true` |
| `kafka.broker.properties` | `/opt/kafka/config/broker.properties` | параметры из modify request (num.io.threads, compression.type, num.network.threads) |
| `kafka.controller.properties` | `/opt/kafka/config/controller.properties` | параметры из controllerConfig.config |
| `kafka.cruisecontrol.properties` | `/opt/cruise-control/config/cruisecontrol.properties` | auto.rebalance, replication.throttle, bootstrap.servers |
| `kafka.cruisecontrol.capacity.json` | `/opt/cruise-control/config/capacity.json` | disk/nw значения |
| `kafka.cruisecontrol.sysconfig` | `/etc/sysconfig/cruise-control` | heap size для cruise |
| `kafka.log4j.properties` | `/opt/kafka/config/log4j.properties` | appender config |
| `kafka.tools.log4j.properties` | `/opt/kafka/config/tools-log4j.properties` | tools appender |
| `kafka.soc.audit.enabled` | `/opt/kafka/config/log4j.properties` (блок `socKafkaAppender`, рендерится из шаблона `kafka-log-config` в backstage) | `enabled=true` ⇔ блок `socKafkaAppender` присутствует; `enabled=false`/`<NOT_SET>` ⇔ блока нет |

## Шаг 4: сравнить

Если PMS-API показывает `KAFKA_HEAP_OPTS="-Xms2048m -Xmx2048m"`, а в файле на хосте
`KAFKA_HEAP_OPTS="-Xms1027m -Xmx1027m"` — значит PMS обновлён, но **не отрендерился**
на хост. Это либо broker не перезапущен, либо render-таска не отработала, либо хост
в другом DC и PMS-распространение ещё не дошло.

Если PMS-API `<NOT_SET>` а на хосте файл есть — кто-то положил файл вручную, или
PMS-переменная была удалена, но файл остался.

## Важно

- **`mcc scp` destination — всегда директория** (существующая, `mkdir -p`), не путь к файлу.
  Файл-путь → `failed to open destination directory ...: no such file or directory`.
- **`-n infra`** нужен не всегда — добавляй если mcc падает с `NamespaceMissingException`.
- **Для команд на хосте** используй `mcc ssh` + `expect` (см. kafka-cluster-inspector), а не
  `mcc sshexec` (таймаутит к cloud-ops). Но конфиги достаточно читать через `mcc scp`.
- **Файлы без расширения** (`/etc/sysconfig/kafka`, `/etc/sysconfig/cruise-control`,
  `jaas.conf`) иногда падают с `failed to read downloaded archive header: EOF` при
  одиночном scp. Решение — качать всю директорию `/opt/kafka/config/` (или
  `/opt/cruise-control/config/`) разом через trailing `/`. Для sysconfig путь
  принципиально `/etc/sysconfig/kafka`, не `/opt/kafka/config/sysconfig`.
- Хосты в `host_state` — это **прод-FQDN**, локально не резолвятся. Доступ только
  через `mcc`.
- **Не модифицируй** файлы на хостах — только читаешь через `mcc scp`.
- **Не пиши в PMS** — PMS-API только читаем. Меняется только modify-флоу mdb-processing.
- Для `mcc scp` директорий — trailing `/` в source (`$HOST:/opt/kafka/config/`).

## Пример: инспекция после modify broker heap

```bash
# 1. Хосты кластера 7569c837 (test-resize) — все broker-хосты (1.broker, 2.broker, 3.broker, …)
docker exec pg_backstage_plugin_mdb psql -U dev -d backstage_plugin_mdb -tA -c \
  "SELECT host FROM host_state WHERE cluster_id='7569c837-37ba-4041-9046-92329683237e' AND host LIKE '%.broker.%';"

# 2. PMS-API: что записано в kafka.sysconfig
~/.claude/skills/kafka-config-inspector/bin/pms-read.sh 1.broker.test-resize-mdbdev-kafka.dc.one-infra.ru kafka.sysconfig | grep KAFKA_HEAP_OPTS

# 3. Скачать /opt/kafka/config/ целиком + /etc/sysconfig/kafka отдельно (dest — директория!)
HOST=1.broker.test-resize-mdbdev-kafka.dc.one-infra.ru
mkdir -p /tmp/kafka-inspect/$HOST/sysconfig
mcc --local scp "$HOST:/opt/kafka/config/"   /tmp/kafka-inspect/$HOST/          2>&1 | tail -3
mcc --local scp "$HOST:/etc/sysconfig/kafka" /tmp/kafka-inspect/$HOST/sysconfig/ 2>&1 | tail -3

# 4. Что физически на хосте
grep KAFKA_HEAP_OPTS /tmp/kafka-inspect/$HOST/sysconfig/kafka
grep -E "num.io.threads|compression.type" /tmp/kafka-inspect/$HOST/opt/kafka/config/broker.properties 2>/dev/null \
  || grep -E "num.io.threads|compression.type" /tmp/kafka-inspect/$HOST/broker.properties

# 5. Сравнить
# PMS-API:           KAFKA_HEAP_OPTS="-Xms2048m -Xmx2048m"
# Файл на хосте:      KAFKA_HEAP_OPTS="-Xms1024m -Xmx1024m"
# Вывод: PMS обновлён, файл не отрендерен — broker не перезапущен после modify.
```

## История

Каждый инспектируемый кластер сохраняй в `history/<cluster>-<date>.md` с:
- cluster_id + список хостов
- PMS-API snapshot (ключевые переменные)
- Файлы скачаны в `/tmp/kafka-inspect/<host>/`
- Расхождения найдены / не найдены
