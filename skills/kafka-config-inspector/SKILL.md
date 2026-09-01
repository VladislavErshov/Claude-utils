---
name: kafka-config-inspector
description: Инспекция конфиг-файлов Kafka-хостов — сверка PMS-переменных (pms.cloud.vk.team API) с отрендеренными конфиг-файлами на хостах (broker.properties, controller.properties, cruisecontrol.properties, capacity.json, sysconfig, jaas.conf, log4j.properties, tools-log4j.properties). Список хостов берётся из БД pg_backstage_plugin_mdb, файлы читаются через скилл mcc-host-worker. Используй когда нужно проверить, что PMS-API значения физически применились в /opt/kafka/config/ после modify-флоу. Скилл проверяет только property-файлы — он НЕ проверяет здоровье кластера (ISR, replication, partition balance и т.п.). Поддерживает два namespace: infra (one-infra.ru) и dzen (idzn.ru) — для дзена передавай `ns=dzen` в pms-read.sh.
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

> Доступ к хостам и копирование файлов — через скилл [`mcc-host-worker`](../mcc-host-worker/SKILL.md).
> Ниже — только специфика сверки PMS-API ↔ конфиг-файлы Kafka.

Список хостов берётся из локальной БД `pg_backstage_plugin_mdb` (`host_state` по
`cluster_id`). Файлы скачиваются через скилл [`mcc-host-worker`](../mcc-host-worker/SKILL.md).

## Когда применять

- После modify-флоу — убедиться что PMS-API получил значения **и** что они
  отрендерились в файлы на хостах (например, `KAFKA_HEAP_OPTS` в `/opt/kafka/config/sysconfig`
  совпадает с `kafka.sysconfig` в PMS-API).
- При разборе «почему broker игнорирует новую конфигурацию» — PMS-API может быть
  обновлён, а файлы на хосте не перерисованы (или наоборот — файлы есть, PMS пустой).
- Для инспекции любого состояния Kafka-кластера без запуска modify.

## Что нужно

- **mTLS-сертификаты** в `~/.mccloud/` (`client.cert`, `client.key`, `ca.crt`) — для
  PMS-API (скрипт [`pms-worker/bin/pms-read.sh`](../pms-worker/SKILL.md), это НЕ доступ
  к хостам — прямой curl+mTLS к `https://pms.cloud.vk.team/api/conf/values.do`).
  Общая механика PMS (namespaces, чтение/запись, грабли) — скилл [`pms-worker`](../pms-worker/SKILL.md).
- **Доступ к хостам** — через скилл [`mcc-host-worker`](../mcc-host-worker/SKILL.md).
  Грабли scp (dest-директория, `EOF на tar header` для файлов без
  расширения, `NamespaceMissingException`) — в скилле `mcc-host-worker`.
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

> Чтение/запись PMS, namespaces (`infra`/`dzen`/`vkontakte`), скрипт `pms-read.sh`,
> rate-limit и прочие грабли — скилл **[`pms-worker`](../pms-worker/SKILL.md)**.
> Скрипт переехал: `~/.claude/skills/pms-worker/bin/pms-read.sh`.
> Ниже — только Kafka-специфика.

### Namespace для Kafka-кластеров

- Общий контур (`*.one-infra.ru`) — `namespace=infra` (дефолт скрипта).
- Дзен (FQDN `<N>.<role>.<queue>.<dc>.idzn.ru`) — обязательно `namespace=dzen`,
  иначе все переменные `<NOT_SET>`. На хосте `cloud_hierarchy` в `/proc/1/environ`
  содержит `...front.db.production.mdb.prod`.
- Vkontakte (FQDN `.vkcl.ru`) — `namespace=vkontakte` (НЕ `vkcl` — HTTP 400);
  на запись ACCESS_DENIED, только чтение. Точный namespace — из БД:
  `SELECT ns.name FROM db_cluster dc JOIN namespaces ns ON ns.id = dc.namespace_id WHERE dc.id = '<cluster_id>'`.

```bash
# Дзен-кластер:
~/.claude/skills/pms-worker/bin/pms-read.sh 12.broker.events-front-kafka.dc.idzn.ru kafka.sysconfig dzen mdb
# Общий контур (infra — дефолт):
~/.claude/skills/pms-worker/bin/pms-read.sh 1.broker.test-mdbdev-kafka.dc.one-infra.ru
# Все известные Kafka-переменные (19 шт., дефолтный список скрипта):
~/.claude/skills/pms-worker/bin/pms-read.sh <host> "" infra mdb
```

### ⚠️ Грабля: PMS-ключи для controller-хостов разбиты на два

Для controller-хостов PMS-переменные **разнесены по двум PMS-ключам**:

| PMS-ключ | Какие переменные там лежат |
|---|---|
| `controller.<queue>.clouds` | **только** `kafka.sysconfig` (heap/jolokia/jmx/prometheus javaagent) |
| `<queue>.clouds` (брокерский ключ) | **все остальные** controller-настройки: `kafka.controller.properties`, `kafka.layout`, `kafka.controller.quorum`, `kafka.ssl.enabled`, `kafka.keystore.password.vault.path`, `kafka.truststore.password.vault.path`, `kafka.log4j.properties`, `kafka.tools.log4j.properties`, `kafka.users`, и т.д. |

Если дёргать `controller.<queue>.clouds` для всего списка переменных через
`pms-read.sh ... "" infra mdb` — почти все строки покажутся `<NOT_SET>`, хотя
на самом деле они лежат на брокерском ключе `<queue>.clouds`. Это не ошибка
PMS, а особенность шаблона mdb-data.

Правильный паттерн для controller-хоста:
```bash
# 1. sysconfig — с controller-ключа
~/.claude/skills/pms-worker/bin/pms-read.sh "controller.<queue>.clouds" kafka.sysconfig infra mdb
# 2. Все остальные controller-настройки — с брокерского ключа
~/.claude/skills/pms-worker/bin/pms-read.sh "<queue>.clouds" "kafka.controller.properties,kafka.controller.quorum,kafka.layout,kafka.ssl.enabled" infra mdb
```

Подтверждено на кластере `dsp-notices-msk-adtech-kafka` (2026-08-14):
`controller.dsp-notices-msk-adtech-kafka.clouds` отдаёт только `kafka.sysconfig`,
а `kafka.controller.properties` / `kafka.layout` / `kafka.controller.quorum` /
`kafka.ssl.enabled` — все лежат на `dsp-notices-msk-adtech-kafka.clouds`.

### Запись в PMS

Ручная запись (`update.do`) — **только после явного подтверждения пользователя**;
рутинный путь изменения kafka.*-переменных — modify-флоу mdb-processing.
API записи, правила верификации (байт-в-байт, rate-limit) — скилл
**[`pms-worker`](../pms-worker/SKILL.md)**, секция «Запись: update.do».

## Шаг 3: файлы на хостах

Доступ к хостам и копирование файлов — через скилл [`mcc-host-worker`](../mcc-host-worker/SKILL.md).
Здесь — только Kafka-специфика путей.

### Скачать конфиги broker/controller (файлы в `/opt/kafka/config/` + `/etc/sysconfig/kafka`)

⚠️ `sysconfig` рендерится в **`/etc/sysconfig/kafka`**, НЕ в `/opt/kafka/config/sysconfig`
(см. `docker-images/ubuntu20-kafka-base/rootfs/etc/confp/resources.d/kafka.yml:38-39`).
Качать отдельно, потому что вне `/opt/kafka/config/`.

Скачать `/opt/kafka/config/` целиком (одним tarball — обходит баг с файлами без расширения) и
`/etc/sysconfig/kafka` отдельно (destination — директория, не файл!) — через скилл
[`mcc-host-worker`](../mcc-host-worker/SKILL.md) (команда `scp`, namespace `infra`).

Пример (для хоста `1.broker.test-resize-mdbdev-kafka.dc.one-infra.ru`):
- `mkdir -p /tmp/kafka-inspect/$HOST/sysconfig`
- `$HOST:/opt/kafka/config/` → `/tmp/kafka-inspect/$HOST/`
- `$HOST:/etc/sysconfig/kafka` → `/tmp/kafka-inspect/$HOST/sysconfig/`

### Скачать конфиги cruise-control (файлы в `/opt/cruise-control/config/` + `/etc/sysconfig/cruise-control`)

⚠️ Cruise-control файлы лежат в **`/opt/cruise-control/config/`**, НЕ в `/opt/kafka/config/`
(см. `docker-images/ubuntu20-mdb-cruisecontrol/rootfs/etc/confp/resources.d/cruise-control.yml`).
`cruisecontrol-sysconfig` рендерится в **`/etc/sysconfig/cruise-control`**.

Скачать `/opt/cruise-control/config/` и `/etc/sysconfig/cruise-control` — через скилл
[`mcc-host-worker`](../mcc-host-worker/SKILL.md) (команда `scp`, namespace `infra`).

⚠️ Одиночный `scp` файла без расширения (`sysconfig`, `jaas.conf`) падает с
`failed to read downloaded archive header: EOF` — баг. Качать всю директорию целиком.
Подробнее — скилл [`mcc-host-worker`](../mcc-host-worker/SKILL.md).
Для `sysconfig` принципиально качать с `/etc/sysconfig/kafka`, не из `/opt/kafka/config/`.

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

- Доступ к хостам и грабли scp (dest-директория, `EOF на tar header` для файлов без расширения,
  `NamespaceMissingException` → `-n infra`, trailing `/` для директорий) —
  в скилле [`mcc-host-worker`](../mcc-host-worker/SKILL.md).
- Хосты в `host_state` — это **прод-FQDN**, локально не резолвятся. Доступ только
  через скилл [`mcc-host-worker`](../mcc-host-worker/SKILL.md).
- **Не модифицируй** файлы на хостах — только читаешь (через скилл mcc-host-worker).
- **Не пиши в PMS без разрешения** — запись через `update.do` возможна (см. секцию
  «Запись в PMS» выше), но каждый раз сначала спрашивай пользователя: что, куда,
  какое значение. PMS-API по умолчанию — только читаем.

## Пример: инспекция после modify broker heap

```bash
# 1. Хосты кластера 7569c837 (test-resize) — все broker-хосты (1.broker, 2.broker, 3.broker, …)
docker exec pg_backstage_plugin_mdb psql -U dev -d backstage_plugin_mdb -tA -c \
  "SELECT host FROM host_state WHERE cluster_id='7569c837-37ba-4041-9046-92329683237e' AND host LIKE '%.broker.%';"

# 2. PMS-API: что записано в kafka.sysconfig
~/.claude/skills/pms-worker/bin/pms-read.sh 1.broker.test-resize-mdbdev-kafka.dc.one-infra.ru kafka.sysconfig | grep KAFKA_HEAP_OPTS

# 3. Скачать /opt/kafka/config/ целиком + /etc/sysconfig/kafka отдельно (dest — директория!)
#    через скилл mcc-host-worker (команда scp, namespace infra).
HOST=1.broker.test-resize-mdbdev-kafka.dc.one-infra.ru
mkdir -p /tmp/kafka-inspect/$HOST/sysconfig
# scp "$HOST:/opt/kafka/config/"   → /tmp/kafka-inspect/$HOST/
# scp "$HOST:/etc/sysconfig/kafka" → /tmp/kafka-inspect/$HOST/sysconfig/

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
