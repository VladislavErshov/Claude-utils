---
name: kafka-docker-developer
description: Разработка docker-образа MDB Kafka (ubuntu20-kafka-base, ubuntu20-kafka-3.8.0) в репозитории docker-images — правка Python-чекеров (check_kafka.py, rscheck@kafka), confp jinja2-шаблонов (broker/controller.properties, jaas.conf, sysconfig), systemd-юнитов (kafka-broker/controller/exporter.service), build.d-скриптов и dockerfile, горячая заливка файлов на тестовый кластер через mcc scp без пересборки образа, сборка/деплой образа через CI и верификация через rscheck@kafka / UI mdb-data. TRIGGER — «поправить чекер / роль хоста Kafka», «hot-reload check_kafka.py на кластер», «изменить конфиг/сервис/сборку образа Kafka», «пересобрать/задеплоить образ ubuntu20-kafka», «разобраться в цикле разработки docker-images для Kafka». SKIP — диагностика живого кластера (broker dead, KRaft quorum, MBean) → /kafka-cluster-inspector; сверка PMS-переменных с отрендеренными конфигами → /kafka-config-inspector; топики/ACL/quotas → mdb-data API; графики/метрики → /grafana-plot-creator.
allowed-tools: [Bash, Read, Write, Edit, Grep, Glob]
---

# Скилл разработки docker-образа MDB Kafka

Скилл для разработки образов `ubuntu20-kafka-base` и `ubuntu20-kafka-3.8.0` в репозитории
`docker-images`. Покрывает два цикла:

1. **Hot-reload** — правка Python-чекеров и заливка на хост тестового кластера через `mcc scp`
   без пересборки образа. Для быстрой проверки гипотез.
2. **Full rebuild** — правка systemd-юнитов, confp-шаблонов, build.d-скриптов с пересборкой
   образа и деплоем через CI/mdb modify.

⚠️ Скилл про **разработку и деплой файлов образа**. Для диагностики уже запущенного Kafka-кластера
используй `/kafka-cluster-inspector`. Для сверки PMS-переменных с отрендеренными конфигами —
`/kafka-config-inspector`.

## Документация

- https://docs.vk.team/mdb/docs/kafka/kafka-intro.html — введение
- https://docs.vk.team/mdb/docs/kafka/kafka.html — детали

## Структура образов

### `ubuntu20-kafka-base/` — базовый образ (конфиги, сервисы, чекеры)

```
docker-images/ubuntu20-kafka-base/
├── ubuntu20-kafka-base-onecloud.dockerfile     ← FROM ubuntu20-mdb-base, COPY rootfs, RUN docker/build
└── rootfs/
    ├── docker/
    │   └── build.d/                            ← shell-хуки сборки (вызываются из docker/build)
    │       ├── 10-install-java.sh
    │       ├── 21-jars.sh                      ← kafka-exporter jars
    │       ├── 22-exporter.sh
    │       ├── 30-users.sh                     ← пользователь kafka
    │       ├── 40-kafka-rscheck.sh             ← systemctl enable rscheck@kafka
    │       └── 99-clean.sh
    ├── etc/
    │   ├── host_checker/
    │   │   ├── checks/check_kafka.py           ← основной Python-чекер роли Kafka-узла
    │   │   └── host_checker_config.ini.j2
    │   ├── rscheck/
    │   │   ├── kafka.conf.j2                   ← конфиг rscheck@kafka (CheckKafka, порт 81)
    │   │   └── modules/                        ← Python-модули rscheck (backstage_client и т.д.)
    │   ├── confp/
    │   │   ├── templates.d/                    ← jinja2-шаблоны, рендерятся в /opt/kafka/config/
    │   │   │   ├── client.properties.j2
    │   │   │   ├── jaas.conf.j2
    │   │   │   ├── create_keystore.sh.j2
    │   │   │   ├── pre-start-kafka-broker.sh.j2
    │   │   │   ├── pre-start-kafka-controller.sh.j2
    │   │   │   ├── pre-start-kafka-exporter.sh.j2
    │   │   │   ├── vector-default.toml.j2
    │   │   │   ├── .pass.j2
    │   │   │   └── .ssl_enabled.j2
    │   │   └── resources.d/
    │   │       └── kafka.yml                   ← confp resource: пути рендера, права, source
    │   ├── systemd/system/
    │   │   ├── kafka-broker.service
    │   │   ├── kafka-controller.service
    │   │   ├── kafka-exporter.service
    │   │   └── logrotate.timer
    │   ├── kafka/
    │   │   ├── get-user-info.sh
    │   │   └── kafka-broker.yml                ← Prometheus JMX exporter config
    │   ├── logrotate.d/
    │   ├── profile.d/
    │   └── tmpfiles.d/
    ├── opt/prometheus/                         ← kafka-2_0_0.yml, cruise-control.yml
    └── usr/changelog/changelog.md              ← история изменений образа
```

### `ubuntu20-kafka-3.8.0/` — версионный образ (ставит конкретную Kafka)

```
docker-images/ubuntu20-kafka-3.8.0/
├── ubuntu20-kafka-3.8.0-onecloud.dockerfile    ← ARG BASE_IMAGE=ubuntu20-kafka-base, ENV KAFKA_VERSION=3.8.0
└── rootfs/
    ├── docker/build.d/
    │   ├── 20-packages.sh
    │   ├── 31-kafka-packages.sh                ← ставит Kafka из oneart
    │   ├── 50-run-kafka.sh                     ← mkdir /opt/kafka/{ssl,scripts}, enable services
    │   └── 99-clean.sh
    └── usr/changelog/versions.md
```

## Кто что запускает

| Сервис | Что использует | Период |
|---|---|---|
| `rscheck@kafka.service` | `/etc/rscheck/kafka.conf` → `/etc/host_checker/checks/*.py` | регулярно шлёт host info в backstage |
| `host-check.service` | тот же `/etc/host_checker/checks/` | реже, не на всех хостах |

На брокерах `host-check.service` обычно `inactive dead` — основной канал `rscheck@kafka`. На
контроллерах работают оба.

## Циклы разработки

### Hot-reload (без пересборки образа)

Подходит для Python-чекеров (`check_kafka.py`) и других текстовых файлов, которые можно
перезалить без переустановки пакета. См. `commands/checker_hot_deploy.md`.

1. Правишь `check_kafka.py` локально в `docker-images`.
2. Заливаешь файл на хост(ы) тестового кластера через `mcc scp` (dest = **директория**, не файл).
3. Перезапускаешь `rscheck@kafka.service`.
4. Проверяешь, что в UI mdb-data роль/статус обновились.
5. Если гипотеза подтвердилась — коммитишь в `docker-images`, идёт штатный деплой через CI.

### Full rebuild (с пересборкой образа)

Для systemd-юнитов, confp-шаблонов, build.d-скриптов, prometheus-конфигов. См. заглушки
`commands/confp_templates.md`, `commands/systemd_units.md`, `commands/build_image.md`,
`commands/deploy_image.md` — заполняются по мере использования.

## mcc scp / mcc ssh — особенности

Подробности — в `/kafka-cluster-inspector/commands/run_commands.md`. Кратко:

- **`mcc --local`** (`-l`) — обязательно. Без него mcc тянет свежую версию с мастера на каждый
  вызов (медленно + мусор в выводе).
- **`mcc scp local <host>:/dir/`** — dest **всегда директория**. Если указать путь с именем
  файла, mcc создаст на хосте директорию с этим именем и положит файл внутрь.
- **`mcc ssh <host>`** — интерактивный, не принимает command как аргумент. Команды — через
  `expect` (шаблон в `commands/checker_hot_deploy.md` и в
  `/kafka-cluster-inspector/commands/run_commands.md`).
- **`SSL Handshake is not finished`** — повторить через 1-2 сек, tunnel ещё поднимается.
- **NamespaceMissingException** на scp — добавить `-n infra`.

## Типичные хосты

```
1.broker.test-<cluster>-mdbdev-kafka.dc.one-infra.ru       — Kafka broker
1.controller.test-<cluster>-mdbdev-kafka.uc.one-infra.ru   — KRaft controller (leader)
1.controller.test-<cluster>-mdbdev-kafka.ic.one-infra.ru   — KRaft controller (follower)
1.cruise.<cluster>-<dc>.one-infra.ru                       — Cruise Control
```

## Структура скилла

- `SKILL.md` — этот файл.
- `commands/checker_hot_deploy.md` — правка check_kafka.py + заливка + рестарт + верификация.
- `commands/confp_templates.md` — TODO: правка j2-шаблонов и `confp/resources.d/kafka.yml`.
- `commands/systemd_units.md` — TODO: правка `kafka-*.service`, `logrotate.timer`.
- `commands/build_image.md` — TODO: сборка образа (oneart, dockerfile, build.d).
- `commands/deploy_image.md` — TODO: деплой через CI / mdb modify.
- `history/` — логи удачных и неудачных подходов.

## Что НЕ покрывает скилл

- Диагностику Kafka-кластера (broker dead, KRaft quorum, MBean'ы) — `/kafka-cluster-inspector`.
- Сверку PMS-переменных с отрендеренными конфигами — `/kafka-config-inspector`.
- Throughput / latency / performance — к Prometheus/Grafana.
- Настройку топиков / ACL / quotas — к mdb-data API.

## Связанные скиллы

- `/kafka-cluster-inspector` — выполнение команд на хосте через `expect`, детали `mcc ssh`/`scp`,
  чтение логов, Jolokia.
- `/kafka-config-inspector` — сверка PMS-переменных с конфигами на хостах.
