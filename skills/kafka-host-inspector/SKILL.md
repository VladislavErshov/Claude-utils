---
name: kafka-host-inspector
description: Методы работы с хостами MDB Kafka (broker / controller / cruise-control) — путеводитель по путям на хосте (логи, конфиги, SSL, systemd, rscheck, host_checker, prometheus, cruise-control), специфика выполнения команд на Kafka-хосте. Список хостов задаёт пользователь (формат 1.broker.<cluster>.<dc>.one-infra.ru / 1.controller.<cluster>.<dc>.one-infra.ru / 1.cruise.<cluster>.<dc>.one-infra.ru). Используй когда нужно выполнить команду на Kafka-хосте, скачать/залить файл, найти где лежит конфиг или лог.
allowed-tools: [Bash, Read, Write, Edit, Grep, Glob]
---

# Скилл работы с хостами MDB Kafka

Скилл-помощник для подключения и выполнения команд на хостах Kafka-кластера под управлением
mdb-data. Не содержит диагностики — только методы работы с хостами и путеводитель по путям.

> Работа с хостами (ssh + expect, scp, sshexec) — через скилл
> [`mcc-host-worker`](../mcc-host-worker/SKILL.md). Ниже — только специфика Kafka.

Диагностику кластера (логи, KRaft quorum, известные проблемы) — см. `kafka-cluster-inspector`.
Метрики и Jolokia MBean'ы — см. `kafka-metrics-investigator`.

## Формат хостов

```
{1,2,3,...}.broker.<cluster>.<dc>.one-infra.ru     — Kafka broker
{1,2,3,...}.controller.<cluster>.<dc>.one-infra.ru — KRaft controller
1.cruise.<cluster>.<dc>.one-infra.ru               — Cruise Control (один на кластер, может не быть)
```

ДЦ — любые (`hc`, `pc`, `uc`, `kc`, `ec`, `dc`, `rc`, ...). Формат хоста не зависит от ДЦ.

Пользователь даёт список хостов. Скилл не угадывает хосты — только работает с тем, что дал юзер.

## Специфика Kafka-хостов

- Для выполнения команд на хосте — скилл [`mcc-host-worker`](../mcc-host-worker/SKILL.md)
  (команда `ssh` + expect, см. `commands/ssh.md`).
- Kafka-скрипты НЕ в PATH при `mcc sshexec` (`which kafka-topics.sh` пусто) — запускать
  с полным путём `/opt/kafka/bin/`. Для CLI-инструментов auth-конфиг —
  `/opt/kafka/config/client.properties` (не `/etc/kafka/kafka-console-consumer.properties`,
  которого на хосте нет):
  ```bash
  mcc --local -n infra sshexec <host> \
    "/opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --command-config /opt/kafka/config/client.properties --describe --under-replicated-partitions"
  ```
- Для `sudo -u kafka` с env-переменными — `source` из `/etc/sysconfig/kafka`
  (см. `commands/run_commands.md`).
- Для скачивания конфигов — скачать директорию целиком (`/opt/kafka/config/`),
  не отдельными файлами — обходит проблему с файлами без расширения
  (`jaas.conf`, `sysconfig`). Подробнее — см. [`mcc-host-worker`](../mcc-host-worker/SKILL.md)
  (команда `scp`, `commands/scp.md`).
- При загрузке скриптов-чекеров на хост — путь назначения **директория**
  (см. `commands/run_commands.md`).

## Хосты и пути

| Что | Путь на хосте |
|---|---|
| Логи сервисов | `/mnt/logs/dbms/` (kafka-broker/controller/exporter, cruise-control) |
| Конфиги Kafka | `/opt/kafka/config/` (server.properties, client.properties) |
| SSL | `/opt/kafka/ssl/` (server.keystore.jks, server.truststore.jks) |
| Systemd | `/etc/systemd/system/kafka-*.service`, `cruise-control.service` |
| rscheck | `/etc/rscheck/` (kafka.conf.j2, modules/checkkafka.py) |
| host_checker | `/etc/host_checker/` (checks/check_kafka.py) |
| Prometheus JMX | `/opt/prometheus/` (kafka-2_0_0.yml, cruise-control.yml) |
| Cruise Control | `/opt/cruise-control/` (config/, libs/, dependant-libs/) |

## Грабли: Porto-контейнер, nproc и CPU-метрики

Kafka-хосты под mdb-data запущены **внутри Porto-контейнера** (контейнерная система VK), поверх
которого работает libpod/podman + systemd. Это влияет на интерпретацию системных метрик.

### `nproc` показывает хостовые ядра, не контейнерные

`nproc` и `/proc/cpuinfo` возвращают **физические ядра хоста** (например, 64), а не квоту
контейнера (часто 4 ядра). mdb-data ограничивает CPU через Porto, не через `cpu.cfs_quota_us`
на cgroup systemd — поэтому:

- `systemctl show kafka-broker.service -p CPUQuotaPerSecUSec` → `infinity` (не означает, что
  квоты нет — квота выставлена на уровне Porto, наружу не видна).
- `nproc` бесполезен для оценки «сколько ядер реально доступно брокеру».
- `loadavg` выше `nproc` — норма, если `nproc` показывает хостовые ядра. Сравнивать `loadavg`
  надо с **Porto-квотой** (узнавать у пользователя или через mdb-data API), а не с `nproc`.

**Как оценить реальную квоту CPU:**
- По mdb-data API / UI mdb-data (параметр `resources.cores` или аналог) — это канон.
- CPU% в UI mdb-data уже посчитан относительно Porto-квоты — им можно верить.
- Если очень нужно на хосте — `/proc/<kafka_pid>/cgroup` показывает Porto-путь
  `/porto/<host>/pids-prod/libpod-<id>`, но лимит CPU в Porto-конфиге, не в cgroup.

### Cgroup v1, не v2

На Kafka-хостах **cgroup v1** (hybrid). `/sys/fs/cgroup/cpu.stat` (v2 root) **пустой или
отсутствует** — не делайте вывод об отсутствии throttling по пустому файлу.

Реальный путь cgroup процесса kafka-broker:
```
/sys/fs/cgroup/cpu/porto/<host>/pids-prod/libpod-<id>/cpu.stat
/sys/fs/cgroup/cpu/porto/<host>/pids-prod/libpod-<id>/cpu.cfs_quota_us
/sys/fs/cgroup/cpu/porto/<host>/pids-prod/libpod-<id>/cpu.cfs_period_us
```

Получить путь для конкретного процесса:
```bash
PID=$(systemctl show -p MainPID --value kafka-broker.service)
cat /proc/$PID/cgroup
# строка вида "2:cpu,cpuacct:/porto/<host>/pids-prod/libpod-<id>"
# → реальный путь /sys/fs/cgroup/cpu + это значение
```

Но throttling-метрики в cgroup могут быть неинформативны, если Porto управляет CPU иначе —
для диагностики «CPU throttling» в первую очередь **смотрите метрики Kafka через Jolokia**
(скилл [`kafka-metrics-investigator`](../kafka-metrics-investigator/SKILL.md)):
`BytesInPerSec`, `BytesOutPerSec`, `MessagesInPerSec`, `RequestHandlerAvgIdlePercent`,
`ProduceThrottleRate`, `FetchThrottleRate`.

### `ps %CPU` — это доля одного ядра

`ps -o pcpu` показывает **процент одного ядра** (0-100% = 0-1 ядро), не общий CPU. На 4-ядерной
Porto-квоте java-процесс с `pcpu=115%` занимает ~1.15 ядра из 4 — это ~29% квоты, не 115%.
Для оценки «сколько всего ядер жрёт брокер» складывайте `pcpu` по всем java-процессам и top-тредам
(`ps -L -p <pid> -o lwp,pcpu --sort=-pcpu`), либо смотрите CPU% в UI mdb-data.

## Структура скилла

- `SKILL.md` — этот файл.
- `commands/run_commands.md` — Kafka-специфичные шаблоны команд на хосте
  (env через `/etc/sysconfig/kafka`, проверка после deploy, проверка share-group-lag-exporter).

## Эталонный список сервисов (cruise-control хост)

На здоровом cruise-control хосте `systemctl list-units --type=service` показывает 12 active
юнитов (эталон снят с `1.cruise.test-43version-4-mdbdev-kafka.hc.one-infra.ru`):

```
confp-init.service             active exited   Run confp as oneshot service
cruise-control.service         active running  Linkeding Cruise Control
dbus.service                   active running  D-Bus System Message Bus
import-environment.service     active exited   Import environment from pid 1
network-wait-online.service    active exited   Wait for network to be configured
nginx.service                  active running  A high performance web server and a reverse proxy server
rscheck@cruisecontrol.service  active running  RSCheck cruisecontrol service
rsyslog.service                active running  System Logging Service
systemd-journald.service       active running  Journal Service
systemd-remount-fs.service     active exited   Remount Root and Kernel File Systems
systemd-tmpfiles-setup.service active exited   Create Volatile Files and Directories
vector.service                 active running  Vector service for producing logs from files to kafka
```

⚠️ **Если active сервисов сильно меньше 12 (часто 3 — `cruise-control`, `systemd-remount-fs`,
`systemd-tmpfiles-setup`) — systemd застрял на `sysinit.target` и не перешёл к `multi-user.target`.**

Маркеры проблемы:
- `ps -p 1` показывает `/usr/lib/systemd/systemd --unit=sysinit.target` (должен быть без
  `--unit=...` или с `--unit=multi-user.target`).
- `systemctl is-active multi-user.target` → `inactive`.
- `systemctl get-default` → `graphical.target` (это нормально, не причина).

Фикс:
```bash
systemctl start multi-user.target
```
После этого подтянутся `confp-init`, `nginx`, `rscheck@cruisecontrol`, `vector`, `dbus`,
`journald`, `rsyslog` и т.д. Без `rscheck@cruisecontrol.service` mdb-data считает хост dead,
даже если `cruise-control.service` running.

Эталон для broker / controller хостов — отдельный (не покрыт здесь). Если нужно — собери с
здорового хоста и добавь аналогичный блок.
