---
name: kafka-host-inspector
description: Методы работы с хостами MDB Kafka (broker / controller / cruise-control) — путеводитель по путям на хосте (логи, конфиги, SSL, systemd, rscheck, host_checker, prometheus, cruise-control), специфика выполнения команд на Kafka-хосте. Список хостов задаёт пользователь (формат 1.broker.<cluster>.<dc>.one-infra.ru / 1.controller.<cluster>.<dc>.one-infra.ru / 1.cruise.<cluster>.<dc>.one-infra.ru). Используй когда нужно выполнить команду на Kafka-хосте, скачать/залить файл, найти где лежит конфиг или лог.
allowed-tools: [Bash, Read, Write, Edit, Grep, Glob]
---

# Скилл работы с хостами MDB Kafka

Скилл-помощник для подключения и выполнения команд на хостах Kafka-кластера под управлением
mdb-data. Не содержит диагностики — только методы работы с хостами и путеводитель по путям.

> Работа с хостами (ssh + expect, scp, sshexec) — через скилл
> [`mcc-host-access`](../mcc-host-access/SKILL.md). Ниже — только специфика Kafka.

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

- Для выполнения команд на хосте — скилл [`mcc-host-access`](../mcc-host-access/SKILL.md)
  (команда `ssh` + expect, см. `commands/ssh.md`).
- Для `sudo -u kafka` с env-переменными — `source` из `/etc/sysconfig/kafka`
  (см. `commands/run_commands.md`).
- Для скачивания конфигов — скачать директорию целиком (`/opt/kafka/config/`),
  не отдельными файлами — обходит проблему с файлами без расширения
  (`jaas.conf`, `sysconfig`). Подробнее — см. [`mcc-host-access`](../mcc-host-access/SKILL.md)
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

## Структура скилла

- `SKILL.md` — этот файл.
- `commands/run_commands.md` — Kafka-специфичные шаблоны команд на хосте
  (env через `/etc/sysconfig/kafka`, проверка после deploy, проверка share-group-lag-exporter).
