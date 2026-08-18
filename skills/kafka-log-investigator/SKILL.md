---
name: kafka-log-investigator
description: Скачивание и анализ логов MDB Kafka (broker / controller / cruise-control) — каталог что грепать в kafka-broker.out.log / kafka-controller.out.log / cruise-control.err.log, маркеры успешного старта брокера (Successfully registered broker, Kafka Server started), ошибки InvalidReplicationFactorException, <unresolved> controller hostname, CruiseControlMetricsReporter, UnsupportedClassVersionError, KRaft quorum/voters. Логи читаются через скилл `mcc-host-access` из `/mnt/logs/dbms/`. Список хостов задаёт пользователь. Используй когда нужно скачать логи с Kafka-хостов и найти в них причину проблемы (брокер не стартует, CC не запускается, регистрация в quorum не прошла, metrics reporter падает).
allowed-tools: [Bash, Read, Write, Edit, Grep, Glob]
---

# Скилл анализа логов MDB Kafka

Скилл для скачивания и разбора логов broker/controller/cruise-control хостов Kafka-кластера
под управлением mdb-data.

## Что делает скилл

- Скачивает логи сервисов с хостов через скилл [`mcc-host-access`](../mcc-host-access/SKILL.md)
  (команда `scp`, путь `/mnt/logs/dbms/`).
- Подсказывает структуру скачанных логов (какой файл за что отвечает).
- Даёт готовые `grep`-команды для типичных проблем: старт брокера, InvalidReplicationFactor,
  unresolved controller hostname, CruiseControlMetricsReporter, Java version mismatch в CC,
  KRaft quorum/voters.

## Подчинённые скиллы

- Работа с хостами (команды `scp`, `ssh + expect`) — см.
  [`mcc-host-access`](../mcc-host-access/SKILL.md). Этот скилл использует команду `scp`
  для скачивания логов из `/mnt/logs/dbms/`.
- Каталог известных проблем кластера (симптомы/причины/фиксы) — см. `kafka-cluster-inspector`,
  `commands/known_issues.md`. Этот скилл фокусируется на **поиске маркеров в логах**, а не на
  описании фиксов.
- Метрики, MBean'ы, "Broker is dead" — см. `kafka-metrics-investigator`.

## Формат хостов

```
{1,2,3,...}.broker.<cluster>.<dc>.one-infra.ru     — Kafka broker
{1,2,3,...}.controller.<cluster>.<dc>.one-infra.ru — KRaft controller
1.cruise.<cluster>.<dc>.one-infra.ru               — Cruise Control (один на кластер, может не быть)
```

Пользователь даёт список хостов. Скилл не угадывает хосты — только работает с тем, что дал юзер.

## Структура скилла

- `SKILL.md` — этот файл.
- `commands/download_logs.md` — скачивание логов со всех хостов, структура скачанных логов,
  что искать в `kafka-broker.out.log` / `kafka-controller.out.log` / `cruise-control.err.log`,
  маркеры успеха и ошибок, проверка после фикса, очистка.

## Что НЕ покрывает скилл

- Метрики / MBean'ы / exporter'ы — `kafka-metrics-investigator`.
- Конфиги Kafka / rscheck / host_checker — `kafka-config-inspector`, `kafka-cluster-inspector`.
- KRaft log corruption — нужен `kafka-dump-log.sh`.
- Live-tailing логов (journalctl/systemctl status) — см. `kafka-host-inspector`,
  `commands/run_commands.md`.
