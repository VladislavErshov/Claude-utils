---
name: mcc-host-access
description: Работа с mcc (внутренний CLI доступа к хостам MDB) — mcc ssh + expect-обёртки (промпт `/# `), mcc scp (копирование файлов с граблями dest-директории), mcc sshexec (неинтерактивный запуск, требует `-n infra`), mcc instances/status/log-streams/logs (интроспекция без ssh), mcc tp-port-forward (проброс порта на localhost через Teleport), JVM-диагностика (jstack/jmap/profile/perf), lifecycle (start/stop/restart), mcc ops (проверка one-cloud-ops), известные грабли (SSL Handshake is not finished, NamespaceMissingException, Tcl/expect `[...]`/`$VAR`, EOF на tar header, ANSI-коды, self-update без --local). Список хостов задаёт пользователь. Используй когда нужно подключиться к хосту, выполнить команду, скачать/залить файл, перечислить хосты кластера, проверить статус, пробросить порт, снять thread/heap dump, запустить команду на нескольких хостах — особенно в контексте Kafka/Redis/ClickHouse/PostgreSQL/mdb-data-хостов. Все остальные инспекторы ссылаются сюда для базовых паттернов mcc.
allowed-tools: [Bash, Read, Write, Edit, Grep, Glob]
---

# Скилл работы с mcc

mcc (`/Users/vl.ershov/Documents/mcc/mcc`, есть в PATH) — внутренний CLI для доступа к
хостам MDB под управлением mdb-data. Скилл собирает канонические паттерны и известные
грабли, вытащенные из всех инспекторов (kafka/redis/clickhouse/postgres/mdb-data).

## Канон (читай перед использованием mcc)

1. **Всегда `mcc --local`** (`-l`). Без него mcc пытается self-update на каждый вызов —
   медленно, мусор в выводе (`Self-update failed, try to use --local flag...`).
2. **`mcc ssh` интерактивный** — аргументы команды не принимает (`too many positional
   arguments`), stdin не работает (`failed to get terminal size`). Неинтерактивно — только
   через `expect` после промпта `/# `. См. [commands/ssh.md](commands/ssh.md).
3. **`mcc sshexec <host> "<cmd>"`** — неинтерактивная альтернатива. **Требует `-n infra`**
   (без namespace → `NamespaceMissingException`, даже на dev-кластерах). **Не использовать к
   cloud-ops узлам** (TLS handshake timeout). См. [commands/sshexec.md](commands/sshexec.md).
4. **`mcc scp`**: dest — всегда директория (существующая, `mkdir -p`), source с trailing
   `/` для директорий, файлы без расширения качать как директорию целиком. См.
   [commands/scp.md](commands/scp.md).
5. **`-n infra`** добавлять при `NamespaceMissingException`.
6. **`SSL Handshake is not finished` / `Too early`** — туннель к minion не успел
   подняться. Повторить через 2-5 сек, `sleep 2-3` между заходами.
7. **expect + Tcl**: `[...]` → escape/переписать, `$VAR` → `\$VAR`, сложные команды —
   heredoc в файл, python с list comprehensions — base64. См.
   [commands/pitfalls.md](commands/pitfalls.md).
8. **ANSI-коды ломают grep** → `sed -E 's/\x1b\[[0-9;]*[mK]//g'`.
9. **FQDN, не localhost** для bootstrap-server Kafka/CH/Postgres (SAN сертификата).
10. **`mcc --local -n <ns> -c <dc> ops <cluster-id>`** — проверка one-cloud-ops. См.
    [commands/ops.md](commands/ops.md).
11. **Интроспекция без ssh**: `instances` (список хостов по паттерну — замена хардкод-циклам),
    `status` (RUNNING/STOPPED/availability), `log-streams`/`logs` (логи контейнера). Все с
    `-n infra`. См. [commands/query.md](commands/query.md).
12. **Проброс порта на localhost** возможен через `mcc tp-port-forward <host>:<port>`
    (Teleport, нужен доступ к tp.odkl.io). См. [commands/portforward.md](commands/portforward.md).

## FQDN-шаблоны хостов

| Сервис | Формат |
|---|---|
| Kafka broker | `1.broker.<cluster>.<dc>.one-infra.ru` |
| Kafka controller | `1.controller.<cluster>.<dc>.one-infra.ru` |
| Kafka cruise-control | `1.cruise.<cluster>.<dc>.one-infra.ru` |
| PostgreSQL | `1.db.<cluster>.<dc>.one-infra.ru` |
| ClickHouse shard | `1.shard{N}-db.<cluster>.<dc>.one-infra.ru` |
| ClickHouse Keeper | `1.keeper.<cluster>.<dc>.one-infra.ru` |
| Redis Sentinel | `1.db.<cluster>-cfs-redis.<dc>.one-infra.ru` |
| Redis Cluster | `1.shard{N}-db.<cluster>.<dc>.one-infra.ru` |
| mdb-data | `1.mdb-data.mdb-data.{hc,pc,uc,kc}.one-infra.ru` |
| mdb-processing | `1.mdb-processing.java.{hc,pc,uc,kc}.one-infra.ru` |

ДЦ: `hc`, `pc`, `uc`, `kc`, `ec`, `dc`, `rc`, `ic`, `nc`, `zc`, `sc`, ...

## Команды

- [commands/ssh.md](commands/ssh.md) — expect-обёртки, промпт `/# `, multi-command, heredoc-трюк.
- [commands/scp.md](commands/scp.md) — копирование файлов, грабли с dest-директорией.
- [commands/sshexec.md](commands/sshexec.md) — неинтерактивный запуск, перебор хостов × ДЦ.
- [commands/ops.md](commands/ops.md) — проверка one-cloud-ops.
- [commands/query.md](commands/query.md) — интроспекция без ssh: `instances` (список хостов), `status`, `log-streams`/`logs`.
- [commands/portforward.md](commands/portforward.md) — `tp-port-forward` (проброс порта), `tp-create-ssh-node`, JVM-диагностика (`jstack`/`jmap`/`profile`/`perf`), lifecycle (`start`/`stop`/`restart`).
- [commands/pitfalls.md](commands/pitfalls.md) — Tcl/expect грабли, ANSI, self-update, EOF на tar header, SSL Handshake.

## Конфигурация mcc

- Бинарник: `/Users/vl.ershov/Documents/mcc/mcc` (в PATH).
- Конфиг и mTLS-сертификаты для PMS-API: `~/.mccloud/` (`config.yaml`, `client.cert`,
  `client.key`, `ca.crt`). Напрямую через curl+mTLS к `https://pms.cloud.vk.team/api/conf/values.do`
  ходит `kafka-config-inspector/bin/pms-read.sh` — это НЕ через mcc.
- С бекстейджа/локальной машины DNS `cdb.cloud-ops.clouds.vkcl.ru` может не резолвиться
  для namespace `vkontakte` → `dial tcp: i/o timeout`. Если нужен этот namespace —
  запускать mcc с хоста, у которого есть доступ.

## Что НЕ покрывает скилл

- PMS-API (через mTLS curl, не mcc) — см. `kafka-config-inspector/bin/pms-read.sh`.
- Проброс SSH-агента через mcc — интерфейсом не предусмотрено. Проброс TCP-порта на
  localhost — теперь есть (`mcc tp-port-forward`, Teleport). Для сервисов с TLS помни про
  SAN сертификата (`localhost` может не совпасть) — см. [commands/portforward.md](commands/portforward.md).
- Диагностику конкретного сервиса (Kafka/Redis/CH/Postgres) — только mec-метод доступа.
  См. специализированные инспекторы.
