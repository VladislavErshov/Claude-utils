---
name: clickhouse-cluster-inspector
description: Инспекция MDB ClickHouse кластеров (версии 24.x) — архитектура shard+replica+Keeper, разбор Raft quorum / Keeper leader election, каталог известных проблем (broken parts, Keeper split-brain, нет кворума, "Keeper server rejected the connection during the handshake", TOO_MANY_SIMULTANEOUS_QUERIES, Part intersects previous part, сломалась схема на реплике). Список хостов задаёт пользователь (формат 1.shard{N}-db.<cluster>.<dc>.one-infra.ru / 1.keeper.<cluster>.<dc>.one-infra.ru). Используй когда нужно понять состояние кластера, найти причину почему CH-хост или Keeper не стартует / не входит в Raft quorum, разобраться с репликацией / broken parts / зависшими запросами. Работа с хостами — через скилл `mcc-host-worker` (`mcc ssh` + expect), SQL через `clickhouse-client --user backup-admin` (пароль в `/etc/rscheck/checkclickhouse.conf`).
allowed-tools: [Bash, Read, Write, Edit, Grep, Glob]
---

# Скилл инспекции MDB ClickHouse кластеров

Скилл-каталог для разбора состояния ClickHouse-кластеров под управлением mdb-data.
Содержит архитектуру кластера, формат хостов, путеводитель по путям/логам и каталог
известных проблем. Конкретные запросы и рецепты — в `commands/`.

⚠️ Скилл описывает **состояние процессов ClickHouse + Keeper** на уровне кластера
(запуск, Raft quorum, rscheck, репликация, broken parts) и каталог известных проблем.

> Работа с хостами (ssh + expect, scp, sshexec) — через скилл
> [`mcc-host-worker`](../mcc-host-worker/SKILL.md). Ниже — только специфика ClickHouse.

## Документация

- **Дежурная инструкция (SSOT)**: [Дежурство MDB: Clickhouse](https://confluence.vk.team/pages/viewpage.action?pageId=1348619034)
  — известные проблемы (broken/detached parts, реплика долго переналивается, сброс пароля)
  и административные задачи (настройки, макросы, словари, named_collections, обновление,
  перенос кипера) с полными шаблонами. Вики живая; в скилле копии не храним —
  `commands/` содержат ссылки + только наши дополнения (Raft-диагностика Keeper,
  mcc-обёртки, разборы инцидентов).
- https://docs.vk.team/mdb/docs/clickhouse/ch-intro.html — введение
- https://docs.vk.team/mdb/docs/clickhouse/ch-administration.html — администрирование
- https://docs.vk.team/mdb/docs/clickhouse/ch-integrations.html — интеграции (Kafka и др.)

Доки лежат в соседнем репо `mdb-docs`.

## Архитектура кластера

- **Shard + replica** — шардированный MASTER-MASTER. На каждом шарде по несколько реплик
  (обычно по одной на ДЦ). `ReplicatedMergeTree` реплицируется через Keeper.
- **Keeper** — отдельные хосты (не встроенный keeper в CH). Обычно 3 ноды в Raft-кворуме,
  по одной на ДЦ (`hc`, `pc`, `kc`, ...). Кворум 2/3.
- **ДЦ** — любые (`hc`, `pc`, `uc`, `kc`, `ec`, `dc`, `rc`, ...).
- **Keeper хранит** metadata репликации (`/clickhouse/paths/...`), распределённые DDL-очереди,
  координацию мержей/мутаций.
- **CH-реплика без Keeper** — может работать на чтение локальных таблиц, но Distributed-запросы
  и репликация `ReplicatedMergeTree` отваливаются. Запросы, требующие ZK, висят и копятся
  → `TOO_MANY_SIMULTANEOUS_QUERIES`.

## Формат хостов

```
{1,2,3,...}.shard{N}-db.<cluster>.<dc>.one-infra.ru  — ClickHouse server (роль MASTER в UI)
{1,2,3,...}.keeper.<cluster>.<dc>.one-infra.ru       — ClickHouse Keeper (Raft voter)
```

Пользователь даёт список хостов. Скилл не угадывает хосты — только работает с тем, что дал юзер.

## Подключение

**К CH-серверу** (пароль `backup-admin` лежит в `/etc/rscheck/checkclickhouse.conf`):
```bash
# Достать пароль неинтерактивно (НЕ используем \$() внутри expect — Tcl ломается на скобках,
# пишем скрипт на хост через heredoc — см. mcc-host-worker/commands/pitfalls.md)
cat /etc/rscheck/checkclickhouse.conf | grep -oP 'password:\s*\K[^ ]+' | head -1
clickhouse-client --user backup-admin --password '<pass>' --query 'SELECT 1'
```

**К Keeper** (локально на keeper-хосте):
```bash
clickhouse-keeper-client -h 127.0.0.1 -p 2181 -q ruok    # должно вернуть `imok`
clickhouse-keeper-client -h 127.0.0.1 -p 2181 -q mntr    # метрики Raft
clickhouse-keeper-client -h 127.0.0.1 -p 2181 -q srvr    # server info, role
```

Если `ruok` возвращает `Keeper server rejected the connection during the handshake.
Possibly it's overloaded, doesn't see leader or stale` — Raft-подсистема не активна
(нет лидера). Это **не** сетевая проблема, это **Raft state**.

## Путеводитель по путям

| Что               | Путь на хосте                                                                                                                                                                                               |
|-------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| Логи CH-сервера   | `/mnt/logs/dbms/clickhouse-server.log`, `.err.log` (старые кластеры: `/one/logs/clickhouse/`, совсем старые: `/var/log/clickhouse-server/`)                                                                 |
| Логи Keeper       | `/mnt/logs/dbms/clickhouse-keeper.log`, `.err.log`                                                                                                                                                          |
| Ротация логов     | `.log.0.gz`, `.log.1.gz`, ... (по размеру; `.err.log` живёт дольше, иногда с момента старта сервиса)                                                                                                        |
| Конфиги CH        | рендерятся из PMS через `confp --oneshot`: `zen.clickhouse.config.xml`, `zen.clickhouse.users.xml`, `zen.clickhouse.additional_config.xml`, `zen.clickhouse.macros.xml`, `zen.clickhouse-keeper.config.xml` (механика PMS — скилл [`pms-worker`](../pms-worker/SKILL.md)) |
| Данные CH         | `/var/lib/clickhouse/1/` (диск 1), `/var/lib/clickhouse/2/` (диск 2 на гибридах)                                                                                                                            |
| Flags             | `/var/lib/clickhouse/1/flags/force_restore_data` — пропустить проверку broken parts при старте                                                                                                              |
| Detached parts    | `/var/lib/clickhouse/1/store/<hash>/<hash>/detached/`                                                                                                                                                       |
| rscheck           | `/etc/rscheck/checkclickhouse.conf` (пароль backup-admin), `/etc/rscheck/checkclickhouse-keeper.conf`                                                                                                       |
| host_checker      | `/etc/host_checker/checks/check_ch-keeper.py` (помечает keeper UNAVAILABLE если `ruok != imok`)                                                                                                             |
| Systemd           | `mdb-clickhouse-server`, `mdb-clickhouse-keeper`                                                                                                                                                            |
| Скрипты           | `/usr/scripts/reload-config.py` (релоад конфигов), `/usr/scripts/request_leadership.py` (попросить leadership у Keeper)                                                                                     |

## Порты

- CH server: `8123` (HTTP), `9000` (native)
- Keeper: `2181` (client, как ZK), `9444` (Raft inter-node)

## Структура скилла

- `SKILL.md` — этот файл.
- `commands/known_issues.md` — известные проблемы: ссылка на вики + наши разборы
  (Raft-диагностика Keeper, TOO_MANY_SIMULTANEOUS_QUERIES, mcc-переборы).
- `commands/administration.md` — административные задачи: ссылка на вики + сводка
  PMS-конфигов и mcc-обёртки.
- `commands/queries.md` — полезные SQL-запросы для диагностики (system.replicas, system.parts, system.errors, system.processes, system.zookeeper_connection).
- `history/` — разборы реальных инцидентов (формат `incident_<YYYY-MM>_<краткое_описание>.md`). История для будущих разборов — что видели, чем лечили, что сработало.
  - `incident_2026_07_keeper_split.md` — sellgate-media-infra-ch: Keeper split-brain после остановки одного кипера в облаке, зависший Raft-state на pc, фикс через дроп диска + рестарт.
  - `incident_2026_07_mysql_port_9004.md` — uchiru-bi-dwh (MDBSUP-3886): включение MySQL emulation port 9004. Три шага: PMS `<mysql_port>` + манифест сервиса `'9004': 'lan,tcp'` + `confp --oneshot && systemctl restart mdb-clickhouse-server` (не `RELOAD CONFIG`).
  - `incident_2026_08_mnt_logs_full.md` — uv-content-id-meta-dev-uv-ch (MDBSUP-4673): хост не поднимается, `Dependency failed for Clickhouse Server`. Корень — диск `/mnt/logs` (15G) забит на 100% файлом `clickhouse-syslog.err.log` (root-owned, не ротируется CH). `dir-init.service` не может создать `/mnt/logs/analytics/probes` → валит dependency CH-сервера. Фикс: `rm -rf /mnt/logs/{dbms,analytics,system,vector}/*` + `systemctl start mdb-clickhouse-server`.

## Известные проблемы (кратко)

Подробности — `commands/known_issues.md`.

- **Broken parts при старте** — `Code: 231. Suspiciously many (N parts, X KiB in total) broken parts to remove while maximum allowed ... is 100`. Лечение: `force_restore_data` flag + restart, либо поднять лимиты в PMS `zen.clickhouse.additional_config.xml` (`max_suspicious_broken_parts`, `max_suspicious_broken_parts_bytes`).
- **Keeper UNAVAILABLE / нет кворума** — `Keeper server rejected the connection during the handshake. Possibly it's overloaded, doesn't see leader or stale`. Один или несколько киперов не в Raft-кворуме. См. `history/incident_2026_07_keeper_split.md`.
- **`peer N is busy` в Keeper логах** — асимметрия: pc→hc "is busy", но hc→pc работает. Сеть при этом жива (ping/TCP-9444 OK). Зависший Raft-state, лечится **рестартом keeper-процесса** (или дропом диска Keeper + рестартом, если и это не помогает).
- **`Election Timer is never started but is requested to stop, protential a bug`** — известный баг ClickHouse Keeper 24.3, Raft-подсистема не стартует选举-таймер. Фикс — рестарт keeper.
- **`TOO_MANY_SIMULTANEOUS_QUERIES` на CH** — обычно следствие недоступности Keeper: запросы висят на ZK-операциях и копятся. Чинить корень (Keeper), а не лимит.
- **Хост не поднялся после работ в облаке** — `Task Instance ... is not scheduling on a minion, please start it first` → зайти в облако, нажать start. Если RUNNING но UNAVAILABLE долго — смотреть логи.
- **`Dependency failed for Clickhouse Server` при старте** — `dir-init.service` упал (обычно `No space left on device` на `/mnt/logs`). Виновник часто — `/mnt/logs/dbms/clickhouse-syslog.err.log` (root-owned, не ротируется CH). Фикс: `rm -rf /mnt/logs/{dbms,analytics,system,vector}/*` + `systemctl start mdb-clickhouse-server`. См. `history/incident_2026_08_mnt_logs_full.md`.
- **Part intersects previous part** — https://clickhouse.com/docs/knowledgebase/part_intersects_previous_part. Удалить проблемный парт (с бэкапом), проверить путь в Keeper, рестарт CH.
- **Сломалась схема на реплике** — `Table target.X_local does not exist`, реплика пустая. Смотреть `/mnt/logs/system/restore_ch.log`, при `can't create table ... already exist` — `SYSTEM DROP REPLICA 'name' FROM ZKPATH 'path'`, перезапустить restore.

## Что НЕ покрывает скилл

- Throughput / latency / performance — к Prometheus/Grafana.
- Настройка distributed-таблиц / DDL-запросы пользователей — к mdb-data API / пользователю.
- Дисковое место / memory — к хостовым чекерам.
- Backup/restore — отдельная дока.
