# Скилл инспекции MDB PostgreSQL кластеров

Скилл для разбора состояния PostgreSQL-кластеров под управлением mdb-data (Stolon + etcd + Citus + pgbouncer) через логи, etcd-состояние и stolonctl.

⚠️ Скилл проверяет **состояние процессов postgres + stolon + etcd + pgbouncer** (запуск, роль, replication, rscheck) через **логи и stolonctl/etcdctl**. Не покрывает: throughput / latency, настройки пользователей / баз, производительность — это к Prometheus/Grafana и mdb-data API.

> Доступ к хостам и грабли Tcl/SSL/Namespace — в скилле
> [`mcc-host-worker`](../mcc-host-worker/SKILL.md). Ниже — только специфика PostgreSQL.

## Документация

- **Дежурная инструкция (SSOT)**: [Дежурство MDB: Postgres](https://confluence.vk.team/pages/viewpage.action?pageId=1348619018)
  — полный runbook дежурного (правила безопасной работы, пользователи/базы/подписки,
  переналивка, etcd без кворума, pgbouncer и т.д.). Вики живая — копии в скилле не храним.
- **Шардированный PostgreSQL (Citus)**: [Дежурство MDB: шардированный PostgreSQL](https://confluence.vk.team/pages/viewpage.action?pageId=2107500377)
  — архитектура, pg_dist_*-диагностика, добавление БД, недоступность координатора/шарда.
- https://docs.vk.team/mdb/docs/ — общая документация MDB

## Архитектура кластера

- **Stolon** — управление репликацией и failover. Роли: master / standby.
- **etcd** — распределённое хранилище состояния Stolon (clusterdata, keepers, sentinels, proxies). 3 члена в кластере, кворум 2/3.
- **PostgreSQL** — фактическая БД, управляется stolon-keeper. Запускается **не** через `postgresql.service`, а через `stolon-keeper.service` (postgres — дочерний процесс keeper).
- **pgbouncer** — пулер коннектов, отдельный сервис.
- **Citus** — sharding-расширение (если кластер шардированный). Coordinator + workers. На worker-хостах крутится `citus_failover_worker.py`, `citus_poolinfo_sync.py`.
- **WAL архивация** — в S3 (через wal-g). Используется для восстановления реплик.

## Разделение ролей в Stolon

- **master keeper** — UID БД является "master" в clusterdata, принимает writes.
- **standby keeper** — UID БД следует за master DB UID (`followedDB`), стримит WAL.
- **sentinel** — 3 штуки на кластер, выбирают лидера, принимают решения о failover.
- **proxy** — 3 штуки, проксируют клиентские коннекты к текущему мастеру.

## Формат хостов

```
{1,2,3,...}.db.<cluster>.<dc>.one-infra.ru         — обычный postgresql
{1,2,3,...}.shard<N>-db.<cluster>.<dc>.one-infra.ru — Citus shard (worker)
<cluster> = <name>-pgsql, <dc> = hc|pc|uc|kc|ec|dc|rc|...
```

Пользователь даёт список хостов. Скилл не угадывает хосты — только работает с тем, что дал юзер.

## Что нужно

- **Доступ к хостам** — через скилл [`mcc-host-worker`](../mcc-host-worker/SKILL.md).
  Специфика PostgreSQL-хостов — `commands/connection.md`.

## Хосты и пути

| Что | Путь на хосте |
|---|---|
| Логи сервисов | `/mnt/logs/dbms/` (postgres.log, stolon-keeper.log, stolon-proxy.log, stolon-sentinel.log, etcd.log, pgbouncer.log, citus-*.log) |
| Data-dir postgres | `/mnt/postgres/postgres/` (PGDATA) |
| Stolon state | `/mnt/postgres/{dbstate,keeperstate,lock}` |
| WAL | `/mnt/postgres/postgres/pg_wal/` (сегменты + `archive_status/` + `.history` файлы) |
| etcd data | `/mnt/etcd/etcd/` — **КАТЕГОРИЧЕСКИ не трогать** |
| Stolon-конфиг | `/etc/stolon_init/stolon.conf` |
| Stolon креды | `/etc/stolon/pg-su-password`, `/etc/stolon/pg-repl-password` |
| rscheck | `/etc/rscheck/` |
| pgbouncer | `/etc/pgbouncer/pgbouncer.ini` |
| Systemd | `/etc/systemd/system/{stolon-keeper,stolon-proxy,stolon-sentinel,etcd,pgbouncer}.service` |
| Journal | `/var/log/journal/` — часто **volatile-only** (пустой после ребута!) |

## Структура скилла

- `SKILL.md` — этот файл.
- `commands/connection.md` — PostgreSQL-специфичные команды на хосте (stolonctl, etcdctl, psql/pgbouncer admin, чтение dbstate/keeperstate).
- `commands/host-paths.md` — путеводитель по путям на хосте.
- `commands/diagnostics.md` — что проверять при разных симптомах (postgres is dead, etcd is dead, реплика не догоняет).
- `commands/reinit_replica.md` — Переналивка реплики: Простой/Сложный/Самый сложный случаи.
- `commands/runbook.md` — навигация: ссылки на вики-страницы дежурства (Postgres +
  шардированный PostgreSQL).
- `history/` — каталог разобранных инцидентов:
  - `history/2026-07-23-timeline-gap-shard1.md` — кейс timeline-gap после failover, переналивка через pg_basebackup.

## Диагностика по симптомам

| Симптом | Куда смотреть | Подробности |
|---|---|---|
| rscheck: postgres is dead | `/mnt/logs/dbms/postgres.log` + `/mnt/logs/dbms/stolon-keeper.log` | `commands/diagnostics.md` |
| rscheck: etcd is dead | `/mnt/logs/dbms/etcd.log` | `commands/diagnostics.md`, `commands/reinit_replica.md` (Сложный случай) |
| Crash-loop postgres | `postgres.log` — повторяющиеся FATAL | `commands/diagnostics.md` |
| Replication lag растёт | `pg_stat_replication`, `pg_wal` | вне скилла — к Grafana |
| `requested WAL segment ... has already been removed` | `postgres.log` + S3 archive | `commands/reinit_replica.md` (Простой случай) |
| `requested timeline N does not contain minimum recovery point ...` | `postgres.log` + S3 archive (.history files) | `history/2026-07-23-timeline-gap-shard1.md` |
| `different local dbUID but init mode is none` | `stolon-keeper.log` | кто-то удалил диск → `commands/reinit_replica.md` (Простой случай) |

## Известные проблемы (кратко)

Подробности — `known_issues.md` и
[вики «Дежурство MDB: Postgres»](https://confluence.vk.team/pages/viewpage.action?pageId=1348619018).

- **Timeline-gap после failover** — на мастере promote без попадания нужного WAL-сегмента в S3. Реплика не может переключиться на новую timeline. Лечится переналивкой (Простой случай). Разбор — `history/2026-07-23-timeline-gap-shard1.md`.
- **`requested WAL segment ... has already been removed`** — реплика лежала дольше TTL архива (7 дней), либо wal-g не успел заархивировать. Лечится переналивкой.
- **`max_connections на реплике меньше чем на мастере`** — поправить конфиг в PMS → `confp --oneshot` → `stolonctl update`. Без PMS-синхронизации чинить нельзя.
- **`different local dbUID but init mode is none`** — кто-то удалил диск (проверить audit storage). Переналивка (Простой случай) с обязательным `stolonctl removekeeper` + `rm -r /mnt/postgres/*`.
- **Crash-loop postgres после ребута хоста** — `/var/log/journal/` volatile-only, journal до ребута не сохраняется. Источник shutdown установить нельзя — копать mdb-data аудиты / логи соседних хостов.
- **`stolon keeper is dead` в rscheck** — обычно следствие, не причина. Сначала проверить `stolon-keeper.service` и `stolon-keeper.log`.
- **`pgbouncer-security-bootstrap` падает** — нормально для standby-реплики (не нужен), не причина.
- **Replica не поднимается даже после полной переналивки** — в S3-бакете есть `.history` файл от timeline номер больше актуального. Удалить из S3 + форсировать повторную переналивку. Разбор — [вики «Дежурство MDB: Postgres»](https://confluence.vk.team/pages/viewpage.action?pageId=1348619018), раздел «Реплика не поднимается даже после полной переналивки».

## Что НЕ покрывает скилл

- Throughput / latency / performance — к Prometheus/Grafana.
- Настройка пользователей / баз / ACL — к mdb-data API и
  [вики «Дежурство MDB: Postgres»](https://confluence.vk.team/pages/viewpage.action?pageId=1348619018).
- Backup / restore — к mdb-data оператору.
- Сетевые лимиты между ДЦ — к cloud-инфра.
- Diskquota / memory — к хостовым чекерам.

## Важные предупреждения

- **НЕ зачищать диски etcd** — `/mnt/etcd/` трогать категорически нельзя, развалит весь кластер Stolon.
- **НЕ менять параметры postgres без синхронизации с PMS** — получим расхождение и неожиданное поведение при следующих обновлениях.
- **НЕ делать `stolonctl update` без `confp --oneshot`** — конфиг в etcd рассинхронизируется с PMS.
- **`/var/log/journal/` часто volatile** — не полагаться на journalctl для разбора инцидентов до ребута.
- **Перед `rm -r /mnt/postgres/*`** — сохранить `dbstate`/`keeperstate` содержимое (cat в файл), это единственный след старого DB UID.
