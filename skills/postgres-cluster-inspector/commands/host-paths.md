# Путеводитель по путям на MDB PostgreSQL хосте

## Диски и mount points

| Путь | Назначение | Размер (типично) | LV |
|---|---|---|---|
| `/mnt/postgres/` | Stolon data-dir + PGDATA | 1.0 TB | cloud.nvme-... |
| `/mnt/etcd/` | etcd data — **НЕ ТРОГАТЬ** | 4 GB | cloud.nvme-... |
| `/mnt/logs/` | Логи сервисов | 10 GB | cloud.nvme-... |
| `/one/logs/` | Системные логи | 224 GB | srvd... |

## Stolon data-dir (`/mnt/postgres/`)

```
/mnt/postgres/
├── postgres/              ← PGDATA (data-dir postgres)
│   ├── base/              ← данные таблиц
│   ├── global/            ← глобальные каталоги
│   ├── pg_wal/            ← WAL-сегменты
│   │   ├── 000000030000003D00000075  ← WAL-сегмент (timeline 3, segment 3D/75)
│   │   ├── ...
│   │   ├── 00000003.history           ← history timeline 3
│   │   ├── 00000004.history           ← history timeline 4
│   │   └── archive_status/            ← .done/.ready файлы для archive_command
│   ├── postmaster.pid     ← PID-файл postgres
│   ├── postgresql.auto.conf ← conninfo для standby (primary_conninfo)
│   ├── standby.signal      ← маркер standby-режима
│   └── ...
├── dbstate                ← JSON: {"UID":"...","Generation":N,"Initializing":false,...}
├── keeperstate            ← JSON: {"UID":"hostname_with_underscores","ClusterUID":"..."}
└── lock                   ← lock-файл (0 байт)
```

### dbstate / keeperstate — критически важны

- `dbstate.UID` — DB UID, который stolon-keeper считает своим. После `stolonctl removekeeper` + `rm -r /mnt/postgres/*` этот файл пересоздаётся с новым UID.
- `keeperstate.UID` — ID keeper'а (= hostname с подчёркиваниями вместо `-` и `.`).
- `keeperstate.ClusterUID` — UID кластера Stolon.
- **Если `dbstate.UID` не совпадает с clusterdata** → stolon-keeper пишет `current db UID different than cluster data db UID` и не переинициализирует.

## Логи (`/mnt/logs/dbms/`)

| Файл | Что внутри |
|---|---|
| `postgres.log` | Логи PostgreSQL (запуск, recovery, ошибки, FATAL) |
| `stolon-keeper.log` | Логи stolon-keeper (смешаны с логами postgres — keeper запускает postgres как дочерний процесс) |
| `stolon-proxy.log` | Логи stolon-proxy (маршрутизация клиентов к мастстре) |
| `stolon-sentinel.log` | Логи sentinel (только если наш sentinel — лидер; иначе пусто) |
| `etcd.log` | Логи etcd |
| `pgbouncer.log` | Логи pgbouncer |
| `pgbouncer-reloader.log` | Перезагрузка pgbouncer при failover |
| `pgbouncer-exporter.log` | Prometheus exporter для pgbouncer |
| `postgresql-exporter.log` | Prometheus exporter для postgres |
| `citus-failover-worker.log` | Citus failover sync (на worker-хостах) |
| `citus-failover-coordinator.log` | Citus failover sync (на coordinator-хостах, часто пустой на worker) |
| `citus-poolinfo-sync.log` | Citus poolinfo sync |
| `reset-on-empty-disk.log` | Лог при reset хоста на пустой диск |
| `stolon-init-service.log` | Лог инициализации stolon при старте |

### Формат timestamp в логах

- postgres.log: `2026-07-23 03:12:13.461 MSK [PID] LOG:  ...`
- stolon-keeper.log: `2026-07-23T13:45:38.555+0300  INFO  cmd/keeper.go:1583  ...` (RFC3339)
- etcd.log: `2026-07-23 11:47:16.643341 I | etcdserver: ...`

### Греп по времени

```bash
# postgres.log за 03:00-03:59
grep "^2026-07-23 03:" /mnt/logs/dbms/postgres.log | head -60

# stolon-keeper.log за 03:00-03:59
grep "^2026-07-23T03:" /mnt/logs/dbms/stolon-keeper.log | head -60
```

⚠️ **Не использовать `grep -E "0[23]:"`** — Tcl в expect ломается на `[23]`. Использовать отдельные grep или escape `\[23\]`.

## Конфиги

| Путь | Что |
|---|---|
| `/etc/stolon_init/stolon.conf` | Конфиг Stolon (рендерится из PMS через `confp --oneshot`) |
| `/etc/stolon/pg-su-password` | Пароль суперпользователя postgres |
| `/etc/stolon/pg-repl-password` | Пароль replication-пользователя |
| `/etc/pgbouncer/pgbouncer.ini` | Конфиг pgbouncer |
| `/etc/etcd/etcd.conf` | Конфиг etcd (правится при добавлении/удалении члена) |
| `/etc/rscheck/` | Конфиги rscheck |
| `/etc/analytics/pg_analytics.yaml` | Конфиг аналитики |
| `/etc/citus/` | Скрипты Citus (citus_failover_worker.py, citus_common.py, citus_poolinfo_sync.py) |

## Systemd

```bash
# Все ключевые сервисы
systemctl status stolon-keeper stolon-proxy stolon-sentinel etcd pgbouncer --no-pager -l

# Журнал сервиса за период
journalctl -u stolon-keeper --since="2026-07-23 11:46:30" --until="2026-07-23 11:48:00" --no-pager

# Список boot'ов (если /var/log/journal/ пустой — будет только текущий boot)
journalctl --list-boots
```

⚠️ **`/var/log/journal/` часто volatile-only** — после ребута хоста journal до ребута теряется. Не полагаться на journalctl для разбора инцидентов до ребута.

## AWS CLI для S3 archive

`/usr/local/bin/aws` — есть на хосте. Используется для проверки WAL-архива:

```bash
# Листинг WAL-сегментов в бакете (нужно знать бакет и префикс кластера)
aws s3api --endpoint-url https://s3.idzn.ru list-objects --bucket db-backups --prefix "pgsql/<cluster-uuid>/wal_005/" --query 'Contents[].Key' | head -20

# Удалить битый .history из S3
aws s3api --endpoint-url https://s3.idzn.ru delete-object --bucket db-backups --key "pgsql/<cluster-uuid>/wal_005/0000001B.history.br"
```

## PMS / confp

`confp` — утилита для рендера конфигов из PMS (pms.cloud.vk.team):

```bash
confp --oneshot          # применить PMS-конфиги к хосту (перезапишет /etc/stolon_init/stolon.conf и др.)
```

После `confp --oneshot` нужно сделать `stolonctl update`:

```bash
stolonctl update --cluster-name stolon --store-backend etcdv3 --file /etc/stolon_init/stolon.conf
```

## Etcd

```bash
etcdctl endpoint health                          # здоровье локального etcd
etcdctl member list -w table                     # список членов etcd-кластера
etcdctl --endpoints=127.0.0.1:2379 get /stolon/cluster/stolon/ --prefix --keys-only  # ключи Stolon
```

## Важные команды на хосте

| Что | Команда |
|---|---|
| Текущий timeline postgres | `sudo -u postgres psql -h /tmp -p 5432 -U root -d postgres -c "SELECT timeline_id FROM pg_control_checkpoint();"` |
| LSN на мастере | `SELECT pg_current_wal_lsn();` |
| Состояние репликации (на мастере) | `SELECT * FROM pg_stat_replication;` |
| Состояние репликации (на реплике) | `SELECT * FROM pg_stat_wal_receiver;` |
| In recovery? | `SELECT pg_is_in_recovery();` |
| Список слотов | `SELECT * FROM pg_replication_slots;` |
