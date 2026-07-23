# Диагностика MDB PostgreSQL хостов

## Чеклист первичной разведки (read-only)

При поступлении "хост лежит" — выполнить одним заходом через expect:

```bash
expect -c '
set timeout 60
spawn mcc --local ssh <host>
expect "/# "
send "uptime; echo ===UPTIME===\r"
expect "===UPTIME==="
send "free -h; echo ===MEM===\r"
expect "===MEM==="
send "df -h | grep -E \"^/dev|Filesystem\"; echo ===DISK===\r"
expect "===DISK==="
send "systemctl status stolon-keeper stolon-proxy stolon-sentinel etcd pgbouncer --no-pager -l 2>&1 | grep -E \"●|Active:|Main PID:\" | head -40; echo ===SVC===\r"
expect "===SVC==="
send "ls -la /mnt/logs/dbms/ 2>&1 | head -30; echo ===LOGS===\r"
expect "===LOGS==="
send "exit\r"
expect eof
' 2>&1 | tail -150
```

Что смотреть:
- **uptime** — был ли ребут (load average не причина, но индикатор).
- **free** — память (если 0 free + swap — OOM).
- **df** — диск (если `/mnt/postgres` 100% — pg_wal переполнен).
- **systemctl** — все ли сервисы active.
- **ls /mnt/logs/dbms/** — какие логи есть, размеры (активно пишется = сервис жив).

## Симптом: rscheck: postgres is dead

### Шаг 1. stolon-keeper.log — что делает keeper

```bash
tail -80 /mnt/logs/dbms/stolon-keeper.log
```

Возможные картины:

**A. pg_basebackup идёт (реплика наливается):**
```
5523/26931 kB (20%), 0/1 tablespace (/mnt/postgres/postgres/base/1/2658)
```
→ **Ждать**, не рестартить. Наливка пойдёт с нуля при рестарте. Можно ускорить — см. `reinit_replica.md` (раздел "Как увеличить скорость наливки").

**B. pg_rewind идёт (реплика догоняется через rewind):**
```
pg_rewind: 2: 6FA/D508BD58 - 0/0
```
→ Ждать, обычно ~5 минут (интервал чекпоинтов).

**C. Разные local/cluster DB UID:**
```
INFO  cmd/keeper.go:1142  current db UID different than cluster data db UID  {"db": "", "cdDB": "60b20d7e"}
ERROR cmd/keeper.go:1469  different local dbUID but init mode is none, this shouldn't happen. Something bad happened to the keeper data. Check that keeper data is on a persistent volume and that the keeper state files weren't removed
```
→ Кто-то удалил диск (проверить audit storage: `https://cloud.vk.team/cloud/<DC>/ns/infra/shard/<cluster>.mdbdev.db.production.mdb.prod/db/<N>`).
→ **Переналивка, Простой случай** — см. `reinit_replica.md`.

**D. `database cluster not initialized` + `our db role is none`:**
```
INFO  cmd/keeper.go:1510  database cluster not initialized
INFO  cmd/keeper.go:1583  our db requested role is standby  {"followedDB": "..."}
INFO  cmd/keeper.go:1669  our db role is none
```
→ Keeper видит пустой data-dir, но stolon не даёт роль. Это бывает если:
  - data-dir был переименован без `stolonctl removekeeper` — stolon всё ещё помнит старый DB UID.
  - **Нужно**: `stolonctl removekeeper` + `rm -r /mnt/postgres/*` + start keeper. См. `reinit_replica.md` (Простой случай).

**E. Crash-loop postgres (fallback на walreceiver):**
```
INFO  cmd/keeper.go:1583  our db requested role is standby
INFO  postgresql/postgresql.go:319  starting database
ERROR cmd/keeper.go:727  cannot get configured pg parameters  {"error": "dial unix /tmp/.s.PGSQL.5432: connect: no such file or directory"}
```
→ Keeper стартует postgres, postgres падает. Смотреть `postgres.log` для FATAL.

### Шаг 2. postgres.log — повторяющиеся ошибки

```bash
tail -80 /mnt/logs/dbms/postgres.log
```

Возможные FATAL-ошибки:

**`FATAL: requested WAL segment ... has already been removed`**
→ Реплика лежала долго, нужный WAL уже удалён из архива. **Переналивка, Простой случай**.

**`FATAL: requested timeline N does not contain minimum recovery point ...`**
→ Timeline-gap после failover. WAL-сегмент с minimum recovery point не попал в S3-архив. **Переналивка, Простой случай**. Разбор — `history/2026-07-23-timeline-gap-shard1.md`.

**`FATAL: the database system is starting up`** (вместе с другими ошибками)
→ Не причина, следствие. Ждать или смотреть основную ошибку выше.

**WARNING: `invalid configuration parameter name "citus.max_shard_pool_size", removing it`**
→ Не критично, citus-параметр устарел. Не причина падения.

**`max_connections на реплике меньше чем на мастере`**
→ Поправить конфиг в PMS:
```bash
# 1. Добавить параметр в PMS в zen.pgsql.stolon.conf
# 2. На хосте:
confp --oneshot
stolonctl update --cluster-name stolon --store-backend etcdv3 --file /etc/stolon_init/stolon.conf
```
После следующего ретрая postgres поднимется.

### Шаг 3. etcd.log — если rscheck: etcd is dead

```bash
tail -50 /mnt/logs/dbms/etcd.log
etcdctl endpoint health
```

Если etcd зависает / dead → **Сложный случай** или **Самый сложный случай** — см. `reinit_replica.md`.

## Симптом: rscheck: stolon keeper is dead

Обычно следствие, не причина. Проверить:
```bash
systemctl status stolon-keeper
tail -50 /mnt/logs/dbms/stolon-keeper.log
```

Если `stolon-keeper.service` упал — рестартовать. Но если он падает повторно — смотреть лог, копать первопричину (обычно проблема с etcd или с postgres data-dir).

## Симптом: после ребута хоста postgres не поднимается

1. Проверить `/var/log/journal/` — есть ли persistent journal.
   ```bash
   ls -la /var/log/journal/
   journalctl --list-boots
   ```
   Если пусто и только 1 boot — journal до ребута **потерян**, источник shutdown не узнать.

2. Искать в `postgres.log` последнюю запись до `database system is shut down`:
   ```bash
   grep -B 5 "database system is shut down" /mnt/logs/dbms/postgres.log | head -30
   ```

3. `received fast shutdown request` — кто-то послал SIGINT. Кто именно — не узнать без journal. Варианты: stolon-keeper (при смене роли), mdb-data (через systemctl), ручное `pg_ctl stop`.

4. После shutdown postgres должен подняться автоматически. Если падает — смотреть FATAL в `postgres.log` после `starting PostgreSQL ...`.

## Проверка состояния Stolon-кластера

```bash
stolonctl --cluster-name stolon --store-backend etcdv3 --store-endpoints=127.0.0.1:2379 status
```

Что показывает:
- **Active sentinels** — 3 шт, лидер помечен `true`.
- **Active proxies** — 3 шт.
- **Keepers** — список всех хостов с `HEALTHY`, `PG HEALTHY`, `WantedGeneration`, `CurrentGeneration`.
- **Master Keeper** — текущий мастер.
- **Keepers/DB tree** — топология (мастер → standby'ы).

Если `PG HEALTHY: false` на каком-то хосте — там проблема. Если `WantedGeneration > CurrentGeneration` — stolon ждёт, пока keeper догонит конфиг.

## Проверка содержимого dbstate / keeperstate

```bash
cat /mnt/postgres/dbstate
cat /mnt/postgres/keeperstate
```

- `dbstate.UID` должен совпадать с DB UID в clusterdata (можно увидеть в `stolonctl status` или через `stolonctl clusterdata read`).
- Если `dbstate.UID` не совпадает — будет ошибка `current db UID different than cluster data db UID`.

## Проверка WAL на диске

```bash
# Размер
du -sh /mnt/postgres/postgres/pg_wal

# Количество файлов
ls /mnt/postgres/postgres/pg_wal/ | wc -l

# История timelines
ls /mnt/postgres/postgres/pg_wal/*.history

# Статус архивации
ls /mnt/postgres/postgres/pg_wal/archive_status/ | head -10
```

Если `pg_wal` занимает больше, чем сама БД — диск может переполниться, и pg_basebackup начнётся заново. В этом случае можно удалить файлы сегментов из `pg_wal/` — при старте реплика подтянет их из S3.

## Проверка S3-архива WAL

```bash
aws s3api --endpoint-url https://s3.idzn.ru list-objects \
  --bucket db-backups \
  --prefix "pgsql/<cluster-uuid>/wal_005/" \
  --query 'Contents[].Key' | head -20
```

Сравнить последние сегменты с `pg_current_wal_lsn()` на мастере — если отстают, archive_command не успевает.

## Что НЕ делать при диагностике

- **Не рестартить сходу** — если реплика наливается, наливка пойдёт с нуля.
- **Не `rm` ничего без подтверждения** — особенно в `/mnt/etcd/` и `/mnt/postgres/`.
- **Не `stolonctl update` без `confp --oneshot`** — рассинхронизация PMS ↔ etcd.
- **Не менять параметры postgres без синхронизации с PMS** — будет расхождение при следующих update.
- **Не удалять `dbstate`/`keeperstate` без `stolonctl removekeeper`** — stolon потеряет keeper, но в clusterdata он останется → конфликт.
