# 2026-07-23 — Timeline-gap после failover на shard1-db

## Кратко
- **Хост:** `1.shard1-db.postgres-load-test-cxhub-pgsql.dc.one-infra.ru` (standby в ДЦ dc).
- **Кластер:** `postgres-load-test-cxhub-pgsql` (3 хоста: `1.shard1-db` в dc/hc/pc).
- **Симптом:** postgres краш-лупится с `FATAL: requested timeline 4 does not contain
  minimum recovery point 3D/F60493D8 on timeline 3`.
- **Диагноз:** после failover WAL-сегмент с minimum recovery point (`3D/F6`) не попал
  в S3-архив. Локально на реплике тоже отсутствует.
- **Решение:** полная переналивка (Простой случай) — `stolonctl removekeeper` + `rm -r
  /mnt/postgres/*` + рестарт `stolon-keeper`.

## Хронология инцидента

| Время (MSK) | Событие |
|---|---|
| 03:00–11:47 | На кластере failover: мастер сменился (`dc30568d` → `26bb70de` на хосте hc). |
| 03:12:10 | `postgres.log`: `received fast shutdown request` — stolon-keeper остановил postgres. |
| 03:12:1x | postgres в recovery: `could not receive data from WAL stream` → FATAL timeline 4 does not contain minimum recovery point `3D/F60493D8` on timeline 3. |
| 03:12–11:47 | Краш-луп: stolon-keeper стартует postgres → FATAL → restart → FATAL → … |
| 11:47:03 | Ребут хоста. `/var/log/journal/` пустой — journal до ребута потерян. |
| 13:45 | Начали разбор. Сохранили состояние: `postgres.broken-20260723/`, логи скачаны. |
| 13:49 | `systemctl stop pgbouncer`, `systemctl stop stolon-keeper`, `stolonctl removekeeper 1_shard1db_postgresloadtestcxhubpgsql_dc_oneinfra_ru`. |
| 13:56 | `rm -r /mnt/postgres/*`, `systemctl start stolon-keeper` → старт pg_basebackup. |
| 13:56–16:05 | `pg_basebackup` качает ~77 ГБ при ~10 МБ/с (без `STOLON_PG_BASEBACKUP_MAX_RATE`). |
| 16:05 | postgres поднялся в standby: `database system is ready to accept read-only connections`, `started streaming WAL from primary at 3D/FD000000 on timeline 4`. |
| 16:05+ | `stolonctl status`: наш keeper `PG HEALTHY: true`, Generation 2/2. |

## Диагностика

### Stolon status до восстановления
- Master keeper UID: `26bb70de` (на хосте `hc.one-infra.ru`) — стал мастером после failover.
- Наш keeper (`dc_oneinfra_ru`): `PG HEALTHY: false`, роль standby, но postgres не поднимается.

### postgres.log (краш-луп)
```
LOG:  received fast shutdown request
LOG:  aborting any active transactions
...
LOG:  database system is shut down
LOG:  PostgreSQL 18.4 ... starting
LOG:  entering standby mode
LOG:  redo starts at 3D/92000028
LOG:  consistent recovery state reached at 3D/92264E30
LOG:  database system is ready to accept read-only connections
LOG:  restored log file "000000030000003D000000F5" from archive
FATAL:  requested timeline 4 does not contain minimum recovery point 3D/F60493D8 on timeline 3
LOG:  startup process exit with code 1
... (повтор)
```

### Локальный WAL на реплике
```
ls /mnt/postgres/postgres/pg_wal/
000000030000003D00000092   ← последний локальный
000000030000003D000000F5   ← подтянут из архива
00000004.history           ← текущий таймлайн мастера = 4
```
Сегмента `000000030000003D000000F6` (содержит minimum recovery point `3D/F6...`) нет ни
локально, ни в архиве.

### S3-архив
```bash
aws s3api --endpoint-url https://s3.idzn.ru list-objects \
  --bucket db-backups --prefix "pgsql/<UUID>/wal_005/" \
  --query 'Contents[].Key' | grep "3D/F"
# есть 3D/F5, нет 3D/F6, нет 00000005.history
```

### Вывод
WAL-сегмент `3D/F6`, содержащий minimum recovery point `3D/F60493D8`, не был
заархивирован в S3 до переключения мастера. Реплика не может пройти точку
переключения — нет ни локального, ни архивного WAL. Единственный путь —
полная переналивка с мастера.

## Процедура восстановления (Простой случай)

Выполнялось через `expect + mcc ssh` (см. `commands/connection.md`).

```bash
systemctl stop pgbouncer
systemctl stop stolon-keeper

# keeper UID = hostname с подчёркиваниями вместо - и .
stolonctl removekeeper 1_shard1db_postgresloadtestcxhubpgsql_dc_oneinfra_ru \
  --cluster-name stolon --store-backend etcdv3

# проверить, что keeper исчез
stolonctl status --cluster-name stolon --store-backend etcdv3

# полная очистка data-dir (включая dbstate/keeperstate/lock!)
rm -r /mnt/postgres/*

systemctl start stolon-keeper
systemctl start pgbouncer
```

### Контроль что pg_basebackup идёт
```bash
tail -f /mnt/logs/dbms/stolon-keeper.log
# 5523/26931 kB (20%), 0/1 tablespace (/mnt/postgres/postgres/base/1/2658)
ps -eo pid,etime,comm,args | grep pg_basebackup | grep -v grep
```

### Финальная проверка
`postgres.log`:
```
LOG:  entering standby mode
LOG:  redo starts at ...
LOG:  consistent recovery state reached at ...
LOG:  database system is ready to accept read-only connections
LOG:  started streaming WAL from primary at ... on timeline 4
```

`stolonctl status` — наш keeper `PG HEALTHY: true`, `WantedGeneration == CurrentGeneration`.

## Ошибки, допущенные при разборе

1. **Первая попытка восстановления не удалась.**
   Только переименовал `postgres/` в `postgres.broken-20260723`, оставил
   `dbstate`/`keeperstate`/`lock`, не сделал `stolonctl removekeeper`.
   → Keeper крутится в цикле `database cluster not initialized` + `our db role is none`.
   **Фикс:** `stolonctl removekeeper` + `rm -r /mnt/postgres/*` (полная очистка).

2. **Первая попытка `stolonctl removekeeper` оборвала expect-сессию.**
   Команда не успела завершиться, keeper остался в clusterdata.
   **Фикс:** повторный заход — `stolonctl removekeeper` отработал, keeper исчез из статуса.

3. **`grep -E "0[23]:"` сломал Tcl-парсер expect.**
   `invalid command name "23"` — Tcl интерпретирует `[23]` как command substitution.
   **Фикс:** `grep "^2026-07-23 03:"` без класса символов, либо экранировать `\[23\]`.

## Извлечённые уроки

1. **Перед очисткой `/mnt/postgres/` — всегда `stolonctl removekeeper`.** Иначе Stolon
   помнит старый DB UID в clusterdata и не даёт новую роль.
2. **Очищать весь data-dir, не только `postgres/`.** `dbstate`/`keeperstate`/`lock`
   тоже нужно снести — иначе keeper может подхватить старое состояние.
3. **`/var/log/journal/` часто volatile-only.** После ребута journal до ребута теряется.
   Не полагаться на `journalctl` для разбора инцидентов до ребута — только `/mnt/logs/dbms/`.
4. **Tcl expect ломается на `[...]`.** Использовать `grep` без character class либо
   экранировать.
5. **При timeline-gap единственный фикс — полная переналивка.** Локальный WAL/архив
   не помогут, нужного сегмента нет нигде.
6. **`STOLON_PG_BASEBACKUP_MAX_RATE=25M` ускоряет наливку (200 Мбит/с).** Для 77 ГБ
   это разница между 2 часами и 30 минутами. Задаётся через env в манифесте.

## Ссылки
- `commands/reinit_replica.md` — Простой случай (процедура).
- `commands/diagnostics.md` — картина E (краш-цикл).
- `commands/known_issues.md` — проблема 1 (timeline-gap).
