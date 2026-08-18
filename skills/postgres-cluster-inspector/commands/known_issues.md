# Известные проблемы MDB PostgreSQL

Каталог проблем, встречающихся при диагностике Stolon-кластеров. Каждая запись:
симптом → причина → диагностика → фикс.

## 1. Timeline-gap после failover

### Симптом
На реплике в `postgres.log`:
```
FATAL:  requested timeline 4 does not contain minimum recovery point 3D/F60493D8 on timeline 3
```
Реплика краш-лупится, не может встать в standby.

### Причина
После переключения мастера (failover) новый мастер поднялся на новом таймлайне (4).
Для восстановления реплики нужен WAL-сегмент с minimum recovery point (`3D/F6...`),
но этот сегмент **не попал в S3-архив** (archive_command не успел или не успел дойти
до переключения).

Локально на реплике WAL тоже обрезан — pg_wal хранит только последние сегменты.

### Диагностика
1. На мастере: `SELECT timeline_id FROM pg_control_checkpoint();` → текущий timeline.
2. На реплике: `ls /mnt/postgres/postgres/pg_wal/*.history` — какие history-файлы есть.
3. В S3:
   ```bash
   aws s3api --endpoint-url https://s3.idzn.ru list-objects \
     --bucket db-backups --prefix "pgsql/<UUID>/wal_005/" \
     --query 'Contents[].Key' | grep -E "3D|history"
   ```
   Сравнить — есть ли сегмент с minimum recovery point.

### Фикс
**Переналивка реплики, Простой случай** — см. `reinit_replica.md`.
Локальные WAL/`pg_wal` не помогут — нужного сегмента нет нигде.

### Разбор
- `history/2026-07-23-timeline-gap-shard1.md`

## 2. `requested WAL segment ... has already been removed`

### Симптом
В `postgres.log`:
```
FATAL:  could not receive data from WAL stream: ERROR:  requested WAL segment 0000000B000007AB00000047 has already been removed
```

### Причина
Реплика лежала долго (больше `wal_keep_size` + `archive_retention` в S3). Нужный
WAL-сегмент удалён из `pg_wal` мастера и из S3-архива.

### Диагностика
- `stolonctl status` — посмотреть `CurrentGeneration` и когда хост последний раз был healthy.
- В S3 посмотреть последний WAL-сегмент — если он сильно старше нужного, archive_command
  ничего не поделает.

### Фикс
Переналивка, Простой случай — см. `reinit_replica.md`.

## 3. `current db UID different than cluster data db UID`

### Симптом
В `stolon-keeper.log`:
```
INFO  current db UID different than cluster data db UID  {"db": "", "cdDB": "60b20d7e"}
ERROR different local dbUID but init mode is none, this shouldn't happen.
```
Keeper не переинициализирует, крутится в цикле.

### Причина
Кто-то удалил/переименовал `postgres/` без `stolonctl removekeeper`. В clusterdata
в etcd ещё помнят старый DB UID, а локально в `dbstate` UID либо пустой, либо другой.

### Диагностика
```bash
cat /mnt/postgres/dbstate                              # локальный UID
stolonctl status --cluster-name stolon --store-backend etcdv3
# в clusterdata — UID для нашего keeper'а
```

### Фикс
1. `stolonctl removekeeper <keeper_uid>` — убрать keeper из clusterdata.
2. `rm -r /mnt/postgres/*` — очистить **весь** data-dir (включая `dbstate`/`keeperstate`/`lock`).
3. `systemctl start stolon-keeper` — keeper поднимется с новым UID.

См. `reinit_replica.md` (Простой случай).

## 4. `database cluster not initialized` + `our db role is none`

### Симптом
```
INFO  database cluster not initialized
INFO  our db requested role is standby
INFO  our db role is none
```
Keeper видит пустой data-dir, но Stolon не даёт роль.

### Причина
- `data-dir` был очищен без `stolonctl removekeeper` → Stolon помнит старый DB UID.
- Либо keeper не зарегистрирован в clusterdata (новый хост, не добавлен через `stolonctl`).

### Фикс
См. `reinit_replica.md` (Простой случай, Частые ошибки).

## 5. `max_connections` на реплике меньше чем на мастере

### Симптом
В `postgres.log`:
```
FATAL:  parameter "max_connections" must be greater than or equal to ...
```

### Причина
На мастере подняли `max_connections`, в PMS это не отразили. После failover реплика
получает значение из своего конфига, которое меньше.

### Фикс
1. В PMS (`zen.pgsql.stolon.conf`) выставить `max_connections` так же как на мастере.
2. На хосте:
   ```bash
   confp --oneshot
   stolonctl update --cluster-name stolon --store-backend etcdv3 --file /etc/stolon_init/stolon.conf
   ```

## 6. etcd сломался / `rscheck: etcd is dead`

### Симптом
- `etcdctl endpoint health` зависает или возвращает ошибку.
- `stolon-keeper.log` сыпет `failed to connect to etcd`.

### Причина
- Смерть железа / кривая миграция на другой миньон.
- 2 копии volume в статусе NORMAL.
- Рассыпался кворум (2 из 3 миньонов выпали).

### Фикс
- Если etcd-диск повреждён, но кворум есть — `reinit_replica.md` (Сложный случай):
  удаление volume, `etcdctl member remove` + `member add`, `wipe-etcd`.
- Если кворум развалился (2 из 3 хостов down) — `reinit_replica.md` (Самый сложный случай):
  snapshot restore на оставшемся хосте, потом регистрация остальных.

## 7. `.history` с таймлайном больше актуального

### Симптом
Реплика полностью переналилась (pg_basebackup завершился успешно), но не поднимается.
В логах невнятные ошибки о битом WAL.

### Причина
При хитрой комбинации нетсплитов и удачных таймингов переключений в бакете появляется
`.history` файл от таймлайна с номером **больше** чем актуальный таймлайн мастера.
Реплика встаёт на этот таймлайн (как самый старший), но он не потомок актуального —
не может стримить WAL.

### Диагностика
1. На мастере: `SELECT timeline_id FROM pg_control_checkpoint();` (десятичный).
2. На реплике: `ls -la /mnt/postgres/postgres/pg_wal | grep .history` (HEX!).
3. Если есть history-файл с номером больше → проблема.

### Фикс
1. Сохранить копию history-файла локально (текстовый, маленький).
2. Удалить из S3:
   ```bash
   aws s3api --endpoint-url https://s3.idzn.ru delete-object \
     --bucket db-backups --key "pgsql/<UUID>/wal_005/0000001B.history.br"
   ```
3. Удалить с хоста.
4. Форсировать полную переналивку — `reinit_replica.md` (Простой случай).

См. `reinit_replica.md` (раздел "Реплика не поднимается даже после полной переналивки").

## 8. Postgres падает после ребута хоста

### Симптом
После перезагрузки хоста postgres не поднимается. В `postgres.log`:
```
LOG:  database system is shut down
```
И больше ничего / FATAL после.

### Диагностика
1. `journalctl --list-boots` — если только 1 boot и `/var/log/journal/` пустой →
   journal до ребута потерян, причину shutdown не узнать.
2. `grep -B 5 "database system is shut down" /mnt/logs/dbms/postgres.log` — контекст
   перед shutdown.
3. Искать FATAL после `starting PostgreSQL` — это уже причина невосстановления.

### Частые причины
- `requested WAL segment ... removed` (см. проблему 2).
- Timeline-gap (см. проблему 1).
- `max_connections` (см. проблему 5).

## 9. pg_wal переполняет диск

### Симптом
- `df -h /mnt/postgres` → 100%.
- `du -sh /mnt/postgres/postgres/pg_wal` → размер сопоставим с размером БД.

### Причина
- `archive_command` не справляется / S3 недоступен → WAL не архивируется и копится.
- Реплика отстаёт, мастер держит WAL для неё.

### Фикс
- Удалить старые WAL-сегменты из `pg_wal/` — при старте реплика подтянет из S3.
- Проверить `archive_status/` — если много `.ready` без `.done`, archive_command не работает.
- Проверить S3-архив (см. `diagnostics.md`).

## Что НЕ делать

- **Не рестартить `stolon-keeper` на наливающейся реплике** — наливка начнётся с нуля.
- **Не `rm` в `/mnt/etcd/`** — etcd умрёт, в postgresql пока нет поднятия реплики на
  пустых дисках без ручных вмешательств (MDBDEV-1372).
- **Не `stolonctl update` без `confp --oneshot`** — рассинхрон PMS ↔ etcd.
- **Не удалять `dbstate`/`keeperstate` без `stolonctl removekeeper`** — в clusterdata
  останется старый keeper, будет конфликт.
- **Не менять параметры postgresql.conf руками** — только через PMS + `confp` + `stolonctl update`.
