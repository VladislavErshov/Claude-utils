# Runbook для дежурного по Redis Sentinel

Детали по процедурам из `SKILL.md`. Краткие версии — там же.

## Вечная переливка реплик

Наиболее частая проблема — «вечная переливка» реплик.

**Симптомы**:
- Долгое время CPU у мастера > 100%.
- Высокая загрузка сети у реплики и мастера.
- В логах:
  ```
  Replication buffer limit has been reached (268435456 bytes),
  stopped buffering replication stream. Further accumulation may occur on master side.
  ```

**Быстрое решение**:
1. Поднять сеть на OUT у мастера, на IN у реплики.
2. Зайти через `mcc ssh` на мастер, через `redis-cli`:

```
redis-cli
127.0.0.1:6379> auth master <password>
OK
127.0.0.1:6379> config get repl-backlog-size
1) "repl-backlog-size"
2) "2147483648"
127.0.0.1:6379> config set repl-backlog-size 2GB
OK
127.0.0.1:6379> config get repl-timeout
1) "repl-timeout"
2) "60"
127.0.0.1:6379> config set repl-timeout 120
OK
127.0.0.1:6379> config get client-output-buffer-limit
1) "client-output-buffer-limit"
2) "normal 0 0 0 slave 2147483648 1073741824 180 pubsub 33554432 8388608 60"
127.0.0.1:6379> config set client-output-buffer-limit "replica 2GB 1GB 180"
OK
```

По умолчанию `repl-backlog-size` = 1mb — очень низкое значение! Поднимать до 10–20%
от `maxmemory`.

3. Чтобы быстро поменять конфигурацию на всех хостах — скрипт из раздела
   «Включить access-логи / Повысить уровень логирования / Задать параметр конфигурации
   в рантайме» (см. `admin.md`).
4. Обновить настройки в `zen.redis.config` и БД.
5. После того как реплика нальётся и кластер стабилизируется — пересмотреть параметры и
   обновить их через UI.

**Отслеживание**: Мониторинг → Replication > Replica backlog size.

## Закончился диск

Может возникать на старых кластерах, где логи писались вместе со снапшотами и AOF-файлами
(на один диск), либо при неправильной конфигурации.

**Симптомы**:
- Реплика поднимается, видит рассинхрон с мастером, пытается скачать бэкап, но падает —
  заканчивается место (копятся `temp-%d.rdb`).
- Либо мастер делает перезапись AOF или снапшот — диск вдруг заканчивается.
- Либо AOF-часть заполнила весь диск.

**Лечение**:
- Поднять диск.
- Если это реплика и с мастером всё в порядке — можно просто почистить диск, но после
  в любом случае требуется увеличение диска.
- Если там старый образ без логов — обновить по инструкции (см. раздел обновления образа
  в `SKILL.md` → «Обновить Redis 7 → Redis 8»).
- Если это единственная реплика (так бывает с шардированным редисом) — решение только
  одно: увеличивать диск. Если это кеш и данные не важны — можно выполнить чистку.

## Реплика не поднимается из-за битого AOF

Может возникнуть после внештатного отключения Redis или неожиданного окончания диска.

**Симптомы**: Redis пытается прочитать AOF, но он закоррапчен, не может подняться. В логах:

```
1:M 01 Jan 12:34:56.789 # AOF is not enabled, cannot fix the AOF file
1:M 01 Jan 12:34:56.789 # Please check the Redis documentation for instructions on how to repair the AOF file:
1:M 01 Jan 12:34:56.789 # https://redis.io/topics/persistence#append-only-file
1:M 01 Jan 12:34:56.789 #
1:M 01 Jan 12:34:56.789 # To fix the AOF file use:
1:M 01 Jan 12:34:56.789 # redis-check-aof --fix
```

**Решение**:

Если это реплика и с мастером всё в порядке — проще почистить диск, чтобы реплика
заново синхронизировалась.

Если это единственный мастер:

1. Делаем копии повреждённых файлов и сохраняем их к себе (через `mcc scp`) — всю папку
   `/mnt/appendonlydir`.
2. `systemctl stop redis`.
3. `redis-check-aof /mnt/redis/appendonlydir/appendonly.aof.manifest` — найти повреждённый
   файл (ждать окончания, чтобы понять какие файлы фиксить).
4. `redis-check-aof --fix /mnt/redis/appendonlydir/appendonly.aof.<номер>.incr.aof`.
5. `systemctl start redis`.
6. Понять вместе с пользователями, какие данные потеряны (fix делает обрезание до
   минимального консистентного состояния).

## Что-то другое

- Логи: `/mnt/logs/dbms/redis.log` или `/mnt/logs/dbms/redis-sentinel.log`.
- Графики — основные дашборды:
  - Replica backlog size
  - Total Memory Usage
  - Errors / sec
  - Total CPU Usage Main Thread
  - Connected clients

Записи рассказа Лёни о Redis: `video1820578341.mp4`. Команды из конца видео:
`redis-check-aof --fix appendonly.aof.manifest`, `redis-check-rdb`.
