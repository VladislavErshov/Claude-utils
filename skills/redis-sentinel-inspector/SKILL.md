---
name: redis-sentinel-inspector
description: Инспекция и дежурство по Redis Sentinel-кластерами (mdb-data, cfs-redis) — known-peers, забытые хосты, спам "Failed to resolve hostname" в redis-sentinel.log, SENTINEL RESET, вечная переливка реплик, закончился диск, битый AOF, смена мастера, ACL-пользователи, миграция 7→8. Канон процедур — дежурная страница Confluence «Дежурство MDB: Redis» (Sentinel-секции); скилл хранит уникальные разборы (known-peers, разбор логов, SENTINEL RESET). Список хостов даёт пользователь (формат 1.db.<cluster>-cfs-redis.<dc>.one-infra.ru). Конфиги и логи читаются через скилл `mcc-host-worker` (`mcc scp`/`mcc sshexec`). Используй когда нужно проверить состояние Sentinel-кластера, найти зомби-хост, остановить переливку, починить битый AOF, поменять ACL, провести failover. Для шардированного Redis Cluster — см. скилл `redis-cluster-inspector`.
allowed-tools: [bash, read_file, write_file, edit_file]
---

# Скилл инспекции Redis Sentinel-кластеров

Скилл для дежурства по Redis Sentinel-кластерам, управляемым mdb-data. Replica set
Redis = 2 системы на каждом хосте — сам Redis как БД (порт 6379) и управляющая
система Sentinel (порт 26379). Sentinel решает кворумом делать ли смену мастера.

**Канон дежурной инструкции — Confluence «Дежурство MDB: Redis» (SSOT)**:
https://confluence.vk.team/pages/viewpage.action?pageId=1351178658
(Sentinel-секции: подключение, статус, runbook, «Частые запросы» — смена мастера,
Sentinel wrong info, старые ip/hostname дубликаты, ACL, параметр вне UI, isPersistent,
access-логи, 7→8 с sentinel-шагами, бэкап другого кластера, учения, вернуть ноду,
нода в loading). Вики живая — процедуры править там; здесь подключение/статус,
наши разборы и грабли.

⚠️ Скилл покрывает **только Sentinel** (replica set). Для **шардированного Redis Cluster**
используй скилл `redis-cluster-inspector`.

## Подключение

```bash
cat /etc/redis/acl/users.acl   # пароль: самый верхний, начинается с '>', иначе — vault
redis-cli -p 6379              # сама БД
auth master {password}
```

Для sentinel пароль и юзер те же, отличается только порт (26379):

```bash
redis-cli -p 26379
auth master {password}
```

Про пароль `default`-пользователя и vault — вики-секция «Подключение».

## Статус системы

**На БД (порт 6379):**

```
info          # кто мастер, жив или нет по мнению хоста, лаг репликации
acl list      # список юзеров
```

**На Sentinel (порт 26379):**

```
sentinel masters <masterName>     # один sentinel может управлять несколькими кластерами;
                                   # master = название кластера, у нас совпадает с названием в mdb
sentinel sentinels <masterName>   # список с состоянием остальных sentinels
sentinel slaves <masterName>      # список с состоянием всех реплик
sentinel reset *                  # удаляет всю информацию (пока не придёт heartbeat от остальных)
```

⚠️ `sentinel reset *` — нужно для удаления хоста при downscale. **Если делаете для всех
хостов — перед запуском на следующем хосте убедитесь, что после выполнения команды на
текущем хосте он успел подтянуть информацию после очистки.** Иначе можно везде удалить
разом и система будет в невалидном состоянии.

См. `commands/sentinel_reset.md` — как из `sentinel.conf` получить имя мастера и
sentinel-pass, сформировать команду `SENTINEL RESET`.
См. `commands/read_sentinel_logs.md` — как скачать и анализировать `redis-sentinel.log`.

## Известная проблема: забытые удалённые хосты в known-peers

**Симптом**: на всех оставшихся sentinel-хостах кластера в `redis-sentinel.log`
каждую секунду появляется строка:

```
<pid>:X <date> # Failed to resolve hostname '1.db.<cluster>-cfs-redis.<dc>.one-infra.ru'
```

**Причина**:
- При удалении redis-хоста через mdb-data флоу **не вызывает** `SENTINEL RESET <master>`
  на оставшихся sentinel-инстансах. Sentinel помнит удалённый хост в runtime-state
  (`known-sentinel` + `known-replica`) и продолжает раз в секунду пытаться его
  резолвить и пинговать.
- `+sdown` (субъективный down) — **не выкидывает** пир из конфигурации автоматически.
  Sentinel просто отмечает "не отвечающим", но не забывает. В итоге хост остаётся
  в known-peers навсегда.
- DNS-имя удалённого хоста перестаёт резолвиться → бесконечный спам в лог и
  нагрузка на sentinel (вплоть до `+tilt` mode при перегрузке).

**Хронология в логе** (пример для удалённого rc-хоста):
1. `<дата добавления>` — `+sentinel sentinel <myid> <rc-host> 26379 @ <master> <master-host> 6379`
2. `<дата добавления>` — `+slave slave <rc-host>:6379 <rc-host> 6379 @ <master> <master-host> 6379`
3. `<дата удаления>` — `+sdown sentinel <myid> <rc-host> 26379 @ ...`
4. `<дата удаления>` — `+sdown slave <rc-host>:6379 <rc-host> 6379 @ ...`
5. С тех пор каждую секунду: `Failed to resolve hostname '<rc-host>'`

**Лечение**: выполнить `SENTINEL RESET <master-name>` на **каждом** оставшемся
sentinel-хосте кластера. Команда сбрасывает known-slaves и known-sentinels для
указанного мастера; sentinel пере-обнаруживает только живых пиров. Удалённый хост
никто не анонсирует → он выпадает из state.

## Runbook для дежурного

**Канон — вики «Дежурство MDB: Redis», секция «Runbook для дежурного»** (ссылка вверху).
Что там есть: вечная переливка реплик (`repl-backlog-size`/`repl-timeout`/
`client-output-buffer-limit`, сеть OUT мастера / IN реплики), закончился диск, битый AOF
(копия `/mnt/appendonlydir`, `redis-check-aof --fix`), «что-то другое» (дашборды).
Краткая навигация — [commands/runbook.md](commands/runbook.md).

## Администрирование

**Канон — вики, Sentinel-секции «Частые запросы»** (ссылка вверху). Что там есть:
смена мастера (`sentinel failover`), Sentinel wrong info (баг Redis 8.0–8.4, MDBDEV-1418 —
правка `known-replica` в `/mnt/redis/senti/sentinel.conf` + `SENTINEL RESET`), старые
ip/hostname дубликаты, вернуть долго лежавшую ноду, нода в loading, ACL (забанить команду,
default-пользователь через оператор `redis-sentinel.upsert-user`, права для диагностики,
долгое добавление, удаление), параметр вне UI (`cluster_to_template`), isPersistent,
access-логи (`loglevel verbose`, скрипт CONFIG SET без перезагрузки), обновление 7→8
(включая sentinel-шаги с `/mnt/redis/senti/sentinel.conf`), бэкап другого кластера,
учения. Вики живая — править там.

⚠️ У SmsAPI с 1 шарда читается топология — его поломка может принести больший ущерб.

## Хосты и пути

| Что | Путь на хосте |
|---|---|
| Конфиги | `/etc/redis/` (sentinel.conf, redis.conf, acl/) |
| Логи | `/mnt/logs/dbms/` (redis.log, redis-sentinel.log, redis-server-systemd-service.log) |
| Sentinel state (dir) | `/mnt/redis/senti` (из `dir` в sentinel.conf) |
| AOF | `/mnt/redis/appendonlydir/` |
| RDB dump | `/mnt/redis/dump.rdb` |

⚠️ Путь именно `/mnt/logs/dbms` (с 's' в `logs`), не `/mnt/log/dbms`. Опечатка приводит
к ошибке скачивания логов.

## Работа с хостами

Подключение к хосту, выполнение команд и скачивание файлов — через скилл
[`mcc-host-worker`](../mcc-host-worker/SKILL.md). Шаблон хоста:
`1.db.<cluster>-cfs-redis.<dc>.one-infra.ru`.

## Структура скилла

- `SKILL.md` — этот файл: навигация, подключение/статус, уникальная проблема known-peers.
- `commands/read_sentinel_logs.md` — как скачать и анализировать `redis-sentinel.log`
  (формат строк, ключевые события, grep-шаблоны) — наш, в вики нет.
- `commands/sentinel_reset.md` — как сформировать команду `SENTINEL RESET`
  (master-name/sentinel-pass из sentinel.conf, persistence-файл) — наш, в вики нет.
- `commands/runbook.md` — навигация по вики-ранбуку.
- `commands/admin.md` — навигация по вики-администрированию + заметки.
