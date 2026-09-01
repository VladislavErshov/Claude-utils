---
name: redis-cluster-inspector
description: Инспекция и дежурство по шардированным Redis Cluster-кластерам (mdb-data) — cluster nodes/myid/meet, ERR Slot 10922 is already busy, забытые/зачищенные ноды, 2 мастера в шарде, ОММ реплик, resharding, forget ноды, перебалансировка по ДЦ, модули, cluster-preferred-endpoint-type hostname (телепорт), dial tcp timeout, восстановление из бэкапа, вечная переливка реплик, битый AOF, ACL-пользователи, миграция 7→8. Канон процедур — дежурная страница Confluence «Дежурство MDB: Redis»; скилл хранит только специфику и дополнения. Список хостов даёт пользователь (формат 1.shardN-db.<cluster>-redis.<dc>.one-infra.ru). Конфиги и логи через скилл `mcc-host-worker` (`mcc scp`/`mcc sshexec`). Используй когда нужно проверить состояние Cluster, починить застрявшую миграцию слотов, удалить ноду, перебалансировать мастера, включить модуль, поднять базу из бэкапа. Для Sentinel (replica set) — см. скилл `redis-sentinel-inspector`.
allowed-tools: [bash, read_file, write_file, edit_file]
---

# Скилл инспекции шардированного Redis Cluster

Скилл для дежурства по шардированным Redis Cluster-кластерам, управляемым mdb-data.

**Канон дежурной инструкции — Confluence «Дежурство MDB: Redis» (SSOT)**:
https://confluence.vk.team/pages/viewpage.action?pageId=1351178658
(секции «Шардированность redis», «Подключение», «Статус системы», «Runbook для дежурного»,
«Частые запросы», восстановление из бэкапа). Вики живая — процедуры править там; в скилле
только подключение/статус, наши дополнения и грабли.

## Шардированность Redis

Определить шардированный кластер можно:
- По значению «Шардированный» на странице кластера в mdb.
- По названию сервисов (у шардированного в названии есть название шарда:
  `1.shard1-db.<queue>.<dc>.one-infra.ru`).

Sharded redis имеет управляющий слой вместе с БД (в отличие от Sentinel-кластеров, где
Sentinel отделён на порт 26379). Управляющие команды — через `redis-cli -c` на порту 6379.

⚠️ Скилл покрывает **только шардированный Redis Cluster**. Для **Sentinel** (replica set)
используй скилл `redis-sentinel-inspector`.

## Подключение

```bash
cat /etc/redis/acl/users.acl   # пароль: самый верхний, начинается с '>', иначе — vault
redis-cli -c
auth master {password}
```

Про пароль `default`-пользователя и vault — вики-секция «Подключение».

## Статус системы

**На БД (порт 6379, `redis-cli -c`):**

```
info                  # куча информации: кто мастер, лаг репликации, состояние хоста
acl list              # список юзеров
cluster nodes         # какие ноды есть + состояние, слоты
cluster myid          # id ноды, к которой подключились
cluster meet {ip} {port}    # добавить ноду по ip
cluster info          # состояние кластера (ok / fail)
cluster replicas <master_id>   # список реплик мастера
```

## Runbook для дежурного

**Канон — вики-секция «Runbook для дежурного»** (ссылка вверху). Что там есть:

- **Вечная переливка реплик** — самая частая проблема; `repl-backlog-size` (дефолт 1mb —
  поднимать до 10–20% от maxmemory), `repl-timeout`, `client-output-buffer-limit`;
  сеть OUT мастера / IN реплики.
- **Закончился диск** / **зачистился диск** (обновление образа до 2.0.0+ redis8 / 3.0.0+ redis7).
- **Битый AOF** — копия `/mnt/appendonlydir`, `redis-check-aof --fix`.
- **В шарде 2 мастера** — у какого из мастеров `cluster info` = fail → чинить по вики.
- **Some nodes have disconnected node** — `CLUSTER FORGET` нод с `disconnected`.
- **ОММ реплик** — `Used Memory RSS` ≤ 10–20% maxMemory, `replBacklogSize`.
- **dial tcp timeout** — `tcp-backlog` + sysctl `net.core.somaxconn` /
  `net.ipv4.tcp_max_syn_backlog` (Env очередей). Кейс: MDBSUP-2147 — массовые
  "dial tcp timeout" при rps 250к.
- Записи рассказа Лёни о Redis (видео + команды `redis-check-aof/rdb`).

**Типовой кейс MDBSUP**: после сфейлившегося change_primary/failover операция висит/падает с
«expected 1 MASTER, found 2» — чинить слоты/роль по вики-ранбуку, затем закрывать операцию
в прод-БД (скиллы `jira-mdbsup-solver` / `db-worker`). Разбор реального кейса —
[history/MDBSUP-4910](history/MDBSUP-4910-2026-08-27.md) (as-repuser: 2 мастера в 4 шардах
после add_hosts + баг failover-вейтера mdb-processing).

## Администрирование

**Канон — вики-секция «Частые запросы»** (ссылка вверху). Что там есть: ERR Slot 10922
(FLUSHALL + CLUSTER RESET SOFT), не собирается кластер, смена мастера (`cluster failover`),
решардинг (UI, Wf `reshardRedisCluster`), удалить ноду (полный алгоритм с host_state /
cluster_links / PMS `zen.redis.hosts`+`backupHosts` (механика PMS — скилл [`pms-worker`](../pms-worker/SKILL.md)) / withdraw / рестарт оператора), вернуть
долго лежавшую ноду, перебалансировка мастеров по ДЦ, модули (`mdb.redis.need<Module>`),
телепорт (`cluster-preferred-endpoint-type hostname`), ACL (забанить команду,
default-пользователь через оператор `redis-cluster.upsert-user`, права для диагностики,
долгое добавление/изменение, удаление), параметр вне UI (`cluster_to_template` +
`redis_cluster_config`), isPersistent, access-логи (`loglevel verbose`), обновление 7→8,
восстановление из бэкапа (`/etc/backups/redis_cluster_restore_script.py`, полный флоу
systemd-юнитов), бэкап другого кластера (dump.rdb через scp), учения, скрипты CONFIG SET
на всех нодах и CLUSTER FORGET.

Наши дополнения и грабли (дуалстек ipv6: blacklist `CLUSTER FORGET`, `CLUSTER MEET v6`,
FAILOVER на реплику; «две отдельные команды» для постфактум-hostname) —
[commands/admin.md](commands/admin.md).

## Хосты и пути

| Что | Путь на хосте |
|---|---|
| Конфиги | `/etc/redis/` (redis.conf, acl/) |
| Логи | `/mnt/logs/dbms/` (redis.log, redis-server-systemd-service.log) |
| AOF | `/mnt/redis/appendonlydir/` |
| RDB dump | `/mnt/redis/dump.rdb` |
| Backup script | `/etc/backups/redis_cluster_restore_script.py` |

⚠️ Путь именно `/mnt/logs/dbms` (с 's' в `logs`), не `/mnt/log/dbms`. Опечатка приводит
к ошибке скачивания логов.

## Работа с хостами

Подключение к хосту, выполнение команд, перебор хостов × ДЦ (`sshexec`) и скачивание
файлов (`scp`) — через скилл [`mcc-host-worker`](../mcc-host-worker/SKILL.md).
Шаблон хоста: `1.shard${i}-db.<queue>.<dc>.one-infra.ru`.

## Структура скилла

- `SKILL.md` — этот файл: навигация, подключение/статус, ссылки на вики, уникальные кейсы.
- `commands/runbook.md` — ссылка на вики-ранбук.
- `commands/admin.md` — наши дополнения к вики-администрированию (дуалстек ipv6,
  постфактум hostname).
