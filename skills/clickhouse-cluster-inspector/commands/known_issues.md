# Известные проблемы ClickHouse-кластеров

Подробный разбор симптомов, причин и фиксов. В `SKILL.md` только краткие ссылки сюда.

## Broken parts при старте CH-сервера

**Симптом**: `mdb-clickhouse-server` не стартует, в `clickhouse-server.err.log`:
```
Application: Caught exception while loading metadata: Code: 695. DB::Exception: Load job
'load table vk_video.vk_video_flat_recommends_local_v2' failed: Code: 231. DB::Exception:
Suspiciously many (1153 parts, 44.21 KiB in total) broken parts to remove while maximum
allowed broken parts count is 100.
```

ТОП1 проблема с кликами. CH при старте находит "сломанные" парты и отказывается их удалять,
если их слишком много — защитная мера.

**Единовременное лечение** — пропустить проверку, дать CH стартовать:
```bash
# На хосте БД (диск 1)
touch /var/lib/clickhouse/1/flags/force_restore_data
# Если гибрид (2 диска с данными) — то же для второго диска
touch /var/lib/clickhouse/2/flags/force_restore_data
systemctl restart mdb-clickhouse-server
```

**Постоянное лечение** — поднять лимиты в PMS, проперти `zen.clickhouse.additional_config.xml`
(не требует `cluster_to_template`):
```xml
<clickhouse>
    <merge_tree>
        <max_suspicious_broken_parts>2000</max_suspicious_broken_parts>
        <max_suspicious_broken_parts_bytes>1073741824</max_suspicious_broken_parts_bytes>
    </merge_tree>
</clickhouse>
```
Затем запустить обновление конфигов (см. `administration.md` → «Проставить настройку»).

## Чистим detached parts

Если detached-парты накопились — удалить через DDL (безопаснее, чем файлы):

```bash
clickhouse-client --user backup-admin --password '<pass>'
```
```sql
WITH ['broken','unexpected'] AS FILTER_DETACH_REASONS
SELECT
  concat('alter table ', database, '.', table, ' drop detached part ''',
          name, ''' settings allow_drop_detached=1;') AS drop
FROM (
  SELECT * REPLACE(part[1] AS partition_id,
                   toInt64(part[2]) AS min_block_number,
                   toInt64(part[3]) AS max_block_number),
         arrayFilter(x -> x NOT IN FILTER_DETACH_REASONS, splitByChar('_',name)) AS part
  FROM system.detached_parts
) A
INTO OUTFILE '/tmp/drop_detached_parts.sql'
FORMAT TabSeparatedRaw
```
Выходим из клиента, выполняем:
```bash
clickhouse-client -mn --ask-password --user backup-admin < /tmp/drop_detached_parts.sql
```

Если сервер совсем повис и DDL не работает — удалить файлы напрямую:
```bash
du -sh /var/lib/clickhouse/1/store/*/*/detached/    # ищем папки
# удалить
systemctl restart mdb-clickhouse-server
```

**Почистить весь кластер** (шаблон):
```bash
cluster_name="zen-events-log"
project="zinfra"
clouds=("rc" "pc")
shard_start=1
shard_end=15

for cloud in "${clouds[@]}"; do
    for ((i=shard_start; i<=shard_end; i++)); do
        instance="1.shard${i}-db.${cluster_name}-${project}-ch.${cloud}.idzn.ru"
        echo "Cleaning ${instance}"
        mcc sshexec -n dzen "$instance" "find /var/lib/clickhouse/1/store -path '*/detached/*' -delete"
    done
done
```

## Хост не поднялся после работ в облаке

**Симптом**: хост в UI `unknown`/`UNAVAILABLE`, `mcc ssh` возвращает:
```
*** ERROR (ServiceValidationException): Task Instance <host> is not scheduling on a minion,
please start it first
```

**Причина**: инстанс остановлен в облаке (плановые работы / перезагрузка / падение cloud-мастера).

**Фикс**: зайти в облако, нажать **start**. Чаще всего помогает.

Если хост в `RUNNING`, но `UNAVAILABLE` долгое время → смотреть логи CH/Keeper.

## Логи кликхауса — где искать

Логи могут лежать в трёх местах (зависит от возраста кластера):
1. `/mnt/logs/dbms/` — основной путь (современные кластеры)
   - `clickhouse-server.err.log` — **начинать с него**
   - `clickhouse-server.log` — если в err ничего нет
   - `clickhouse-keeper.log`, `.err.log`
2. `/one/logs/clickhouse/` — старые кластеры
3. `/var/log/clickhouse-server/` — совсем старые

## Не доступен Keeper (ZooKeeper)

**Симптомы**:
- В UI 2+/3 keeper-хостов `RUNNING UNAVAILABLE`.
- CH в `clickhouse-server.err.log`: `Code: 999. Coordination::Exception: Keeper server rejected
  the connection during the handshake. Possibly it's overloaded, doesn't see leader or stale`.
- `system.zookeeper_connection` пустая / ошибка.
- Запросы висят, копятся `TOO_MANY_SIMULTANEOUS_QUERIES` (Maximum: 1000).

**Причина**: нет Raft-кворума среди Keeper-нод. Подробнее про диагностику —
`../history/incident_2026_07_keeper_split.md`.

**Что проверить**:
1. `systemctl is-active mdb-clickhouse-keeper` на каждом кипере — процесс может быть active,
   но Raft-подсистема не активна (`server is not active yet` в логах).
2. `clickhouse-keeper-client -h 127.0.0.1 -p 2181 -q ruok` — должно вернуть `imok`.
3. В логе кипера `clickhouse-keeper.log`:
   - `Election timeout, initiate leader election` — кворум не собирается.
   - `failed to send prevote request: peer N (...:9444) is busy` — зависший Raft-state к peer N.
   - `[VOTE REQ] ... decision: X (deny)` — кандидат отстаёт по логу (`log term: req X / mine Y`, X<Y).
   - `KeeperTCPHandler: Ignoring user request, because the server is not active yet` — кипер
     отклоняет клиентские коннекты, т.к. сам ещё не выбрал лидера.

**Фикс (по дежурной доке)**:
- "Иногда бывает, что 2+/3 RUNNING UNAVAILABLE. В таком случае обычно помогает **рестарт всех**."
- Один из киперов остановлен в облаке → сначала поднять его в UI.
- Рестарт зависшего кипера: `systemctl restart mdb-clickhouse-keeper`.
- Если рестарт не помогает — дропнуть диск Keeper и рестарт (крайняя мера, Keeper поднимет
  лог с других нод). См. `../history/incident_2026_07_keeper_split.md`.

**Network check** (если есть подозрение на сеть, а не на Raft-state):
```bash
# С keeper-хоста:
getent hosts 1.keeper.<cluster>.<dc>.one-infra.ru           # DNS
ping -c 2 <peer-keeper-hostname>                             # L3
timeout 3 bash -c "echo > /dev/tcp/<peer>/<raft_port>"       # L4 к raft_port 9444
```
Если всё OK, а в логе `peer N is busy` — это **Raft-state**, не сеть.

## Реплика долго переналивается и запросы в неё таймаутят

Чтобы на реплику шло меньше запросов — поменять 2 параметра.

### 1. Настройка default-профиля пользователя

В PMS `zen.clickhouse.users.xml` в `<profiles><default>`:
```xml
<distributed_replica_error_half_life>60</distributed_replica_error_half_life>
<distributed_replica_error_cap>3</distributed_replica_error_cap>
```
Отвечают за счётчик ошибок реплики и период хранения ошибок.

### 2. Приоритет реплики

В PMS `zen.clickhouse.config.xml` у реплик по умолчанию приоритет не стоит (=1). Чем больше
`<priority>`, тем реже реплика выбирается для distributed-запросов.

Здоровым репликам поставить `1`, наливающейся — `10`:
```xml
<shard>
   <internal_replication>true</internal_replication>
   <replica>
      <host>1.shard1-db.<cluster>.hc.one-infra.ru</host>
      <port>9000</port>
      <priority>1</priority>
   </replica>
   <!-- исключаем реплику -->
   <replica>
      <host>1.shard1-db.<cluster>.kc.one-infra.ru</host>
      <port>9000</port>
      <priority>10</priority>
   </replica>
</shard>
```

### Релоад конфигов на всём кластере

```bash
cloud="rc"
for ((shardN=1; shardN<=22; shardN++)); do
  instance="1.shard${shardN}-db.<cluster>-${project}-ch.$cloud.one-infra.ru"
  echo "==== ==== ==== Updating $instance ==== ==== ===="
  mcc sshexec "$instance" --namespace infra "confp --oneshot; clickhouse-client --user backup-admin --password \$(grep -oP 'password:\s*\K[^ ]+' /etc/rscheck/checkclickhouse.conf) --query 'SYSTEM RELOAD CONFIG'"
done
```

## Part intersects previous part

https://clickhouse.com/docs/knowledgebase/part_intersects_previous_part

Если не получается решить по доке — удалить проблемный парт (скопировав для бэкапа):
```bash
mv /var/lib/clickhouse/1/store/37d/37dc1adb-61e4-4ebb-8b40-031f403d3a4a/19700101_1722287_100500_2 /tmp/clickhouse_parts_backup/
systemctl restart mdb-clickhouse-server
```
Дополнительно проверить, есть ли к нему путь в Keeper.

## Сломалась схема на реплике

**Симптомы**: в логе `DB::Exception: Table target.X_local does not exist`, табличек на реплике
нет, данные не реплицируются, реплика пустая.

**Причина**: повреждён диск, реплика запускается с пустого диска. После версии 1.6.0 —
встречается реже.

**Решение**:
1. Смотреть лог восстановления `/mnt/logs/system/restore_ch.log`.
2. Поправить ошибку в скрипте восстановления.
3. Если в логе `can't create table ... already exist` — удалить пути в Keeper:
   ```sql
   SYSTEM DROP REPLICA 'имя реплики' FROM ZKPATH 'путь указанный в ошибке'
   ```
   и запустить скрипт восстановления заново.

## TOO_MANY_SIMULTANEOUS_QUERIES — это симптом, не причина

`Code: 202. DB::Exception: Too many simultaneous queries. Maximum: 1000.` в
`clickhouse-server.err.log` почти всегда = следствие недоступности Keeper. Запросы,
которым нужны ZK-операции (большинство запросов к Replicated-таблицам), висят и копятся.

**Что делать**: не повышать лимит, а чинить корень — Keeper. Проверить `system.zookeeper_connection`,
логи Keeper. См. раздел «Не доступен Keeper» выше.

## Сброс пароля default пользователя

1. В vault поменять значения в секретах `default` и `default-password`.
2. В PMS `zen.clickhouse.users.xml` → `<users><default><password_sha256_hex>` проставить
   sha256 от пароля.
3. Перезапустить каждую реплику.
