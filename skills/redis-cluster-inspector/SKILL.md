---
name: redis-cluster-inspector
description: Инспекция и дежурство по шардированным Redis Cluster-кластерам (mdb-data) — cluster nodes/myid/meet, ERR Slot 10922 is already busy, забытые/зачищенные ноды, 2 мастера в шарде, ОММ реплик, resharding, forget ноды, перебалансировка по ДЦ, модули, cluster-preferred-endpoint-type hostname (телепорт), dial tcp timeout, восстановление из бэкапа, вечная переливка реплик, битый AOF, ACL-пользователи, миграция 7→8. Список хостов даёт пользователь (формат 1.shardN-db.<cluster>-redis.<dc>.one-infra.ru). Конфиги и логи через скилл `mcc-host-worker` (`mcc scp`/`mcc sshexec`). Используй когда нужно проверить состояние Cluster, починить застрявшую миграцию слотов, удалить ноду, перебалансировать мастера, включить модуль, поднять базу из бэкапа. Для Sentinel (replica set) — см. скилл `redis-sentinel-inspector`.
allowed-tools: [bash, read_file, write_file, edit_file]
---

# Скилл инспекции шардированного Redis Cluster

Скилл для дежурства по шардированным Redis Cluster-кластерам, управляемым mdb-data.

## Шардированность Redis

Определить шардированный кластер можно:
- По значению «Шардированный» на странице кластера в mdb.
- По названию сервисов (у шардированного в названии есть название шарда:
  `1.shard1-db.<queue>.<dc>.one-infra.ru`).

Sharded redis имеет управляющий слой вместе с БД (в отличие от Sentinel-кластеров, где
Sentinel отделён на порт 26379). Управляющие команды — через `redis-cli -c` на порту 6379.

⚠️ Скилл покрывает **только шардированный Redis Cluster**. Для **Sentinel** (replica set)
используй скилл `redis-sentinel-inspector`.

## Что внутри скилла

1. **Подключение и статус** — `cluster nodes`, `cluster myid`, `cluster meet`, `info`.
2. **Runbook для дежурного** — вечная переливка реплик, закончился диск, битый AOF,
   зачистился диск на хосте шардированного редиса, 2 мастера в шарде, some nodes have
   disconnected node, ОММ реплик, dial tcp timeout.
3. **Администрирование** — ERR Slot 10922, смена мастера, решардинг, удаление ноды,
   перебалансировка мастеров по ДЦ, модули, cluster-preferred-endpoint-type hostname,
   ACL-пользователи, параметр вне UI, isPersistent, access-логи, миграция 7→8,
   восстановление из бэкапа, учения.

## Подключение

Заходим на хост с базой и выполняем:

```bash
cat /etc/redis/acl/users.acl
```

Берём самый верхний пароль, если он начинается с `>`, иначе идём в vault.

```bash
redis-cli -c
auth master {password}
```

Если требуют пароль от `default` user — зайди в vault в папку `users` и у `default`
в поле `permissions` скопируй значение из любого юзера из той же папки (не права master).
Отдать пароль от `default`, после того как перезапустите все хосты redis (после перезапуска
хоста ожидаем `running` + `reserved`).

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

См. `commands/runbook.md` с подробностями. Кратко:

### Вечная переливка реплик

Наиболее частая проблема. **Симптомы**: долгое время CPU мастера > 100%, высокая
загрузка сети у реплики и мастера. В логах:
`Replication buffer limit has been reached (268435456 bytes), stopped buffering
replication stream. Further accumulation may occur on master side.`

**Быстрое решение**: поднять сеть на OUT у мастера, на IN у реплики, зайти на мастер
через скилл [`mcc-host-worker`](../mcc-host-worker/SKILL.md) (`mcc ssh`) и через
`redis-cli` увеличить параметры репликации:

```
config set repl-backlog-size 2GB            # было 1mb — увеличить до 10-20% от maxmemory
config set repl-timeout 120
config set client-output-buffer-limit "replica 2GB 1GB 180"
```

После стабилизации — пересмотреть параметры и обновить через UI. Отслеживать в
Мониторинге: Replication > Replica backlog size.

### Закончился диск

**Симптомы**:
- Реплика поднимается, видит рассинхрон, пытается скачать бэкап, но падает —
  заканчивается место (копятся `temp-%d.rdb`).
- Либо мастер делает перезапись AOF/снапшот и диск вдруг заканчивается.
- Либо AOF заполнила весь диск.

**Решение**: поднять диск. Если это реплика и с мастером всё в порядке — можно
почистить диск, но после требуется увеличение. Если это единственная реплика (так
бывает с шардированным редисом) — решение только одно: увеличивать диск. Если это
кеш и данные не важны — выполнить чистку.

### Зачистился диск на хосте шардированного редиса

Обновить версию образа до 2.0.0+ (redis8) или 3.0.0+ (redis7), где решена эта проблема.

### Реплика не поднимается из-за битого AOF

Возникает после внештатного отключения Redis или неожиданного окончания диска.

**Симптомы**: Redis пытается прочитать AOF, но он закоррапчен, не может подняться.
В логах:
```
# AOF is not enabled, cannot fix the AOF file
# To fix the AOF file use: redis-check-aof --fix
```

**Решение**: если это реплика и мастер в порядке — проще почистить диск, реплика
синхронизируется заново. Если это единственный мастер:

1. Сделать копии повреждённых файлов (через скилл [`mcc-host-worker`](../mcc-host-worker/SKILL.md),
   команда `scp`) — всю папку `/mnt/appendonlydir`.
2. `systemctl stop redis`.
3. `redis-check-aof /mnt/redis/appendonlydir/appendonly.aof.manifest` — найти повреждённый файл.
4. `redis-check-aof --fix /mnt/redis/appendonlydir/appendonly.aof.<номер>.incr.aof`.
5. `systemctl start redis`.
6. Вместе с пользователями понять, какие данные потеряны (fix делает обрезание до
   минимального консистентного состояния).

### В шарде 2 мастера

Вероятно у одной ноды зачистился диск и она поднялась с пустым списком нод кластера.

1. Проверить у какого из мастеров состояние кластера `fail` (команда `cluster info`).
2. Выполнить действия из пункта «Зачистился диск на хосте шардированного редиса».

### Some nodes have disconnected node

Сообщение из оператора. Например:
```
Details: Some nodes have disconnected node:
1.shard1-db.ferryd-reco-redis.kc.one-infra.ru,
```

Происходит когда 1 из нод шардированного редиса долго была недоступна.

1. Зайти на ноду из сообщения, подключиться:
   ```bash
   cat /etc/redis/acl/users.acl
   # берём самый верхний пароль, если он начинается с '>', иначе идём в vault
   redis-cli
   auth master {password}
   cluster nodes
   # ищем ноду где в конце disconnected, например:
   # f29165f01624bacdad7634f85998d53486850171 :0@0 slave,fail,noaddr d6e222b97fc4c6765f5c98b66c9a72b89d5224dd 1757425226029 1757425221000 15 disconnected
   CLUSTER FORGET f29165f01624bacdad7634f85998d53486850171
   ```
2. Так на всех хостах из сообщения оператора. В `commands/admin.md` есть скрипт forget
   на всех нодах кластера.

### ОММ реплик при передергивании мастера / cluster is not ok

1. Посмотреть на графиках `Used Memory RSS`. Значение должно быть не более 10–20% от
   `maxMemory`.
2. Проверить параметр `replBacklogSize`. По умолчанию 1mb — очень низкое значение!
   Увеличивать до 10–20% от `maxmemory`. Если выставлено слишком большое — выставить до
   рекомендуемых.

### dial tcp timeout

При большой нагрузке не получается подключиться к Redis (пример: MDBSUP-2147 — массовые
"dial tcp timeout" при rps 250к).

Причин может быть много, но со стороны Redis можно увеличить лимиты на TCP-соединения:

1. В Redis увеличить параметр `tcp-backlog`, например до 65535. Требует рестарта redis —
   добавить в PMS, потом запустить рестарты хостов, либо `confp --oneshot && systemctl restart redis`.
2. Этот параметр ограничен настройкой ОС `net.core.somaxconn`. Проверить:
   `sysctl net.core.somaxconn`. При необходимости поменять значение в Env очередей
   кластера: `sysctl.net.core.somaxconn`.
3. Можно увеличить `net.ipv4.tcp_max_syn_backlog` до 65535. Аналогично добавить в Env
   очередей параметр `sysctl.net.ipv4.tcp_max_syn_backlog`. Проверить на хосте:
   `sysctl net.ipv4.tcp_max_syn_backlog`.

### Что-то другое

- Логи: `/mnt/logs/dbms/redis.log`.
- Графики — основные дашборды:
  - Replica backlog size
  - Total Memory Usage
  - Errors / sec
  - Total CPU Usage Main Thread
  - Connected clients

## Администрирование

См. `commands/admin.md` с подробностями. Кратко:

### ERR Slot 10922 is already busy при создании кластера

Сделать `FLUSHALL` и `CLUSTER RESET SOFT` на всех нодах, перезапустить операцию.
Перебрать хосты × ДЦ через скилл [`mcc-host-worker`](../mcc-host-worker/SKILL.md)
(команда `sshexec`, см. `commands/sshexec.md` для шаблона перебора). Команды на хосте:

```
redis-cli --user master --pass "$(awk '$1=="user" && $2=="master" { gsub(/^[^>]*>/, "", $0); gsub(/ .*/, "", $0); print }' /etc/redis/acl/users.acl)" FLUSHALL
redis-cli --user master --pass "$(awk '$1=="user" && $2=="master" { gsub(/^[^>]*>/, "", $0); gsub(/ .*/, "", $0); print }' /etc/redis/acl/users.acl)" CLUSTER RESET SOFT
```

Шаблон хоста: `1.shard$i-db.video-api-stat-tkns-vkvideo-redis.$cloud.one-infra.ru`
(`i=1..3`, `clouds=("ic" "nc" "zc")`).

### Cluster sharded: не собирается кластер

Если не собирается кластер (шаг на backstage при создании) — попробовать перезапустить
создание через пару секунд. Если не помогло:
- Посмотреть `cluster nodes` **на всех хостах** (должны быть все ноды).
- Если их нет — проверить **на всех хостах** `telnet {host_i} {port}` где `port = [6379, 16379]`.
- Проверить, что есть доступ с хоста backstage по lan hostname port 6379 до всех нод redis.

### Смена мастера

```
cluster failover
```

### Решардинг шардированного редиса

Запуск в UI (Wf `reshardRedisCluster`).

### Удалить ноду в шардированном Redis

1. На ноде, которую удаляем — подключиться через скилл
   [`mcc-host-worker`](../mcc-host-worker/SKILL.md) (`mcc ssh`), затем:
   ```
   redis-cli
   auth master <password>
   cluster myid          # скопировать полученный id
   ```
2. На реплике-хосте (мастере шарда) — так же через скилл mcc-host-worker:
   ```
   redis-cli
   auth master <password>
   cluster myid          # скопировать id
   cluster replicas <id shard master>    # убедиться, что id из шага 1 есть и является репликой текущего мастера
   ```
3. Остановить инстанс.
4. Выполнить `CLUSTER FORGET <node_id>` на всех остальных нодах (см. скрипт в
   `commands/admin.md`).
5. `cluster replicas <id shard master>` — убедиться, что реплики больше нет.
6. Удалить хост из таблицы `host_state`, и в таблице `cluster_links` из строки подключения.
7. Удалить хост из PMS `zen.redis.backupHosts` и поменять на другой (если включены
   бэкапы и он там есть), также удалить хост из `zen.redis.hosts`, далее `confp --oneshot`
   на новом хосте.
8. Сделать `withdraw` и удаление дисков хоста, который удалили.
9. Рестарт оператора для данного кластера (чтобы удалённый хост больше не обслуживался).

### Пользователи просят вернуть ноду, которая долго лежит

Обычно такое происходит, если minion, на котором поселен инстанс, долго остановлен.

Для Cluster: быть внимательным. Сначала проверить, что это не единственный инстанс в
шарде, чтобы не зачистить данные. Потом проверить, что кластер в стабильном состоянии.
Сделать зачистку диска и идти по инструкции возврата хоста в кластер (раздел
«Зачистился диск на хосте шардированного редиса»).

### Перебалансировать мастеров шардированного редиса по ДЦ

По умолчанию при создании кластера Redis все мастера попадают в 1 ДЦ, иногда просят
распределить их равномерно. Отслеживать распределение по ДЦ можно в Мониторинге.

Скриптом: перебрать хосты × ДЦ через скилл [`mcc-host-worker`](../mcc-host-worker/SKILL.md)
(команда `sshexec`, см. `commands/sshexec.md` для шаблона перебора). Команда на хосте:
`redis-cli -c --user master -a <password> cluster failover`. Шаблон хоста:
`1.shard${i}-db.<queue>.<dc>.one-infra.ru`.
Где:
- `i` — номер шарда, перебор `for ((i=1; i <= S; i+=D))`, где `S` — количество шардов,
  `D` — количество ДЦ.
- `<queue>` — имя очереди в хосте.
- `<dc>` — ДЦ, в котором нужно назначить мастеров.
- `<password>` — пароль мастера (`cat /etc/redis/redis.conf | grep masterauth` на одном хосте).

### Включить модуль (RedisBloom, RedisJson, RedisGraph, RedisSearch, RedisTimeSeries)

1. В PMS поменять значения на `true` в соответствующих параметрах
   `mdb.redis.need<module>` (например `mdb.redis.needRedisJson`).
2. В `db_cluster_version` в json `redisParams` добавить `"need<module>: true"`
   (например `"needRedisJson": true`).
3. На каждом хосте: `confp --oneshot && systemctl restart redis`.

### Пользователи хотят подключаться к Redis Cluster по телепорту

Нужно настроить кластер так, чтобы при редиректе он отдавал hostname вместо IP, чтобы
с этим hostname мог работать HAProxy.

1. `cat /etc/redis/redis.conf | grep masterauth` — пароль.
2. Выполнить скрипт (см. `commands/admin.md` → «Скрипт изменения параметра конфига
   кластера без перезагрузки») с:
   - `PARAM = 'cluster-preferred-endpoint-type'`
   - `VALUE = 'hostname'`

Пользователю нужно подключаться по аналогии с Кафкой.

### Включить дуалстек (ipv4 + ipv6)

Если у клиента был v4-only redis, возможно нет дырки. Сначала добавить v6 в манифест,
попросить заказчика проверить доступ. Только тогда пересобирать кластер. На всякий
случай можно предложить клиенту настроить `cluster-preferred-endpoint-type hostname` —
тогда при редиректе нода будет возвращать имя хоста, а не IP, что облегчает
переключение для клиентов.

Сабмитим в манифесте v4, v6. Далее для каждого шарда:

⚠️ Операции проводим на репликах! Когда нужно будет работать с мастером — переключим его.

1. На реплике: `cluster forget <replica_id>`, на всякий случай на всём кластере.
2. Если нужно:
   - Добавить в PMS, в базе и на хосте `cluster-preferred-endpoint-type hostname`
     (`config set cluster-preferred-endpoint-type hostname`).
   - Проверить, что возвращает `config get cluster-announce-hostname`.
   - Если значение пустое — `config set cluster-announce-hostname <instance_name>`.
3. От этой же реплики: `cluster meet <master_ip_v6>`.
4. `cluster replicate <master_id>`.
5. Ожидать, когда нальётся. Сделать со всеми репликами по очереди. Когда дойдём до
   мастера — переключить его перед операцией.

Если нужно постфактум прописать `cluster-preferred-endpoint-type hostname` на всём
кластере — перебрать хосты × ДЦ через скилл
[`mcc-host-worker`](../mcc-host-worker/SKILL.md) (`mcc sshexec`, см.
`commands/sshexec.md` для шаблона перебора). Команды на хосте:

```
redis-cli -c --user master -a <password> config set cluster-preferred-endpoint-type hostname cluster-announce-hostname $HOST
redis-cli -c --user master -a <password> config REWRITE
```

Шаблон хоста: `1.shard${i}-db.<queue>.${dc}.one-infra.ru`. Поправить: `N` —
количество шардов; список ДЦ; `queue` — имя очереди; `password` —
`cat /etc/redis/redis.conf | grep masterauth`.

### Забанить команду

Если в случае инцидента какая-то команда (например `keys`) ест ресурсы кластера, её
можно экстренно отключить на уровне ACL пользователя:

1. На одном хосте: `cat /etc/redis/redis.conf | grep masterauth` — получить пароль.
2. На каждом хосте:
   ```
   redis-cli -c
   auth master <password>
   acl list                       # скопировать список прав пользователя (всё после логина, включая ~* и &)
   # добавить в список -<command> (e.g. -keys); если есть +<command> — удалить
   acl setuser <username> <new_acl_list>
   ```
3. Добавить изменения в Vault.

### ACL: выдать default пользователя

Default-клиент позволяет подключиться без указания имени пользователя. По умолчанию мы
отключили ему права (`-@all`), чтобы стимулировать разграничение. Иногда клиенты с
legacy-кодом не могут поменять способ авторизации — им можно включить права в рамках sup.

1. Обновить `permissions` у пользователя `default` в Vault. Список прав можно взять у
   других пользователей (кроме `master`).
2. Запустить таск на операторе `redis-cluster.upsert-user` с параметрами:
   - `vault_user_folder`: `zkv/mdb/<project_name>/redis/<full queue>/users/`
     (пример: `zkv/mdb/mdbdev/redis/redis-cluster-nd1-mdbdev-redis.mdbdev.db.production.mdb.prod/users/` — у старых кластеров путь может отличаться, смотреть в админке)
   - `vault_password_key`: `password`
   - `user_settings`: `{"username": "default"}`
3. (Опционально) Зайти на хост через скилл mcc-host-worker, `redis-cli`, `auth master <password>`,
   `acl list` — убедиться, что у `default` стоит то, что в vault в `permission`.

Чтобы не заходить на каждый хост отдельно, можно обновить acl скриптом (см.
`commands/admin.md`). Выдать пользователю права от `default` (см. Vault) через one-secret.

### ACL: выдать права пользователю для диагностики

Заблокированные команды: https://docs.vk.team/mdb/docs/redis/redis-acl.html. Например,
`OBJECT` может понадобиться для диагностики — можно открыть хотя бы на время.

1. В vault у пользователя (`permissions`) убрать `-OBJECT` (у нас `+@all` и запрещённое
   забирается явно).
2. Запустить таск на операторе (как в default-пользователе).
3. В БД в таблице `permissions` поменять колонку `permissions`.
4. Предупредить пользователя, чтобы через UI изменения не запускали — права слетят.
5. Подумать, можно ли открыть команду для всех (добавить в разрешённые → менять доку).

### ACL: пользователь долго не добавляется/не изменяется

Чаще всего проблема в `@all` вместе с `+@all`, либо в командах которые пробуют добавить.
Поменять в vault поле `permissions`, перезапустить операцию (как в default-пользователе),
в БД тоже поменять, чтобы на UI отображались верные значения.

### ACL: удалить пользователя

1. Удалить из Vault (`mdb/{project_name}/redis/{queue_name}/users/`).
2. В таблице `users` установить `is_deleted=true`.
3. На каждом хосте Redis:
   - `cat /etc/redis/redis.conf | grep masterauth` — пароль master.
   - `redis-cli -c`, затем `--user master -a <password>`.
   - `acl deluser <username>`.
   - Проверить: `acl list`.

### Выставить параметр, которого нет в UI

1. Убедиться, что параметр разумен.
2. Добавить в шаблон: таблица `cluster_to_template` (поиск по `cluster_id`),
   `template_type = redis_cluster_config` — добавить в конец сразу с нужным значением.
3. Запустить update через UI с минимальным изменением (например +1 байт в `maxMemory`),
   либо таски оператора на каждый шард (кластер шардированный).
4. Альтернатива — рестарт скриптом: перебрать хосты × ДЦ через скилл
   [`mcc-host-worker`](../mcc-host-worker/SKILL.md) (`mcc sshexec`, см.
   `commands/sshexec.md` для шаблона перебора). Команда на хосте:
   `confp --oneshot; systemctl restart redis`. Шаблон хоста:
   `1.shard${i}-db.mdb-health-mdb-redis.${dc}.one-infra.ru` (`i=1..3`, `dc=hc,kc,pc`).
5. Если параметр применяется без рестарта — добавить в PMS (`zen.redis.conf`), на всех
   инстансах `confp --oneshot`, через `redis-cli`:
   ```
   config set <имя> <значение>
   config get <имя>       # проверить
   ```

### Поменять isPersistent (включить/выключить персистентность)

1. В `db_cluster_version` (по `cluster_id`) в `cluster_params` → `redisParams` →
   `isPersistent`: `true|false`.
2. В PMS поменять `zen.redis.isPersistent host - <cluster name>-<project>-redis.clouds`.
3. Сделать рестарт всех хостов (как в пункте «параметр вне UI», шаг 3).

Если меняется вид персистентности (RDB→AOF): добавить в `zen.redis.conf` в конец (и в
template):
```
appendonly yes
save ""
```
Если меняется частота RDB — добавить нужные значения в `zen.redis.conf`, например
только раз в 12 часов:
```
save ""          # затереть существующие
save 1 43200     # изменение хотя бы одной записи за 12 часов → новый снапшот
```
Не забыть добавить в `cluster_to_template`, чтобы настройки не слетели при будущих
изменениях.

### Включить access-логи / повысить уровень логирования / задать параметр в рантайме

Для access-логов повысить уровень с `notice` до `verbose`. Алгоритм общий для любого
изменения конфига без перезагрузки:

1. В PMS в `zen.redis.conf` найти нужный кластер, добавить/поменять параметр:
   `loglevel verbose`.
2. Обновить конфиг на всех инстансах — скриптом «Скрипт изменения параметра конфига
   кластера без перезагрузки» (см. `commands/admin.md`).

### Обновить Redis 7 → Redis 8

1. В конфиг в PMS `zen.redis.conf` добавить: `locale-collate "C"`.
2. Эту же строку добавить в `cluster_to_template`.
3. Обновить образ в манифесте сервиса на `ubuntu24-redis-cluster`. Взять последнюю
   версию из `db_version_dockers`.
4. В `db_cluster_version` обновить `dockerTag`, `dockerName`, `id` (из `db_version_dockers`).
5. На каждом хосте: `confp --oneshot && systemctl restart redis`.

### Восстановление из бэкапа (Redis Cluster)

На каждом шарде выполнить:

```bash
/bin/python3 /etc/backups/redis_cluster_restore_script.py save-args --timedate "2026-05-26_04:00:00" --shard shard3
cd /etc/backups && /bin/python3 redis_cluster_restore_script.py get-status

systemctl start stop-redis-before-restore.service
cd /etc/backups && /bin/python3 redis_cluster_restore_script.py get-status

systemctl start preparation-before-restore-redis.service
cd /etc/backups && /bin/python3 redis_cluster_restore_script.py get-status

systemctl start download-backup-redis.service
cd /etc/backups && /bin/python3 redis_cluster_restore_script.py get-status
ls -lh /mnt/redis/restore/

systemctl start unzip-backup-redis.service
cd /etc/backups && /bin/python3 redis_cluster_restore_script.py get-status
ls -lh /mnt/redis/

systemctl start start-redis-after-restore.service
cd /etc/backups && /bin/python3 redis_cluster_restore_script.py get-status

systemctl start cleanup-after-restore-redis.service
```

### Поднять базу из бэкапа другого кластера

Например, пользователь хочет развернуть на staging данные прода.

1. Проверить, что конфигурации совместимы: RAM (включая запас на фрагментацию, буферы,
   CoW), место на диске под дамп. Для шардированного кластера должно совпадать
   количество шардов и распределение слотов.
2. Получить бэкап на каждом мастере кластера — скачать `/mnt/redis/dump.rdb` через
   скилл [`mcc-host-worker`](../mcc-host-worker/SKILL.md) (`mcc scp`, см.
   `commands/scp.md` для шаблона массового скачивания).
3. Скопировать rdb на соответствующие мастера таргетного кластера — через скилл
   [`mcc-host-worker`](../mcc-host-worker/SKILL.md) (`mcc scp`, dest = `/mnt/redis`).
4. `systemctl restart redis` — при рестарте подтянется `/mnt/redis/dump.rdb`.

### Провести учения по отключению инстанса Redis

Сценарии:
1. **Штатное выключение мастера с graceful failover**: `restart` в облаке.
2. **Внештатное отключение мастера** — имитация резкого отключения по сетевым причинам.
   `systemctl stop` не подходит (вызовет graceful failover), `systemctl kill` сразу
   перезапустит сервис. Поэтому стопаем redis-процесс руками по pid:
   ```bash
   systemctl show -p MainPID --value redis
   kill -SIGSTOP <pid>
   # после теста:
   kill -SIGCONT <pid>
   ```
3. **Внештатное отключение с увеличенным таймаутом** — для наблюдения ошибок дольше.
   Параметр `cluster-node-timeout` — период, после которого реплики признают мастер
   недоступным и начинают выборы. Поднять на всём кластере скриптом (см. «Скрипт
   изменения параметра конфига кластера без перезагрузки»), выполнить отключение как
   в п.2, потом вернуть старое значение.

⚠️ У SmsAPI с 1 шарда читается топология — его поломка может принести больший ущерб.

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

Подключение к хосту, выполнение команд и скачивание файлов — через скилл
[`mcc-host-worker`](../mcc-host-worker/SKILL.md). Специфика Redis — выше по тексту скилла.

## Структура скилла

- `SKILL.md` — этот файл, общее описание и известные проблемы.
- `commands/runbook.md` — duty runbook с подробностями: вечная переливка реплик,
  закончился диск, битый AOF, зачистился диск, 2 мастера, some nodes disconnected, ОММ,
  dial tcp timeout.
- `commands/admin.md` — административные операции: скрипт изменения параметра конфига
  кластера без перезагрузки, скрипт forget для ноды, детали по ACL-операциям, миграция.
