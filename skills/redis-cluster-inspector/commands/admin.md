# Администрирование шардированного Redis Cluster

Детали по процедурам из `SKILL.md`.

## Скрипт изменения параметра конфига кластера без перезагрузки

Используется для применения параметра конфигурации в рантайме на всех хостах кластера
без рестарта сервиса. Скрипт нужно запускать из окружения, в котором установлен пакет
`redis`. Например, `/opt/redis-env-python/bin`.

```python
/opt/redis-env-python/bin/python3 - << 'PY'
import redis

USER='master'
PASSWORD='password'
PARAM='<param-name>'
VALUE='<param-value>'

conn = redis.Redis(username=USER, password=PASSWORD, decode_responses=True)
nodes = conn.cluster('nodes')
nodes = {k: v for k, v in nodes.items() if 'fail' not in v['flags'] and v['connected']}

for ip, node in nodes.items():
    r = redis.Redis(host=ip.rsplit(':', 1)[0], port='6379',
                    username=USER, password=PASSWORD, decode_responses=True)
    print(f"{ip} {node['node_id']}")

    try:
        r.execute_command('CONFIG', 'SET', PARAM, VALUE)
    except Exception as e:
        print(f"{node['node_id']}: {e}")

print(f"Done")
PY
```

Заполнить `PASSWORD` и подставить нужные `PARAM`/`VALUE`. Перед запуском обновить
параметр в PMS (`zen.redis.conf`), на всех инстансах сделать `confp --oneshot`.

## Скрипт выполнения forget на всех нодах кластера

Заполнить `password` и `node_id` ноды, которую нужно удалить. Скрипт нужно запускать из
окружения, в котором установлен пакет `redis`. Например, `/opt/redis-env-python/bin`.

```python
/opt/redis-env-python/bin/python3 - << 'PY'
import redis

USER='master'
PASSWORD='password'
NODE_TO_FORGET='node-id'

conn = redis.Redis(username=USER, password=PASSWORD, decode_responses=True)
nodes = conn.cluster('nodes')
nodes = {k: v for k, v in nodes.items() if 'fail' not in v['flags'] and v['connected']}

for ip, node in nodes.items():
    r = redis.Redis(host=ip.rsplit(':', 1)[0], port='6379',
                    username=USER, password=PASSWORD, decode_responses=True)
    print(f"{ip} {node['node_id']}")

    try:
        r.execute_command('CLUSTER', 'FORGET', NODE_TO_FORGET)
    except Exception as e:
        print(f"{node['node_id']}: {e}")

print(f"Done")
PY
```

## ERR Slot 10922 is already busy при создании кластера

Нужно сделать `FLUSHALL` и `CLUSTER RESET SOFT` на всех нодах, перезапустить операцию.
Перебрать хосты × ДЦ через скилл [`mcc-host-worker`](../../mcc-host-worker/SKILL.md)
(команда `sshexec`, см. `commands/sshexec.md` для шаблона перебора). Команды на хосте:

```
redis-cli --user master --pass "$(awk '$1=="user" && $2=="master" { gsub(/^[^>]*>/, "", $0); gsub(/ .*/, "", $0); print }' /etc/redis/acl/users.acl)" FLUSHALL
redis-cli --user master --pass "$(awk '$1=="user" && $2=="master" { gsub(/^[^>]*>/, "", $0); gsub(/ .*/, "", $0); print }' /etc/redis/acl/users.acl)" CLUSTER RESET SOFT
```

Шаблон хоста: `1.shard$i-db.video-api-stat-tkns-vkvideo-redis.$cloud.one-infra.ru`
(`i=1..3`, `clouds=("ic" "nc" "zc")`).

## Cluster sharded: не собирается кластер

Если не собирается кластер (шаг на backstage при создании кластера) — попробовать
перезапустить создание через пару секунд. Если не помогло:

1. Посмотреть `cluster nodes` **на всех хостах** (должны быть все ноды).
2. Если их нет — проверить **на всех хостах** `telnet {host_i} {port}` где `port = [6379, 16379]`.
3. Проверить, что есть доступ с хоста backstage по lan hostname port 6379 до всех нод redis.

## Удалить ноду в шардированном Redis (подробный алгоритм)

1. На ноде, которую удаляем — подключиться через скилл
   [`mcc-host-worker`](../../mcc-host-worker/SKILL.md) (`mcc ssh`), затем:
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
4. Выполнить `CLUSTER FORGET <node_id>` на всех остальных нодах (см. скрипт выше).
5. `cluster replicas <id shard master>` — убедиться, что реплики больше нет.
6. Удалить хост из таблицы `host_state`, и в таблице `cluster_links` из строки подключения.
7. Удалить хост из PMS `zen.redis.backupHosts` и поменять на другой (если включены
   бэкапы и он там есть), также удалить хост из `zen.redis.hosts`, далее `confp --oneshot`
   на новом хосте.
8. Сделать `withdraw` и удаление дисков хоста, который удалили.
9. Рестарт оператора для данного кластера (чтобы удалённый хост больше не обслуживался).

## Перебалансировка мастеров по ДЦ

По умолчанию при создании кластера Redis все мастера попадают в 1 ДЦ, иногда просят
распределить их равномерно. Отслеживать распределение по ДЦ можно в Мониторинге.

Перебрать хосты × ДЦ через скилл [`mcc-host-worker`](../../mcc-host-worker/SKILL.md)
(команда `sshexec`, см. `commands/sshexec.md` для шаблона перебора). Команда на хосте:
`redis-cli -c --user master -a <password> cluster failover`. Шаблон хоста:
`1.shard${i}-db.<queue>.<dc>.one-infra.ru`.

Где:
- `i` — номер шарда, перебор `for ((i=1; i <= S; i+=D))`, где `S` — количество шардов,
  `D` — количество ДЦ.
- `<queue>` — имя очереди в хосте.
- `<dc>` — ДЦ, в котором нужно назначить мастеров.
- `<password>` — пароль мастера (`cat /etc/redis/redis.conf | grep masterauth` на одном хосте).

## Включить модуль (RedisBloom, RedisJson, RedisGraph, RedisSearch, RedisTimeSeries)

1. В PMS поменять значения на `true` в соответствующих параметрах
   `mdb.redis.need<module>` (например `mdb.redis.needRedisJson`).
2. В `db_cluster_version` в json `redisParams` добавить `"need<module>: true"`
   (например `"needRedisJson": true`).
3. На каждом хосте: `confp --oneshot && systemctl restart redis`.

## Подключение по телепорту (cluster-preferred-endpoint-type hostname)

Нужно настроить кластер так, чтобы при редиректе он отдавал hostname вместо IP, чтобы
с этим hostname мог работать HAProxy.

1. `cat /etc/redis/redis.conf | grep masterauth` — пароль.
2. Выполнить скрипт «Скрипт изменения параметра конфига кластера без перезагрузки» с:
   - `PARAM = 'cluster-preferred-endpoint-type'`
   - `VALUE = 'hostname'`

Пользователю нужно подключаться по аналогии с Кафкой.

## Включить дуалстек (ipv4 + ipv6)

Если у клиента был v4-only redis, возможно нет дырки. Сначала добавить v6 в манифест,
попросить заказчика проверить доступ. Только тогда пересобирать кластер. На всякий
случай можно предложить клиенту настроить `cluster-preferred-endpoint-type hostname` —
тогда при редиректе нода будет возвращать имя хоста, а не IP.

⚠️ **`cluster-preferred-endpoint-type=hostname` настраивается через UI** (через
`cluster_to_template` + `zen.redis.conf` в PMS + update через UI с минимальным
изменением), а не через `config set` в рантайме. `config set` — только для
экстренного применения в рантайме, но без фиксации в шаблоне настройка слетит при
следующем `confp --oneshot && systemctl restart redis`. См. раздел
«Выставить параметр, которого нет в UI».

Сабмитим в манифесте v4, v6. Далее для каждого шарда:

⚠️ Операции проводим на репликах! Когда нужно будет работать с мастером — переключим его.

1. На реплике: `cluster forget <replica_id>`, на всякий случай на всём кластере.
2. Если нужно:
   - Добавить в PMS, в базе и на хосте `cluster-preferred-endpoint-type hostname`
     (через UI — см. предупреждение выше; `config set` — только в рантайме).
   - Проверить, что возвращает `config get cluster-announce-hostname`.
   - Если значение пустое — `config set cluster-announce-hostname <instance_name>`.
3. От этой же реплики: `cluster meet <master_ip_v6>`.
4. `cluster replicate <master_id>`.
5. Ожидать, когда нальётся. Сделать со всеми репликами по очереди. Когда дойдём до
   мастера — переключить его перед операцией.

⚠️ После `CLUSTER FORGET` ID ноды попадает в blacklist на ~60 сек, и gossip от неё
игнорируется остальными. Поэтому после `MEET v6 + REPLICATE` на реплике нужно
дополнительно сделать `CLUSTER MEET <replica_v6>` **с мастера** (и с остальных нод) —
иначе реплика будет реплицировать по v6, но мастер её в `cluster nodes` не увидит,
и failover на неё не произойдёт.

⚠️ Для обновления endpoint'а уже известной ноды на v6 (без изоляции) достаточно
`CLUSTER MEET <v6>` на остальных нодах — без `FORGET`. Это менее рискованно и
работает, когда нода уже в кластере, просто видна по v4. Например, после `failover`
бывший мастер остаётся видимым по v4 на других шардах — `CLUSTER MEET <v6>` на
каждой ноде обновляет endpoint на v6 без даунтайма.

⚠️ После `CLUSTER FAILOVER` бывший мастер автоматически становится slave'ом нового
мастера и подключается к нему по v6 (если v6-endpoint'ы уже были в `cluster nodes`).
Отдельный `FORGET + MEET + REPLICATE` для бывшего мастера не требуется — только
`CLUSTER MEET <v6_бывшего_мастера>` на остальных нодах для обновления endpoint'а.

`CLUSTER FAILOVER` отправляется **на реплику**, а не на мастер (на мастер вернёт
`ERR You should send CLUSTER FAILOVER to a replica`).

Если нужно постфактум прописать `cluster-preferred-endpoint-type hostname` на всём
кластере в рантайме — перебрать хосты × ДЦ через скилл
[`mcc-host-worker`](../../mcc-host-worker/SKILL.md) (`mcc sshexec`, см.
`commands/sshexec.md` для шаблона перебора). Это **две отдельные команды**, не одна
(в старой версии runbook была опечатка со слитной строкой):

```
redis-cli -c --user master -a <password> config set cluster-preferred-endpoint-type hostname
redis-cli -c --user master -a <password> config set cluster-announce-hostname $HOST
redis-cli -c --user master -a <password> config REWRITE
```

После применения в рантайме — обязательно зафиксировать в PMS (`zen.redis.conf`) и
в `cluster_to_template`, иначе слетит при следующем `confp --oneshot` или update.

Шаблон хоста: `1.shard${i}-db.<queue>.${dc}.one-infra.ru`. Поправить: `N` — количество
шардов; список ДЦ; `queue` — имя очереди; `password` —
`cat /etc/redis/redis.conf | grep masterauth`.

## ACL: детали

### Забанить команду

Если в случае инцидента какая-то команда (например `keys`) ест ресурсы кластера, её
можно экстренно отключить на уровне ACL пользователя:

1. Выполнить на одном хосте:
   ```bash
   cat /etc/redis/redis.conf | grep masterauth
   ```
2. На каждом хосте:
   ```
   redis-cli -c
   auth master <password>
   acl list
   # скопировать список прав пользователя – всё, что идёт после логина, включая ~* и &
   # добавить в этот список -<command> (e.g. -keys)
   # если есть упоминание этой команды с + впереди — удалить
   acl setuser <username> <new_acl_list>
   ```
3. Добавить изменения в Vault.

### Выдать default пользователя

Default-клиент позволяет подключиться к Redis без указания имени пользователя явно. По
умолчанию мы отключили ему права (`-@all`), чтобы стимулировать разграничение. Иногда
клиенты с legacy-кодом не могут поменять способ авторизации — им можно включить права
в рамках sup.

1. Обновить `permissions` у пользователя `default` в Vault. Список прав можно взять у
   других пользователей (кроме `master`).
2. Запустить таск на операторе `redis-cluster.upsert-user` с параметрами:
   - `vault_user_folder`: `zkv/mdb/<project_name>/redis/<full queue>/users/`
   - `vault_password_key`: `password`
   - `user_settings`: `{"username": "default"}`
3. Чтобы не заходить на каждый хост отдельно — обновить acl скриптом (см.
   «Скрипт изменения параметра конфига кластера без перезагрузки» — но для ACL).
   Либо на каждой ноде:
   ```
   cat /etc/redis/redis.conf | grep masterauth   # пароль от master
   redis-cli -c
   auth master <password>
   acl list                                      # посмотреть список пользователей
   acl setuser default <permissions>             # задать права пользователю, список прав из п.1
   ```
4. Выдать пользователю права от `default` (см. Vault) через one-secret.

### Выдать права пользователю для диагностики

Заблокированные команды: https://docs.vk.team/mdb/docs/redis/redis-acl.html. Например,
`OBJECT` может понадобиться для диагностики — можно открыть хотя бы на время.

1. В vault у пользователя (`permissions`) убрать `-OBJECT` (у нас `+@all` и запрещённое
   забирается явно).
2. Запустить таск на операторе (как в default-пользователе).
3. В БД в таблице `permissions` поменять колонку `permissions`.
4. Предупредить пользователя, чтобы через UI изменения не запускали — права слетят.
5. Подумать, можно ли в целом открыть эту команду для всех. Если да — добавить в
   разрешённые, поменять доку, пункт 4 можно не делать.

### Пользователь долго не добавляется/не изменяется

Чаще всего проблема в том, что добавили `@all` вместе с `+@all`, либо что-то с командами,
которые пробуют добавить. Лечение:

1. Поменять в vault поле `permissions`.
2. Перезапустить операцию (как описано в разделе «Выдать default пользователя»).
3. В БД тоже поменять, чтобы на UI отображались верные значения.

### Удалить пользователя

1. Удалить пользователя из Vault (`mdb/{project_name}/redis/{queue_name}/users/`).
2. В таблице `users` установить `is_deleted=true`.
3. Удалить пользователя на каждом хосте Redis:
   ```bash
   cat /etc/redis/redis.conf | grep masterauth   # пароль от master-пользователя
   redis-cli -c
   ```
   Затем:
   ```
   > --user master -a <password>
   > acl deluser <username>
   > acl list                                     # проверить до/после
   ```

## Параметры конфигурации

### Выставить параметр, которого нет в UI

1. Убедиться, что выставляемый параметр разумен.
2. Добавить параметр в шаблон: таблица `cluster_to_template` (поиск по `cluster_id`),
   `template_type = redis_cluster_config` — добавить в самый конец сразу с нужным
   значением.
3. Запустить update через UI с минимальным изменением параметров (например +1 байт в
   `maxMemory`). Либо запускать таски оператора на каждый шард (кластер шардированный).
4. Альтернативный вариант — запустить рестарт скриптом: перебрать хосты × ДЦ через
   скилл [`mcc-host-worker`](../../mcc-host-worker/SKILL.md) (`mcc sshexec`, см.
   `commands/sshexec.md` для шаблона перебора). Команда на хосте:
   `confp --oneshot; systemctl restart redis`. Шаблон хоста:
   `1.shard${i}-db.mdb-health-mdb-redis.${dc}.one-infra.ru` (`i=1..3`, `dc=hc,kc,pc`).
5. Если требуется скорость и параметр может быть применён без рестарта инстанса:
   - Добавить его в PMS (`zen.redis.conf`).
   - Зайти на все инстансы, сделать `confp --oneshot`.
   - Подключившись через `redis-cli`, выполнить `config set <имя> <значение>`.
   - Через `config get` проверить, что значение выставилось.

### Поменять значение isPersistent (выключить/включить персистентность)

1. В `db_cluster_version` (по `cluster_id`) в `cluster_params` → `redisParams` →
   `isPersistent`: `true|false`.
2. В PMS поменять значение поля `zen.redis.isPersistent host - <cluster name>-<project>-redis.clouds`.
3. Сделать то же самое что при выставлении параметра вне UI (п.3 в SKILL.md) — по сути
   рестарт всех хостов.

Если попросили поменять вид персистентности (с RDB на AOF): добавить в `zen.redis.conf`
в конец (и так же прописать в template):
```
appendonly yes
save ""
```
Если попросили поменять частоты RDB — добавить нужные значения в `zen.redis.conf`.
Например только раз в 12 часов:
```
save ""          # чтобы затереть уже существующие
save 1 43200     # изменение хотя бы одной записи за 12 часов (43200 секунд) → новый снапшот
```
Не забыть добавить в `cluster_to_template`, чтобы настройки не слетели при будущих
изменениях.

### Включить access-логи / повысить уровень логирования

Для того чтобы Redis писал логи подключений, нужно повысить уровень логирования с
`notice` до `verbose`. Алгоритм соответствует любому изменению конфига без перезагрузки
нод:

1. В PMS в параметре `zen.redis.conf` найти нужный кластер и добавить/поменять параметр:
   `loglevel verbose`.
2. Обновить конфиг на всех инстансах кластера — скриптом «Скрипт изменения параметра
   конфига кластера без перезагрузки» (выше в этом файле).

## Обновить Redis 7 → Redis 8

1. В конфиг в PMS `zen.redis.conf` добавить: `locale-collate "C"`.
2. Эту же строку добавить в `cluster_to_template`.
3. Обновить образ в манифесте сервиса на `ubuntu24-redis-cluster`. Взять последнюю
   версию из `db_version_dockers`.
4. В `db_cluster_version` обновить `dockerTag`, `dockerName`, `id` (из `db_version_dockers`).
5. На каждом хосте: `confp --oneshot && systemctl restart redis`.

## Миграция старых версий

Шаги по миграции redis-кластера с 6.2 версии на 7.0 версию — отдельная инструкция
(не охвачено). Миграция 7→8 — см. выше.

## Восстановление из бэкапа (Redis Cluster)

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

## Учения

Сценарий учений — см. `SKILL.md` → «Провести учения по отключению инстанса Redis».
