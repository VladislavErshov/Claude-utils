---
name: redis-sentinel-inspector
description: Инспекция и дежурство по Redis Sentinel-кластерам (mdb-data, cfs-redis) — known-peers, забытые хосты, спам "Failed to resolve hostname" в redis-sentinel.log, SENTINEL RESET, вечная переливка реплик, закончился диск, битый AOF, смена мастера, ACL-пользователи, миграция 7→8. Список хостов даёт пользователь (формат 1.db.<cluster>-cfs-redis.<dc>.one-infra.ru). Конфиги и логи читаются через mcc scp/sshexec. Используй когда нужно проверить состояние Sentinel-кластера, найти зомби-хост, остановить переливку, починить битый AOF, поменять ACL, провести failover. Для шардированного Redis Cluster — см. скилл `redis-cluster-inspector`.
allowed-tools: [bash, read_file, write_file, edit_file]
---

# Скилл инспекции Redis Sentinel-кластеров

Скилл для дежурства по Redis Sentinel-кластерам, управляемым mdb-data. Покрывает:
Replica set Redis = 2 системы на каждом хосте — сам Redis как БД (порт 6379) и управляющая
система Sentinel (порт 26379). Sentinel решает кворумом делать ли смену мастера.

## Что внутри скилла

1. **Подключение и статус** — как зайти на хост, взять пароль, какие команды смотреть.
2. **Известная проблема: забытые удалённые хосты в known-peers** — спам
   `Failed to resolve hostname` в `redis-sentinel.log`.
3. **Runbook для дежурного** — вечная переливка реплик, закончился диск, битый AOF.
4. **Администрирование** — смена мастера, забанить команду, ACL-пользователи, параметр
   вне UI, isPersistent, access-логи, миграция 7→8, восстановление из бэкапа, учения.

⚠️ Скилл покрывает **только Sentinel** (replica set). Для **шардированного Redis Cluster**
используй скилл `redis-cluster-inspector` — там свои команды (`cluster nodes`, `cluster
forget`, `cluster failover`, resharding, и т.д.).

## Подключение

Заходим на хост с базой и выполняем:

```bash
cat /etc/redis/acl/users.acl
```

Берём самый верхний пароль, если он начинается с `>`, иначе идём в vault.

```bash
redis-cli -p {port}
auth master {password}
```

Для sentinel пароль и юзер тот же, отличается только port (26379):

```bash
redis-cli -p 26379
auth master {password}
```

Если требуют пароль от `default` user — зайди в vault в папку `users` и у `default`
в поле `permissions` скопируй значение из любого юзера из той же папки (не права master).
Отдать пароль от `default`, после того как перезапустите все хосты redis (после перезапуска
хоста ожидаем `running` + `reserved`).

## Статус системы

**На БД (порт 6379):**

```
info          # кто мастер, жив или нет по мнению хоста, лаг репликации
acl list      # список юзеров
```

**На Sentinel (порт 26379):**

```
sentinel masters <masterName>     # один replica set из sentinel может управлять несколькими кластерами;
                                   # в рамках sentinel master = название кластера, у нас совпадает с названием в mdb
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

См. `commands/runbook.md` с подробностями. Кратко:

### Вечная переливка реплик

Наиболее частая проблема. **Симптомы**: долгое время CPU мастера > 100%, высокая
загрузка сети у реплики и мастера. В логах:
`Replication buffer limit has been reached (268435456 bytes), stopped buffering
replication stream. Further accumulation may occur on master side.`

**Быстрое решение**: поднять сеть на OUT у мастера, на IN у реплики, зайти через
`mcc ssh` на мастер и через `redis-cli` увеличить параметры репликации:

```
config set repl-backlog-size 2GB            # было 1mb — увеличить до 10-20% от maxmemory
config set repl-timeout 120
config set client-output-buffer-limit "replica 2GB 1GB 180"
```

После того как реплика нальётся и кластер стабилизируется — пересмотреть параметры и
обновить их через UI. Отслеживать в Мониторинге: Replication > Replica backlog size.

### Закончился диск

Возникает на старых кластерах, где логи писались вместе со снапшотами и AOF-файлами
на один диск, либо при неправильной конфигурации.

**Симптомы**:
- Реплика поднимается, видит рассинхрон, пытается скачать бэкап, но падает —
  заканчивается место (копятся `temp-%d.rdb`).
- Либо мастер делает перезапись AOF/снапшот и диск вдруг заканчивается.
- Либо AOF заполнила весь диск.

**Решение**: поднять диск. Если это реплика и с мастером всё в порядке — можно
почистить диск, но после требуется увеличение. Если старый образ без логов — обновить
по инструкции (см. раздел обновления образа в `commands/runbook.md`).

### Реплика не поднимается из-за битого AOF

Возникает после внештатного отключения Redis или неожиданного окончания диска.

**Симптомы**: Redis пытается прочитать AOF, но он закоррапчен, не может подняться.
В логах:
```
# AOF is not enabled, cannot fix the AOF file
# Please check the Redis documentation for instructions on how to repair the AOF file
# https://redis.io/topics/persistence#append-only-file
# To fix the AOF file use:
# redis-check-aof --fix
```

**Решение**: если это реплика и мастер в порядке — проще почистить диск, реплика
синхронизируется заново. Если это единственный мастер:

1. Сделать копии повреждённых файлов (через `mcc scp`) — всю папку `/mnt/appendonlydir`.
2. `systemctl stop redis`.
3. `redis-check-aof /mnt/redis/appendonlydir/appendonly.aof.manifest` — найти повреждённый файл.
4. `redis-check-aof --fix /mnt/redis/appendonlydir/appendonly.aof.<номер>.incr.aof`.
5. `systemctl start redis`.
6. Вместе с пользователями понять, какие данные потеряны (fix делает обрезание до
   минимального консистентного состояния).

### Что-то другое

- Логи: `/mnt/logs/dbms/redis.log` или `/mnt/logs/dbms/redis-sentinel.log`.
- Графики — основные дашборды:
  - Replica backlog size
  - Total Memory Usage
  - Errors / sec
  - Total CPU Usage Main Thread
  - Connected clients

## Администрирование

См. `commands/admin.md` с подробностями. Кратко:

### Смена мастера

```
sentinel failover <имя кластера как в MDB, без проекта>
```

### Sentinel wrong info: expected should know hosts but really known hosts

Известный баг в Redis 8.0–8.4 (MDBDEV-1418). В конфиге sentinel появляется лишняя
запись об одной из реплик, содержащая IP вместо имени хоста. Лечение:

1. На мастере: открыть `/mnt/redis/senti/sentinel.conf`, удалить лишние
   `sentinel known-replica` со строками, где IP. Если не хватает записей реплик с
   именем хоста — добавить. После редактирования: `systemctl restart redis-sentinel`.
2. На каждой реплике: `SENTINEL RESET <название кластера>` (порт 26379, auth master).

### Старые ip/hostname дубликаты в sentinel

Старые нешардированные redis раньше собирались по IP, а с какого-то docker-образа —
на hostname. В результате при обновлении у sentinel может быть дубликат одного и того
же хоста (один по IP, один по hostname) — мешает кворуму. Узнать: `sentinel slaves`.
Лечение: `SENTINEL RESET` на всех хостах **АККУРАТНО** (см. предупреждение выше).

### Забанить команду

Если в инциденте какая-то команда (например `keys`) ест ресурсы — отключить на уровне
ACL пользователя:

1. На одном хосте: `cat /etc/redis/redis.conf | grep masterauth` — получить пароль.
2. На каждом хосте:
   ```
   redis-cli
   auth master <password>
   acl list                       # скопировать список прав пользователя (всё после логина, включая ~* и &)
   # добавить в список -<command> (e.g. -keys); если есть +<command> — удалить
   acl setuser <username> <new_acl_list>
   ```
3. Добавить изменения в Vault.

### ACL: выдать default пользователя

Default-пользователя выдаём, когда клиент/приложение не может ходить не от default,
либо при переезде с большим числом приложений на default. Алгоритм:

1. Поставить `default` пользователю пароль от указываемого при создании пользователя
   (можно через https://enigma.bk.ru/).
2. Запустить таск на операторе `redis-sentinel.upsert-user` с параметрами:
   - `vault_user_folder`: `zkv/mdb/<project_name>/redis/<full queue>/users/`
     (пример: `zkv/mdb/mdbdev/redis/redis-cluster-nd1-mdbdev-redis.mdbdev.db.production.mdb.prod/users/` — у старых кластеров путь может отличаться, смотреть в админке)
   - `vault_password_key`: `password`
   - `user_settings`: `{"username": "default"}`
3. (Опционально) Зайти на хост через `mcc ssh`, `redis-cli`, `auth master <password>`,
   `acl list` — убедиться, что у `default` стоит то, что в vault в `permission`.

### ACL: выдать права пользователю для диагностики

Заблокированные команды: https://docs.vk.team/mdb/docs/redis/redis-acl.html. Например,
`OBJECT` может понадобиться для диагностики — можно открыть хотя бы на время.

1. В vault у пользователя (permissions) убрать `-OBJECT` (у нас `+@all` и запрещённое
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
   - `redis-cli` (для шардированного: `redis-cli -c`), затем `--user master -a <password>`.
   - `acl deluser <username>`.
   - Проверить: `acl list`.

### Выставить параметр, которого нет в UI

1. Убедиться, что параметр разумен.
2. Добавить в шаблон: таблица `cluster_to_template` (поиск по `cluster_id`),
   `template_type = redis_cluster_config` — добавить в конец сразу с нужным значением.
3. Запустить update через UI с минимальным изменением (например +1 байт в `maxMemory`),
   либо таски оператора на каждый шард (если кластер шардированный).
4. Альтернатива — рестарт скриптом:
   ```bash
   for ((i=1; i <= 3; i++)); do
     for dc in hc kc pc; do
       local HOST="1.shard${i}-db.mdb-health-mdb-redis.${dc}.one-infra.ru"
       echo "host $HOST"
       mcc sshexec "$HOST" "confp --oneshot; systemctl restart redis" --namespace infra
     done
   done
   ```
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

Дополнительно (если меняется вид персистентности RDB→AOF для sentinel-кластера):
добавить в `zen.redis.conf` в конец (и в template):
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
2. Обновить конфиг на всех инстансах — скрипт ниже.

**Скрипт изменения параметра конфига кластера без перезагрузки** — см.
`commands/admin.md` (раздел «Скрипт изменения параметра конфига кластера без
перезагрузки»).

### Обновить Redis 7 → Redis 8

1. В конфиг в PMS `zen.redis.conf` добавить: `locale-collate "C"`.
2. Эту же строку добавить в `cluster_to_template`.
3. Обновить образ в манифесте сервиса на `ubuntu24-redis-sentinel` (или
   `ubuntu24-redis-cluster` для шардированного). Взять последнюю версию из
   `db_version_dockers`.
4. Для Sentinel на каждом хосте:
   - Добавить `locale-collate "C"` в `/etc/redis/sentinel.conf`.
   - Добавить `locale-collate "C"` в `/mnt/redis/senti/sentinel.conf` в первую часть
     конфига, после `announce-ip`, но до комментария об автогенерируемых параметрах.
   - `systemctl restart redis-sentinel`.
5. В `db_cluster_version` обновить `dockerTag`, `dockerName`, `id` (из `db_version_dockers`).

### Поднять базу из бэкапа другого кластера

Например, пользователь хочет развернуть на staging данные прода.

1. Проверить, что конфигурации совместимы: RAM (включая запас на фрагментацию, буферы,
   CoW), место на диске под дамп.
2. Получить бэкап на каждом мастере кластера:
   ```bash
   mcc scp <source_master_host>:/mnt/redis/dump.rdb ./
   ```
3. Скопировать rdb на соответствующие мастера таргетного кластера:
   ```bash
   mcc scp ./dump.rdb <target_master_host>:/mnt/redis
   ```
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
| Конфиги | `/etc/redis/` (sentinel.conf, redis.conf, acl/) |
| Логи | `/mnt/logs/dbms/` (redis.log, redis-sentinel.log, redis-server-systemd-service.log) |
| Sentinel state (dir) | `/mnt/redis/senti` (из `dir` в sentinel.conf) |
| AOF | `/mnt/redis/appendonlydir/` |
| RDB dump | `/mnt/redis/dump.rdb` |

⚠️ Путь именно `/mnt/logs/dbms` (с 's' в `logs`), не `/mnt/log/dbms`. Опечатка приводит
к `failed to read downloaded archive header: EOF` при scp.

## mcc scp особенности

- **Скачивание директории**: `mcc scp "<host>:/etc/redis/" "<local_dir>/"` — работает,
  локальная директория назначения должна существовать (`mkdir -p` заранее).
- **Скачивание одиночного файла**: `mcc scp "<host>:/path/file" "<local_dir>/"` —
  локальный путь должен быть **директорией**, не путём к файлу. Иначе ошибка
  `failed to open destination directory`.
- **SSL Handshake is not finished** — туннель к minion-у не успел подняться. Просто
  повторить команду (бывает через 1-2 ретрая).
- **EOF на tar header** — файл не существует по указанному пути, либо опечатка в пути
  (например `/mnt/log/dbms` вместо `/mnt/logs/dbms`).

## Структура скилла

- `SKILL.md` — этот файл, общее описание и известные проблемы.
- `commands/read_sentinel_logs.md` — как скачать и анализировать `redis-sentinel.log`.
- `commands/sentinel_reset.md` — как сформировать команду `SENTINEL RESET`.
- `commands/runbook.md` — duty runbook: вечная переливка реплик, закончился диск,
  битый AOF, что-то другое. С подробностями и примерами команд.
- `commands/admin.md` — административные операции: скрипт изменения параметра конфига
  без перезагрузки, скрипт forget для ноды (для Cluster), детали по ACL-операциям.
