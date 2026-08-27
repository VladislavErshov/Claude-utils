---
name: mongo-cluster-inspector
description: Инспекция и дежурство по MongoDB-кластерам (mdb-data, MongoDB 6.0/7.0/8.0, replicaSet + шардированные) — подключение под admin, rs.status()/printSecondaryReplicationInfo, смена мастера, add/remove нод (руками и через оператор mongodb.upscale/downscale-instances), починка отставшей ноды (oplog window, initial sync), восстановление из бэкапа (mongo_push_backup_script.py, 7z + mongorestore --oplogReplay, rscfg без oplog), обновление 6→7→8 (setFeatureCompatibilityVersion, storage.journal.enabled), восстановление пользователя admin через __system, ipv6, мирроринг/переезд в MDB, BSONObjectTooLarge (Datatransfer INCALL-24091), диск 95%+ после удаления данных (compact, MDBSUP-4902). Список хостов даёт пользователь (формат 1.db.<cluster>-mongo.<dc>.one-infra.ru / hidden-хосты). Работа с хостами — через скилл `mcc-host-worker` (`mcc ssh`/`sshexec`/`scp`), конфиги — PMS (zen.mongodb.hosts, zen.mongodb.conf). Источник — дежурная документация https://confluence.vk.team/pages/viewpage.action?pageId=1348619045.
allowed-tools: [bash, read_file, write_file, edit_file]
---

# Скилл инспекции MongoDB-кластеров

Скилл для дежурства по MongoDB-кластерам, управляемым mdb-data.
Источник — страница дежурства «Дежурство MDB: MongoDB»
(Confluence pageId=1348619045, v31) + тикеты MDBSUP.

⚠️ **Все операции с mcc** (`ssh` / `sshexec` / `scp` / `instances` / `status`,
промпт `/# `, `-n infra`, `--local` и грабли expect/Tcl) — брать из скилла
[`mcc-host-worker`](../mcc-host-worker/SKILL.md). Этот скилл НЕ дублирует
mcc-паттерны, только специфику MongoDB.

## Архитектура

- Кластер — replicaSet (или шардированный: mongos + rscfg-конфигсервер + шарды).
- В каждом replicaset есть hidden-инстанс (с него снимаются бэкапы, на него ходим по ssh
  дляrestore-операций).
- Конфиги — PMS: `zen.mongodb.hosts` (список хостов кластера — **порядок важен**, первый
  хост стартует как инициализатор репликасета), `zen.mongodb.conf` (конфиг mongod).
- Пароли: PMS или `/etc/backups/mongo_config.ini` (`backup_mongo_username`,
  `backup_mongo_password`, `archive_password`), admin-пароль — vault.
- Таблица шаблонов конфигов в БД: `cluster_to_template`.

## Подключение

```bash
# через mcc ssh на контейнер (см. скилл mcc-host-worker), затем:
mongosh --username admin
use admin

# либо напрямую без ssh (если есть сетевая связанность):
mongosh <fqdn>:27017/admin --username admin
```

⚠️ Частая проблема пользователей: подключаются с
`--authenticationDatabase <своя бд>` — не работает, нужно
`--authenticationDatabase admin`.

## Полезные команды

```
rs.status()                      # статус replicaSet
rs.printSecondaryReplicationInfo()   # replication lag
db.getUsers()                    # список юзеров
db.serverStatus()                # статус сервера (db.serverStatus().connections)
```

## Runbook для дежурного

### Диск 95%+ после удаления данных (не освободилось место) — MDBSUP-4902

**Симптомы**: пользователи удалили данные, но место на дисках не освободилось
(WiredTiger не возвращает пространство ОС без compact).

**Решение**: запустить `compact` по коллекциям на хостах кластера:
на secondary — напрямую, на primary — `db.runCommand({compact: "<collection>", force: true})`
(compact на первичном требует force и блокирует операции БД коллекции — делать в окно
низкой нагрузки, по одной ноде, дожидаясь возврата в SECONDARY).

⚠️ В доке дежурства (Confluence 1348619045) про compact **ничего нет** — этот раздел
добавлен по тикету MDBSUP-4902 (кластер coordinator-prod-tap-mongo, uuid
5758447a-49f7-4f70-9a18-d2fec9308b3f, MongoDB 6.0). Альтернатива для secondary —
пересинхронизация ноды (см. «Починить отставшую ноду»): initial sync пишет данные
заново без «дырок».

**Опыт MDBSUP-4902 (27.08.2026)**:
- compact на PRIMARY `coordinator.ContainerEntity` (9.3G storage + 13.2G indexes при
  5.3G данных) отработал за 63 сек, **освободил 19.6G**, но спровоцировал failover
  (election на secondary) — компакт на первичном держит локи, реплика отстаёт.
- Признак «дыр»: на одной ноде файлы в разы больше, чем на других (у соседей
  compact-состояние после пересинка). Индексы раздуты сильнее коллекции.
- `compact` (4.4+) пересобирает и коллекцию, и её индексы — один runCommand на
  коллекцию достаточен.
- Проверить реальный размер данных: `db.getCollection(...).stats()` → `size` (данные)
  vs `storageSize` + `totalIndexSize` (на диске).

### Починить отставшую ноду

**Симптомы**: `rs.printSecondaryReplicationInfo()` — момент времени не двигается,
отставание растёт, статус скорее всего RECOVERING.

**Почему**: вышли за пределы oplog окна, реплика не может дотянуть WAL.

**Решение**:
1. Зайти в PMS (`zen.mongodb.hosts`), проверить не стоит ли текущая реплика **первой**
   в списке — если да, убрать в конец (иначе при обнулении она стартует как ещё один
   репликасет).
2. Подключиться по ssh:
   ```
   systemctl stop mongod
   rm -rf /mnt/mongodb/* && rm /mnt/logs/dbms/*
   ```
3. Если правили PMS — рестарт контейнера через интерфейс облака (попутно чинит
   мониторинг), если нет — `systemctl start mongod`.
4. Дождаться синхронизации: `rs.status()` покажет ноду в STARTUP2 (это ок —
   восстановление; после прекращения роста диска ещё строит индексы). В логах
   `/mnt/logs/dbms/mongod-service.log` — `trying to connect...` без ошибок.
5. Если PRIMARY говорит что реплика недоступна — проверить сетевую доступность
   `telnet <replica-host> 27017` с PRIMARY. Часто нет доступа в одну из сторон —
   тогда мигрировать реплику или разбираться с доступами.

**Грабли (опыт MDBSUP-4902)**:
- `systemctl stop mongod` на mongo-хосте останавливает **весь контейнер** (mongod —
  главный процесс): после wipe `systemctl start mongod` недоступен
  («cannot exec into container that is not running»). Тогда стартовать сервис через
  облако: `mcc --local -n infra -c <dc> start "<service>"` (state FINISHED/STOPPING →
  DEPLOYING → RUNNING; скилл mcc-host-worker → commands/lifecycle.md).
- Реальный лог mongod — `/mnt/logs/dbms/mongodb.log` (не mongod-service.log);
  ротация: mongodb.log.1/.gz. Маркеры: `"Compact begin/end"`, `"freedBytes"`.
- PMS `zen.mongodb.hosts` может не содержать hidden-хост (только 3 db-ноды) —
  проверка «первая в списке» для hidden тривиально проходит.

### Сломалась поставка данных в Datatransfer (BSONObjectTooLarge) — INCALL-24091

**Что случилось**: курсор Datatransfer генерировал документы >16MB (слепок предыдущего
состояния + новое; документ 8+MB переходит лимит), база отказывалась их выдавать.

**Детект**: рост числа ассертов `BSONObjectTooLarge` на основном дашборде mongodb; в
логах спам:
```json
{"s":"E","c":"ASSERT","id":23077,"msg":"Assertion","attr":{"error":"BSONObjectTooLarge: BSONObj size: ... is invalid. Size must be between 0 and 16793600(16MB) ..."}}
```

**Лечение**:
1. Узнать могут ли пользователи пропустить обновление одного документа (обработать
   потом руками). Убедиться что есть дежурный из DT.
2. Рядом с ошибкой в логах — сообщение `Aggregate command executor error` с полем
   `cmd` (параметры курсора). Из него: `$db` — база, `pipeline` с `$changeStream` и
   `resumeAfter`.
3. Зайти в mongosh, выбрать нужную базу, создать такой же курсор **без**
   `fullDocument: "updateLookup"`:
   ```js
   db.watch([], { resumeAfter: { _data: "<_data из лога>" } })
   ```
   Вызвать `next()` пару раз — получить корректный следующий `resumeAfter`, передать
   дежурному DT.

**Полезно**: продвинутые пользователи могут сделать это сами (есть доступ к логам и
курсорам) — проконсультировать. Сломанных курсоров может быть несколько — повторить
для каждого.

## Администрирование

### Сменить мастера

```js
// на случайного:
rs.stepDown()

// на конкретного:
var cfg = rs.conf();
cfg.members[<index>].priority = <наибольший приоритет>;
rs.reconfig(cfg);
```

### Удалить ноду

Руками:
1. Удалить из PMS, проперти `zen.mongodb.hosts`.
2. Удалить из конфига:
   ```js
   var cfg = rs.conf();
   cfg.members.splice(<index>, 1);
   rs.reconfig(cfg, { force: true });
   ```

Через оператор:
```bash
mcc op_start --address <operator> queue://<queue-name> mongodb.downscale-instances -- --cloud <облако> --replicas <кол-во реплик после downscale>
```

### Добавить ноду

Руками:
1. Добавить ноду в PMS, проперти `zen.mongodb.hosts`.
2. Если инстанс ещё не поднят в облаке — просто поднять, сам добавится в конфиг.
   Если уже поднят где-то:
   ```js
   var cfg = rs.conf();
   cfg.members.push({
       _id: <new-id>,   // на 1 больше текущего максимального, смотреть rs.status()
       host: "<host>:<port>"
   });
   rs.reconfig(cfg);
   ```

Через оператор:
```bash
mcc op_start --address <operator> queue://<queue-name> mongodb.upscale-instances -- --cloud <облако> --replicas <кол-во реплик после upscale>
```

### Добавить пользователя в шардированную монгу

Заходим на хост mongos, логинимся в mongosh:
```js
use stories   // база, в которой создаём пользователя
db.createUser({
  user: "stories-core-admin",
  pwd: "<password>",
  roles: [{ role: "dbAdmin", db: "stories" }]
})
```
Пароль положить в vault.

### Хотят сделать sh.shardCollection (нужен clusterManager)

```js
use admin
db.grantRolesToUser("<user>", [{ role: "clusterManager", db: "admin" }])
```

### Узнать с каких ip подключения

Подключиться к нужному хосту от admin:
```js
db.adminCommand({
  aggregate: 1,
  pipeline: [
    { $currentOp: { allUsers: true, idleConnections: true } },
    { $match: { client: { $exists: true } } },
    { $project: { ip: { $trim: { input: { $cond: [
        { $regexMatch: { input: "$client", regex: /^\[.*\]/ } },          // IPv6
        { $arrayElemAt: [ { $split: ["$client", "]"] }, 0 ] },            // всё внутри []
        { $arrayElemAt: [ { $split: ["$client", ":"] }, 0 ] }             // IPv4
    ] }, chars: "[]" } } } },
    { $group: { _id: "$ip", connections: { $sum: 1 } } },
    { $sort: { connections: -1 } }
  ],
  cursor: {}
})
```

### Пользователь не может указать число соединений больше разрешенного в пресете

1. Удостовериться, что пользователю действительно нужно поднять соединения без
   достаточных аппаратных ресурсов.
2. Временно поднять лимит для пресета: таблица `hardware_presets`, поле
   `databasePreset` → `mongodbPreset` → `maxValues`.
3. Провести операцию.
4. Опустить лимит обратно, если он слишком завышенный для данного пресета.

### Восстановление из резервной копии

⚠️ В шардированном кластере бэкап репликасета с конфигами (хост содержит `rscfg`)
снимается **без `--oplog`** и восстанавливается **без `--oplogReplay`** (с rscfg не
получается снять дамп с --oplog — ругается на resharding).

1. В интерфейсе MDB проверить, что с кластером нет проблем — все хосты available.
2. Зайти на hidden-инстанс нужного replicaset по ssh.
3. (Опционально) резервная копия текущего состояния:
   ```bash
   python3 /etc/backups/mongo_push_backup_script.py manual-backup
   tail -f /mnt/logs/system/mongo-push-backup.log   # ждать "Move ... to saved"
   ```
4. Рабочая директория (тут больше всего места):
   ```bash
   mkdir /mnt/backup/tmp && cd /mnt/backup/tmp
   ```
5. Выбрать в интерфейсе MDB нужную резервную копию, скачать: `wget https://...`
6. Пароль архива — из PMS или `/etc/backups/mongo_config.ini` (параметр
   `archive_password`). Распаковать `unzip`. На выходе: 7z-файл (новый формат,
   с мая 2025) или директория с файлами (старый формат, требует больше места —
   файлы без сжатия).
7. Найти в интерфейсе MDB мастера текущего replicaset — **все дальнейшие
   подключения на этот хост** (не на hidden).
8. Удалить БД пользователя (пользователя найти в PMS, БД — в интерфейсе MDB):
   ```bash
   mongosh "mongodb://user:password@host:port/database_name" --eval "db.dropDatabase()"
   ```
9. Восстановить с hidden-инстанса на primary (юзер/пароль из PMS или
   `mongo_config.ini`: `backup_mongo_username` / `backup_mongo_password`):
   ```bash
   # 7z-формат:
   7z x -so backup.archive.7z | mongorestore -h <primary-host> -u <user> -p <password> --archive --oplogReplay
   # файловый формат:
   mongorestore -h <primary-host> -u <user> -p <password> ./path/to/backup
   ```

### Обновление версии mongodb 6 → 8

Поэтапно: 6→7, потом 7→8. При 6→7 удалить параметр `storage.journal.enabled: true`
из PMS `zen.mongodb.conf` (и в `db.*`, и в `hidden.*`), не забыть удалить из
`cluster_to_template`.

Выполнение команд mongosh на хостах — через `mcc sshexec` (паттерны и грабли —
скилл [`mcc-host-worker`](../mcc-host-worker/SKILL.md)), шаблон:
`mcc sshexec -n infra <host> "mongosh --username admin --password <password> --authenticationDatabase admin --eval '<eval-команда>'"`.

1. Установить featureCompatibilityVersion на primary:
   `db.adminCommand({ setFeatureCompatibilityVersion: "6.0" })`
2. Проверить на каждом хосте:
   `db.adminCommand({ getParameter: 1, featureCompatibilityVersion: 1 })`
3. Обновить версию образа в манифесте one-cloud: secondary → secondary → hidden по
   очереди (после каждого ждать, что реплика вернулась в SECONDARY).
4. Failover: `rs.stepDown()`
5. Обновить образ бывшего мастера.
6. Вернуть совместимость:
   `db.adminCommand({ setFeatureCompatibilityVersion: "7.0", confirm: true })`
7. Для 7→8 — те же шаги, `setFeatureCompatibilityVersion` последовательно `"7.0"`
   (confirm: true), затем `"8.0"` (confirm: true).

### Включить ipv6

Обычный rolling restart + failover. Не забыть обновить `cluster_to_template`.

1. Изменение в манифесте one-cloud: secondary → secondary → hidden по очереди
   (ждать SECONDARY после каждого).
2. Failover (`rs.stepDown()`).
3. Изменение в манифесте для бывшего мастера.

### Восстановить пользователя admin на кластере

Если admin был перезаписан или удалён:

```bash
# по ssh на primary host
mongosh "mongodb://localhost:27017/?authSource=local" -u __system -p "$(cat /var/lib/mongo/secret.key)"

# обновление:
db.updateUser("admin", { pwd: "<admin-password-from-vault>", roles: [{ role: "root", db: "admin" }] })
# или создание:
db.createUser({ user: "admin", pwd: "<admin-password-from-vault>", roles: [{ role: "root", db: "admin" }] })
```

Выйти, подключиться обычным способом, проверить права (`show users`).

### Пользователи хотят проверить выбивание инстанса из кластера

```bash
# на primary/secondary:
sudo systemctl kill -s SIGKILL mongod.service
# после эксперимента не забыть восстановить:
sudo systemctl start mongod.service
```

### Переезд в MDB: мирроринг

1. **Совместимость версий** — версия внешнего кластера соответствует поддерживаемым.
2. **Сетевые доступы** — в обе стороны: приложение→база и наши ноды↔внешний кластер,
   порт 27017.
3. **secret.key** — подменить у нас `/var/lib/mongo/secret.key` (в Vault) на
   `security.keyFile` внешнего кластера (см. mongod.conf).
4. **Имя репликасета** — сверить с внешним, поправить в `cluster_to_template`.
5. **Подготовка инстансов** — на каждом выставить переменную `skip_mongo_init`
   (не инициировать собственный репликасет) и очистить volumes (или минимум
   `/mnt/mongodb`).
6. **Пользователи** — init-скрипт их не создаст: взять `/etc/mongo_init/script.js`
   и выполнить создание руками.
7. **Подключение нод по одной**: стартуем ноду → на внешнем кластере `rs.add(...)`
   → нода в STARTUP, initial sync. Hidden-ноду добавлять как `hidden: true,
   priority: 0`.
8. **Переезд**: следим за нагрузкой, по одной выключаем ноды внешнего кластера и
   `rs.remove(...)`. Контролируем ресурсы и параметры.

### Восстановление из бэкапа (внешний кластер → наш)

Бэкап с внешнего кластера:
```bash
mkdir /mnt/mongodb/backups/ && cd /mnt/mongodb/backups/
nohup mongodump --host <outer-host> --port <outer-port> --username <backup-username> --password '<pass>' --authenticationDatabase admin --db <db-name> --gzip --out /mnt/mongodb/backups/ > ./backup.log 2>&1 &
```
Наливка:
```bash
nohup mongorestore --host localhost --port 27017 --username admin --password '<admin-pass>' --authenticationDatabase admin --numParallelCollections 4 --drop --gzip --db <db-name> ./<db-name> ./restore.log 2>&1 &

# в репликасет:
nohup mongorestore --uri="mongodb://admin:<admin-user>@host1:27017,host2:27017,host3:27017/<database-name>?authSource=admin" --writeConcern='{w: "majority"}' --drop --gzip ./<db-name> > ./restore.log 2>&1 &
```

**Лайфхак**: наливать пустой кластер быстрее в один инстанс — выключить все ноды,
почистить диски, стартануть один инстанс с `skip_mongo_init`, налить дамп только в
него (выключенные ноды пройдут инициализацию из него).

## Хосты и пути

| Что | Путь на хосте |
|---|---|
| Данные | `/mnt/mongodb/` |
| Логи | `/mnt/logs/dbms/` (mongod-service.log), `/mnt/logs/system/` (mongo-push-backup.log) |
| Backup-скрипты/конфиг | `/etc/backups/` (mongo_push_backup_script.py, mongo_config.ini) |
| Init-скрипт пользователей | `/etc/mongo_init/script.js` |
| keyFile | `/var/lib/mongo/secret.key` |
| Сервис | `systemctl {status,start,stop} mongod` |

⚠️ Путь `/mnt/logs/dbms` (с 's' в `logs`).

## Работа с хостами

Подключение к хосту, выполнение команд и скачивание файлов — через скилл
[`mcc-host-worker`](../mcc-host-worker/SKILL.md) (`mcc ssh` / `sshexec` / `scp`).
Шаблон хоста: `1.db.<cluster>-mongo.<dc>.one-infra.ru` (+ hidden-инстансы).

## Источники

- Дежурство MDB: MongoDB — https://confluence.vk.team/pages/viewpage.action?pageId=1348619045
  (обновлять скилл при изменении страницы).
- MDBSUP-4902 — compact на хостах (диск 95%+ после удаления данных).
