# Runbook для дежурного — Правила безопасной работы с PostgreSQL в MDB

Полный текст Runbook (прислан дежурными, всё что есть). DRAFT.

## Содержание

1. [Если больше половины реплик UNAVAILABLE](#если-больше-половины-реплик-unavailable)
2. [Как дебажить, почему реплика UNAVAILABLE](#как-дебажить-почему-реплика-unavailable)
3. [Правила безопасной работы с PostgreSQL в MDB](#правила-безопасной-работы-с-postgresql-в-mdb)
4. [Подключиться под суперпользователем к postgres/pgbouncer](#подключиться-под-суперпользователем-к-postgrespgbouncer)
5. [Обновление параметров по запросу пользователя, которых нет в UI](#обновление-параметров-по-запросу-пользователя-которых-нет-в-ui)
6. [Добавление пользователя](#добавление-пользователя)
7. [Зависшая операция создания/редактирования пользователя](#зависшая-операция-созданияредактирования-пользователя)
8. [Добавление базы данных](#добавление-базы-данных)
9. [Удаление базы данных](#удаление-базы-данных)
10. [Удалить пользователя](#удалить-пользователя)
11. [Сбросить пароль](#сбросить-пароль)
12. [Пользователю нужно запустить pg_repack](#пользователю-нужно-запустить-pg_repack)
13. [Найти медленный запрос](#найти-медленный-запрос)
14. [Отключить синхронную репликацию](#отключить-синхронную-репликацию)
15. [Закончились подключения к постгресу](#закончились-подключения-к-постгресу)
16. [Ошибка "Could not resize shared memory segment"](#ошибка-could-not-resize-shared-memory-segment)
17. [Переналивка реплики постгреса](#переналивка-реплики-постгреса)
18. [Реплика не поднимается даже после полной переналивки](#реплика-не-поднимается-даже-после-полной-переналивки)
19. [Добавление инстанса](#добавление-инстанса)
20. [Удаление инстанса](#удаление-инстанса)
21. [Переподнять постгрес, если у etcd нет кворума](#переподнять-постгрес-если-у-etcd-нет-кворума)
22. [Подписки: создать и удалить](#подписки-создать-и-удалить)
23. [Подписки 16 постгрес — добавить роль пользователю для создания подписок](#подписки-16-постгрес--добавить-роль-пользователю-для-создания-подписок)
24. [Выдача прав суперпользователя](#выдача-прав-суперпользователя)
25. [Выдача прав для datatransfer](#выдача-прав-для-datatransfer)
26. [Установка расширений](#установка-расширений)
27. [Увеличивается Replication Lag](#увеличивается-replication-lag)
28. [Как включить аналитику запросов](#как-включить-аналитику-запросов)
29. [Копится WAL](#копится-wal)
30. [Перевод кластера на ipv6](#перевод-кластера-на-ipv6)
31. [Поднять лимит хранения слота репликации](#поднять-лимит-хранения-слота-репликации)
32. [Включение логирования для SOC](#включение-логирования-для-soc)
33. [Не проходит ALTER TABLE по таймауту блокировки](#не-проходит-alter-table-по-таймауту-блокировки)
34. [Запуск нескольких экземпляров PgBouncer](#запуск-нескольких-экземпляров-pgbouncer)

---

## Если больше половины реплик UNAVAILABLE

В большинстве случаев это означает, что произошла нештатная ситуация при смене мастера и только мастер остался в живых.

**См. раздел [Отключить синхронную репликацию](#отключить-синхронную-репликацию)**, чтобы пишущие транзакции не подвисали на ожидании подтверждения и кластер продолжал работать.

## Как дебажить, почему реплика UNAVAILABLE

⚠️ **ВАЖНО:** сходу рестартить смысла мало — если реплика наливается, наливка пойдёт с нуля.

### Если статус rscheck: postgres is dead

**Смотрим `/mnt/logs/dbms/stolon-keeper.log`** (пример: `https://cloud.vk.team/cloud/KC/ns/infra/instance/1.db.amokrousov-test-14-mdbdev-pgsql.kc.one-infra.ru/file/mnt/logs/dbms/stolon-keeper.log`).

**Если там последние строчки вида:**
```
5523/26931 kB (20%), 0/1 tablespace (/mnt/postgres/postgres/base/1/2658 )
```
значит реплика наливается через `pg_basebackup`, остаётся ждать.

Можно ускорить, подняв IN на реплике и OUT на мастере:
- для 3-хостового кластера, если 2 реплики переналиваются: `OUT = 2*IN + запас на пользовательский трафик` (например, IN = 500 Mbit/s => OUT > 1000 Mbit/s)
- сейчас есть захардкоженное ограничение 200 Мбит/с, быстрее которого `pg_basebackup` не скачивает (но если 2 реплики качают, они будут 400 Мбит/с потреблять)
- это ограничение не распространяется на передаваемый WAL
- лимит можно поднять на новых образах с помощью переменной окружения, см. начало раздела [Переналивка реплики постгреса](#переналивка-реплики-постгреса)

Иногда WAL на диске на реплике занимает больше чем сама БД, из-за чего диск переполняется и скачивание начинается заново.

Если работает архивация WAL в облако:
- можно зайти в S3 бакет соответствующего кластера и проверить какие последние сегменты есть и их примерное соответствие текущему `pg_current_wal_lsn()` на мастере
- можно удалить файлы сегментов из директории `pg_wal` на наливающейся реплике: при её старте она начнёт подтягивать их из S3

**Иногда бывают строчки вида:**
```
pg_rewind: 2: 6FA/D508BD58 - 0/0
```
(после `pg_rewind` может быть что угодно, это не так важно) — реплика догоняется через `pg_rewind`, на кластере производящем много WAL может занимать около 5 минут (интервал чекпоинтов). Аналогично можно поднять сетевой лимит.

**Иногда бывают ошибки в логе:**
```
2025-09-01T14:28:40.011+0300    INFO    cmd/keeper.go:1142    current db UID different than cluster data db UID    {"db": "", "cdDB": "60b20d7e"}
2025-09-01T14:28:40.011+0300    ERROR    cmd/keeper.go:1469    different local dbUID but init mode is none, this shouldn't happen. Something bad happened to the keeper data. Check that keeper data is on a persistent volume and that the keeper state files weren't removed
```
Кто-то удалил диск (можно проверить по аудиту storage, например здесь `https://cloud.vk.team/cloud/KC/ns/infra/shard/amokrousov-test-14-mdbdev-pgsql.mdbdev.db.production.mdb.prod/db/1`).

**См. раздел [Переналивка реплики постгреса](#переналивка-реплики-постгреса) → Простой случай.**

**Смотрим `/mnt/logs/dbms/postgres.log`** (пример: `https://cloud.vk.team/cloud/KC/ns/infra/instance/1.db.amokrousov-test-14-mdbdev-pgsql.kc.one-infra.ru/file/mnt/logs/dbms/postgres.log`).

Там могут быть повторяющиеся ошибки, из-за которых postgres не поднимается.

**`max_connections` на реплике меньше чем на мастере:**

В такой ситуации нужно:
1. поправить конфиг в PMS
2. `confp --oneshot`
3. `stolonctl update --cluster-name stolon --store-backend etcdv3 --file /etc/stolon_init/stolon.conf`

после этого postgres после очередного ретрая должен подняться.

### Если статус rscheck: etcd is dead

В логе etcd ошибки вида `.`

Опять же кто-то удалил диск. **См. раздел [Переналивка реплики постгреса](#переналивка-реплики-постгреса) → Сложный случай (если etcd разломался/его диск очистили).**

---

## Правила безопасной работы с PostgreSQL в MDB

Здесь предлагается собрать базовые тезисы, чтобы не выстреливать в ногу самим себе.

- **Не зачищайте диски etcd**, просто так хост не переподнимется. В postgresql на данный момент не реализовано поднятие новой реплики на пустых дисках без ручных вмешательств, на это есть задача MDBDEV-1372 — Переналивка хоста после того как на нем зачистили или закораптились диски. Если очень хочется, это можно сделать, инструкция есть ниже.
- **Не меняйте параметры postgresql на хостах без синхронизации с PMS и базой**, при таких изменениях мы получаем неожиданное поведение при дальнейшей эксплуатации кластера. Если что-то делается на время, нужно создавать MDBSUP тикет с указанием что и когда вернуть, либо прописывать то что поменяли в шаблон (если по какой-то причине не устраивает дефолтное значение или параметра нет в UI).
- **Не меняйте параметры hardware на инстансах без синхронизации с базой**. Если во время инцидента что-то делали руками, обязательно нужно переносить настройки в нашу базу. Если нужного пресета нет, выставляем больший.

## Подключиться под суперпользователем к postgres/pgbouncer

Зайти на контейнер через скилл [`mcc-host-access`](../../mcc-host-access/SKILL.md) (`mcc ssh`).

**Сам postgres:**
```bash
sudo -u postgres psql -h /tmp -p 5432 -U root -d postgres
```

**Служебная база pgbouncer** — https://www.pgbouncer.org/usage.html
```bash
sudo -u postgres psql -h /tmp -p 6432 -U pgbouncer -d pgbouncer
```

## Обновление параметров по запросу пользователя, которых нет в UI

Иногда пользователи (обычно админы) приходят и просят поменять какие-то настройки постгреса, которых у нас нет в UI. Для этого нужно:

1. По возможности провалидировать через Мокроусов Антон адекватность изменений.
2. Добавить параметры в PMS в `zen.pgsql.stolon.conf` в секции `pgParameters`.
3. Запустить таску оператора `postgresql.update-config`.
4. Дождаться завершения.
5. Продублировать в шаблоне (табличка `cluster_to_templates`) изменения.

## Добавление пользователя

1. **Создать секрет с данными нового пользователя** (`zkv/mdb/PROJECT/pgsql/QUEUE.db.production.mdb.prod/users/USERNAME`). См. пример `https://vault.idzn.io/ui/vault/secrets/zkv/kv/mdb%2Fsitcentr%2Fpgsql%2Fpgsql-oncall-sitcentr-pgsql.sitcentr.db.production.mdb.prod%2Fusers%2Foncall-ro/`. `permissions` не факт что используются, но лучше проставить.

2. **Запустить таску на операторе:**
   - `vault_user_folder` — путь к папке `users` (а не к секрету)
   - `vault_password_key = password`
   - `user_settings` — скопировать `permissions` из vault, убрав поле `password` и добавив `username` с соответствующим значением

   **Представлять permissions как экранированную строку не нужно:**
   - В секретнице:
     ```json
     "password": "ЧТО-ТОТАМ"
     "permissions": "[{\"dbName\":\"oncall\",\"grants\":[{\"grant\":\"CONNECT\",\"has\":true},{\"grant\":\"CREATE\",\"has\":false}]}]"
     ```
   - В параметре таски:
     ```json
     {"username": "oncall_user", "permissions": [{"dbName":"oncall","grants":[{"grant":"CONNECT","has":true},{"grant":"CREATE","has":false}]}]}
     ```

   ⚠️ **Важно:** в `permissions` должны быть указаны все имеющиеся в кластере базы (в том числе база `postgres`; `template0` и `template1` указывать не нужно) и все гранты (`CONNECT` и `CREATE`) — для тех, которые не надо выдавать, со значением `has=false`. Иначе goal таски не выполнится и она не завершится.

3. Таска может зависнуть: тогда вручную делаем запрос из метода `getDatabases` и смотрим соответствие грантов, прописанных в списке, и грантов присутствующих в базе:
   ```sql
   SELECT
       datname AS database,
       pg_catalog.pg_get_userbyid(datdba) AS owner,
       datacl as acl
   FROM pg_database;
   ```

   Гранты, которые наследуются от роли `PUBLIC` (`CONNECT` и `CREATE` на все базы), нельзя отобрать у пользователя, надо отзывать через `PUBLIC` (перед этим убедившись, что у пользователя ничего не сломается) — подробнее в следующем разделе.

4. **Для read-only пользователя:** нужно сделать `GRANT pg_read_all_data TO dzenpro_readonly;`, тогда он сможет автоматом читать все таблицы в схеме `public` во всех базах данных, куда может подключиться.

5. И наконец нужно добавить пользователя в таблицу `users` в backstage.

## Зависшая операция создания/редактирования пользователя

**Ситуация:** операция создания/редактирования пользователя выполняется бесконечно.

**Как проверить:** подключиться к консоли и проверить права пользователей на базы данных (`\l+`). Если в access privileges есть записи вида `grantee=privileges/grantor` (пример: `testuser=Tc/postgres`), где grantee пустое (пример: `=Tc/postgres`).

Для старых кластеров или кластеров которые заезжали с суперпользователем можно проверить `PUBLIC` роль, доступы которой наследуют все пользователи. Возможно операция не может завершиться потому, что права отрываются у пользователя, но не у `PUBLIC`, и продолжают наследоваться пользователем, и операция выполняется бесконечно.

Проверить `PUBLIC` роль, которую наследуют все пользователи, и список пользователей (`\du`).

⚠️ **ВАЖНО:** требуется раздать уже существующим пользователям необходимые права перед отрывом грантов у `PUBLIC`. Иначе у какого-то важного пользователя может слететь доступ, который прямо сейчас может использоваться в приложении в сервисной учетке на проде — разработчики не ожидают, что у них просто так пропадают доступы, может привести к ИНЦИДЕНТУ.

**Требуется сначала выполнить все GRANT запросы, потом REVOKE.**

Для генерации выражений GRANT:

```sql
-- generate grants
SELECT
    format(
            'GRANT %s ON DATABASE %I TO "%s"%s;',
            acl.privilege_type,
            datname,
            roles.rolname,
            CASE
                WHEN acl.is_grantable THEN ' WITH GRANT OPTION'
                ELSE ''
                END
    ) AS grant_statement
FROM pg_database d,
     LATERAL aclexplode(d.datacl) acl,
     pg_roles roles
WHERE d.datname NOT IN ('template0', 'template1')
  AND d.datacl IS NOT NULL
  AND acl.grantee = 0
  AND roles.rolname !~ '^pg_';
```

Для генерации выражений REVOKE:

```sql
-- generate REVOKES
SELECT
    distinct format(
            'REVOKE %s ON DATABASE %I FROM PUBLIC;',
            acl.privilege_type,
            datname
    ) AS revoke_statement
FROM pg_database d,
     LATERAL aclexplode(d.datacl) acl,
     pg_roles roles
WHERE d.datname NOT IN ('template0', 'template1')
  AND d.datacl IS NOT NULL
  AND acl.grantee = 0
  AND roles.rolname !~ '^pg_';
```

**Требуется выполнить:**
1. Все запросы на GRANT для пользователей.
2. Все запросы на REVOKE для PUBLIC.

## Добавление базы данных

1. Запустить таску `postgresql.upsert-database`:
   ```json
   database_settings = {"name": "db", "params": {"owner": "ownername", "acl":"{=T/ownername,ownername=CTc/ownername}"}}
   ```
   Пример если имя пользователя содержит дефисы (в UI такое создавать запрещено, но где-то таких пользователей вручную создавали):
   ```json
   {"name": "db2", "params": {"owner": "user-user", "acl":"{\"=T/\\\"user-user\\\"\",\"\\\"user-user\\\"=CTc/\\\"user-user\\\"\"}"}}
   ```

2. После успешного завершения добавить в таблицу `databases` в backstage запись по аналогии с имеющимися строчками (поменяв `settings->pgsqlSettings→params`).

3. Добавить в таблицу `permissions` запись для указанного владельца со всеми правами.

## Удаление базы данных

1. Зайти из-под админа на хост.
2. `DROP DATABASE <name> WITH (FORCE)`.
3. Обновить информацию в бекстейдже.

## Удалить пользователя

Для каждой пользовательской базы (`database`) в кластере:

```sql
REASSIGN OWNED BY user TO root;   -- чтобы если что-то случайно принадлежало пользователю, не удалить
DROP OWNED BY user;               -- что удалить гранты
DROP USER user;
```

В базе `backstage_plugin_mdb` в таблице `users` помечаем пользователя как удалённый — выставить флаг в поле `is_deleted`.

Удаляем пользователя в vault. Путь в vault можно узнать из поля `vault_path` в таблице `users` базы `backstage_plugin_mdb`.

## Сбросить пароль

1. Запустить под суперпользователем `psql`.
2. Сделать `\password USERNAME`.
3. Ввести пароль (спросит 2 раза).
4. Выслать через одноразку (например, `enigma.bk.ru`) клиенту.
5. Изменить пароль в vault. Путь в vault можно узнать из поля `vault_path` в таблице `users` базы `backstage_plugin_mdb`.

## Пользователю нужно запустить pg_repack

Суперпользователь не нужен, достаточно:

- Чтоб пользователь был владельцем базы и объектов (таблиц/индексов в ней, обычно создают под владельцем, проблем нет).
- У него была роль `pg_read_all_data`.
- Запускать `pg_repack` с ключом `-k` (`--no-superuser-check`).

Узнать у пользователя от какого юзера будут запускать и выдать гранты:

```sql
GRANT USAGE, CREATE ON SCHEMA repack TO <user>;
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA repack TO <user>;
```

## Найти медленный запрос

См. "Полезные диагностические запросы" (в отдельном документе).

## Отключить синхронную репликацию

**Ситуации, когда нужно:**
- Живо меньше половины реплик, пишущие транзакции зависают на подтверждении, отчего просыпаем данные.
- WAL лог неожиданно не успевает прокачиваться через сеть, см. `https://st.yandex-team.ru/ZPI-1831`.

### Простой случай: живо больше половины реплик

1. Поменять `synchronousReplication` в `false` в конфиге stolon в PMS.
2. `confp --oneshot` чтоб подтянуть новые конфиги.
3. `stolonctl update --cluster-name stolon --store-backend etcdv3 --file /etc/stolon_init/stolon.conf`.
4. Проверить с помощью `SHOW synchronous_standby_names;` — должно быть пустое значение.

После решения проблем лучше поменять обратно на `true` аналогичной процедурой.

### Сложный случай

Stolon не сможет распространить изменения конфига, потому что нет кворума. Нужно вручную сделать:

1. Найти реплику, которая мастер.
2. `SHOW synchronous_standby_names;` — на мастере должен быть не пустым (т.к. включена синхронная репликация).
   - Если такой нет — либо синхронная репликация не была включена, либо умер хост, на котором был мастер, и реанимировать вручную не представляется возможным.
3. Выключить параметр на уровне искомой базы (или баз, если их несколько в кластере; глобально на уровне кластера не установится):
   ```sql
   ALTER DATABASE db_name SET synchronous_commit = 'local';
   ```
4. Прожать `Ctrl+C`, потому что транзакция ждет подтверждения реплик, которые мертвы.
5. Для проверки можно переподключиться к данной базе и сделать `SHOW synchronous_commit;`.
6. `systemctl reload pgbouncer` (чтобы разорвать серверные соединения и открыть новые с новым параметром).
7. `SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE wait_event='SyncRep'` (чтобы убить соединения, которые уже никогда не дождутся подтверждения).

**Как вернуть обратно после нормализации:**

1. Убрать параметр на уровне базы:
   ```sql
   ALTER DATABASE db_name RESET synchronous_commit;
   ```
2. `systemctl reload pgbouncer`.

## Закончились подключения к постгресу

Чаще всего возникает, если пользователь работает через pgbouncer в `transaction` режиме при деградации какого-либо частого запроса или просто большой всплеск нагрузки. Для стабилизации ситуации надо сделать количество подключений через pgbouncer меньше чем в самом postgresql доступно. Есть три варианта:

**Вариант 1.** Если в кластере всего одна основная база и один пользователь, от которого идут практически все запросы — нужно понизить настройку `default_pool_size` для pgbouncer:

1. Найти параметр `zen.pgsql.pgbouncer.conf`.
2. Сделать параметр `default_pool_size` меньше, чем в `zen.pgsql.stolon.conf` параметр `max_connections` (на 50-150).
3. Запустить `confp --oneshot` на всех хостах.
4. Запустить `systemctl reload pgbouncer` на всех хостах.

**Вариант 2.** Если в кластере много баз, то делаем всё то же самое, только с настройкой `max_client_conn`.

**Вариант 3.** Если ходят напрямую, делаем по третьему способу — поднимаем `max_connections` в `zen.pgsql.stolon.conf`:

1. Поднимаем в PMS.
2. `confp --oneshot` на репликах.
3. `stolonctl update --cluster-name stolon --store-backend etcdv3 --file /etc/stolon_init/stolon.conf`.
4. `kill -INT $(head -1 /mnt/postgres/postgres/postmaster.pid)` на репликах по очереди с интервалом 1 минута.
5. `confp --oneshot` на мастере.
6. `kill -INT $(head -1 /mnt/postgres/postgres/postmaster.pid)` на мастере.

После инцидента важно перенести все настройки в нашу базу данных (таблицы `backstage_plugin_mdb.public.db_cluster_version` и `backstage_plugin_mdb.public.cluster_to_template`).

При выполнении 3-го пункта, если осталась только одна реплика (вторая например в наливке), важно отключить синхронную репликацию.

## Ошибка "Could not resize shared memory segment"

**Симптомы:** пользователь приходит с ошибкой вида
```
ERROR: could not resize shared memory segment "/PostgreSQL.931175932" to 185024 bytes: No space left on device (SQLSTATE 53100)
```

**Решение:** нужно добавить переменную окружения `cloud_tmpfs=/dev/shm:2G` на все инстансы (происходит с перезагрузкой, поэтому применять сначала на репликах, потом сменить мастер, а потом только на старом мастере).

## Переналивка реплики постгреса

См. отдельный файл `reinit_replica.md` в этом скилле — полная инструкция по Простой/Сложной/Самой сложной переналивке.

### Почему может происходить

Перезаливка (reinit) в штатном режиме происходит крайне редко и самопроизвольно не возникает. Однако в отдельных случаях она возможна:

- **Перегрузка кластера по сети или другим ресурсам.** Происходит failover. У нового мастера недоступна сеть, поэтому бывший мастер не может подключиться к нему и выполнить `pg_rewind` → выполняется полная переналивка.
- **Повторный подъем бывшего мастера.** После переключения бывший мастер начнет делать `pg_rewind`; если этот процесс прервать (например, рестартнуть инстанс), повторно он это сделать не сможет и будет перезаливка.
- **Добавление реплики.** Это частный случай переналивки (в данном случае наливки реплики данными), это ситуация штатная; наливаться из бэкапа мы пока не умеем.

В описанных выше сценариях переналивка происходит автоматически, однако бывает такое что её нужно вызвать руками. Два наиболее частых кейса:

- **Длительная остановка хоста (больше недели).** Если хост был остановлен больше недели, то нужных WAL-ов в архиве уже нет, и придется переливаться.
- **Некорректная миграция зачита дисков etcd.**

### Общий совет — как увеличить скорость наливки реплики

Для кластеров версии 3.1.0 и выше, для ускорения процесса пераналивки можно использовать env `STOLON_PG_BASEBACKUP_MAX_RATE`, задачется в килобайтах/мегабайтах в секунду. Например, если указать `STOLON_PG_BASEBACKUP_MAX_RATE=25M`, это будет 200 Мбит/с. Задать нужно в манифесте, чтобы подтянулось (приведёт к рестарту, но для переналивающейся реплики это обычно не страшно).

### Простой случай (сломался только postgres)

**Симптомы:** Postgres поднимается, но в логах ошибка вида
```
could not receive data from WAL stream: ERROR:  requested WAL segment 0000000B000007AB00000047 has already been removed
```
То есть он не может догнать изменения, потому что часть WAL-лога из начала очереди уже затерлась на мастере/в архиве WAL.

На новых кластерах не должно воспроизводиться, но на старых (без включенного wal-g) может стрелять, если реплика полежала какое-то время.

UPD: на новых тоже может стрелять, если реплика лежала больше TTL архива (7 дней).

Или более сложные ситуации, когда postgres не поднимается, но etcd жив.

`etcdctl endpoint health` — если зависает, значит etcd сломался, см. следующий раздел.

**Чтоб переналить, нужно зайти на инстанс и сделать:**

```bash
systemctl stop pgbouncer
systemctl stop stolon-keeper

stolonctl removekeeper 1_teststanddevpgsql_dbmoney_kc_idzn_ru --cluster-name stolon --store-backend etcdv3   # исключить инстанс в stolon

stolonctl status --cluster-name stolon --store-backend etcdv3    # убедиться что нода исчезла

rm -r /mnt/postgres/*    # иначе при поднятии stolon попытается использовать старую версию директории данных

systemctl start stolon-keeper
systemctl start pgbouncer
```

### Сложный случай (если etcd разломался/его диск очистили)

Такая проблема может возникнуть при каких-то неполадках в onecloud:

- смерть железа
- кривая миграция на другой миньон при деплое
- появляется и не проходит ошибка вида `dzen::db-money.db.testing.money.prod/money-dev-pgsql/1 ( NORMAL on srvr796 ) reports not consistent for e.g. dzen::db-money.db.testing.money.prod/money-dev-pgsql/1/data NORMAL`
- 2 копии volume в статусе NORMAL

Можно удалить в sources volume и тогда всё заработает, но это не всегда работает, поэтому возможно придётся прибегать к полноценной переналивке.

**Шаги:**

1. Останавливаем инстанс.
2. `shards → volumes`, удаляем etcd/data/оба (пересоздадутся пустые).
3. Делаем на живом инстансе, заменив выделенное жирным на имя удаляемой ноды:
   ```bash
   stolonctl removekeeper 1_teststanddevpgsql_dbmoney_kc_idzn_ru --cluster-name stolon --store-backend etcdv3
   stolonctl status --cluster-name stolon --store-backend etcdv3

   etcdctl member list -w table    # видим id удаляемой ноды
   etcdctl member remove ID
   etcdctl member list -w table    # инстанс должен исчезнуть

   etcdctl member add 1.test-stand-dev-pgsql.db-money.kc.idzn.ru --peer-urls=http://1.test-stand-dev-pgsql.db-money.kc.idzn.ru:2380
   etcdctl member list -w table    # инстанс должен появиться в статусе unstarted
   ```
4. Стартуем инстанс. etcd не должен подниматься, инстанс повиснет в статусе `STARTING`, это ожидаемо.
5. Заходим по ssh (вместо последовательности команд можно запустить `systemctl start wipe-etcd`):
   ```bash
   systemctl stop etcd
   echo initial-cluster-state: existing >> /etc/etcd/etcd.conf

   rm -r /mnt/etcd/etcd
   systemctl start etcd     # тут немного повисит, но это ожидаемо
   ```
   После рестарта эти изменения исчезнут, но это ок.

### Самый сложный случай — развалился кворум etcd

**Ситуация:** выпали 2 из 3 миньонов, где расположены хосты. В такой ситуации у etcd нет кворума, соответственно переключения мастера не происходит.

Отметим, что если etcd жив — это не случай этой инструкции.

Пусть живой хост `1.db.amokrousov-test-16-mdbdev-pgsql.pc.one-infra.ru` и кластер из 3 хостов (наиболее частый случай).

1. **Снять дамп etcd:**
   ```bash
   etcdctl snapshot save snapshot.db
   ```
   Файл `snapshot.db` на всякий случай скачать на локальную машину через скилл
   [`mcc-host-access`](../../mcc-host-access/SKILL.md) (команда `scp`,
   `1.db.amokrousov-test-16-mdbdev-pgsql.pc.one-infra.ru:/snapshot.db` → `.`).

2. **Ребутстрапнуть оставшийся в живых хост etcd из дампа:**
   ```bash
   systemctl stop etcd
   rm -rf /mnt/etcd/etcd
   chown etcd:etcd snapshot.db
   sudo -u etcd etcdctl snapshot restore snapshot.db \
     --name 1.db.amokrousov-test-16-mdbdev-pgsql.pc.one-infra.ru \
     --data-dir /mnt/etcd/etcd \
     --initial-cluster=1.db.amokrousov-test-16-mdbdev-pgsql.pc.one-infra.ru=http://1.db.amokrousov-test-16-mdbdev-pgsql.pc.one-infra.ru:2380 \
     --initial-cluster-token db.amokrousov-test-16-mdbdev-pgsql \
     --initial-advertise-peer-urls=http://1.db.amokrousov-test-16-mdbdev-pgsql.pc.one-infra.ru:2380

   systemctl start etcd
   ```
   После этого важно увидеть, что stolon-keeper в логах перестал кидать ошибки подключения к etcd, а стал печатать статус. Теперь кластер etcd из одной ноды.

3. **Важно:** нужно удалить из столона все остальные реплики, чтоб при их возвращении с другими id не было конфликтов:
   ```bash
   stolonctl status --cluster-name stolon --store-backend etcdv3
   # текущий хост должен быть master (если нет — значит недостаточно подождали после переключения, он должен запромоутиться, т.к. единственный оставшийся в живых)
   stolonctl removekeeper 1_db_amokrousovtest16mdbdevpgsql_hc_oneinfra_ru --cluster-name stolon --store-backend etcdv3
   stolonctl removekeeper 1_db_amokrousovtest16mdbdevpgsql_kc_oneinfra_ru
   ```

4. **Регистрируем 2 ноду (например, HC):**
   ```bash
   etcdctl member list -w table    # убеждаемся что там только одна нода
   etcdctl member add 1.db.amokrousov-test-16-mdbdev-pgsql.hc.one-infra.ru --peer-urls=http://1.db.amokrousov-test-16-mdbdev-pgsql.hc.one-infra.ru:2380
   etcdctl member list -w table    # добавленная нода появилась в статусе unstarted
   ```
   Запускаем ноду в HC. etcd не поднимается, нода висит в starting.
   Редактируем `/etc/etcd/etcd.conf` — удаляем из списка третью ноду (`1.db.amokrousov-test-16-mdbdev-pgsql.pc.one-infra.ru`).
   Иначе при регистрации нода будет ошибка, что количество не сходится.
   ```bash
   systemctl start wipe-etcd
   ```
   После этого нода должна подняться.
   ```bash
   confp --oneshot
   ```
   чтобы откатить локальные изменения конфига etcd (не обязательно, но для избежания путаницы).

5. **Регистрируем третью ноду:** такие же шаги, только `/etc/etcd/etcd.conf` править не нужно.

## Реплика не поднимается даже после полной переналивки

**Ситуация:** реплика полностью переналилась (в логах видно успешно завершившийся `pg_basebackup`), но не поднимается, и в логах нет содержательных ошибок — есть какие-то ошибки, по которым выглядит что WAL поломан.

Очень редко (при хитрой комбинации нетсплитов и "удачных" таймингов переключений) можно поймать краевой случай, что после переключений мастера в бакете появляется `.history` файл от таймлайна, номер которого больше чем актуальный таймлайн в мастере. Реплика ищет самый старший по номеру таймлайн в бакете и пытается на него встать, но у неё это не получится (т.к. он не является потомком таймлайна на мастере).

**Как диагностировать и чинить** (вероятно нужно на каждой реплике сделать):

1. Смотрим номер таймлайна на текущем мастере:
   ```sql
   SELECT timeline_id FROM pg_control_checkpoint();
   ```

2. Идём на реплику, смотрим какие history файлы там лежат:
   ```bash
   ls -la /mnt/postgres/postgres/pg_wal | grep .history
   ```
   ⚠️ **ВАЖНО:** там номера в HEX, а в предыдущем запросе (и логах pg) — в десятичной системе счисления.

3. Если есть history файл с большим номером, чем текущий таймлайн мастера — нужно:
   - сохранить копию локально (там маленький текстовый файл)
   - удалить из бакета:
     ```bash
     aws s3api --endpoint-url https://s3.idzn.ru delete-object --bucket db-backups --key "pgsql/UUIDкластера/wal_005/0000001B.history.br"
     ```
     нужно, т.к. если просто удалить локально, в следующий раз wal-g его снова подтянет
   - удалить с хоста
   - форсировать полную переналивку реплики (по инструкции для простого случая)

## Добавление инстанса

1. **Обновить параметры кластера.** Нужно обновить в PMS:
   - Min/Max число синхронных реплик в конфиге столона
   - `zen.pgsql.hosts`, `zen.pgsql.canBeMasterHosts`, `zen.pgsql.canBeSyncHosts` (для шардированного кластера нужно быть внимательным, так как эти конфиги устанавливаются на уровне сервисов)
   - для шардированного кластера так же надо поменять `mdb.pgsql.sharded.coordinator.hosts`

2. **Для шардированного кластера** (эти пункты только для шардированного кластера):
   - находим мастер и заходим через скилл [`mcc-host-access`](../../mcc-host-access/SKILL.md) (`mcc ssh`)
   - `sudo -u postgres psql -h /tmp -p 5432 -U root -d postgres`
   - далее нужно подключиться к базам, где установлен citus (это все базы кроме служебных `postgres`, `template0`, `template1`); список получаем командой `\l`
   - `\c <database name>`
   - `SELECT * FROM citus_add_node('<hostname добавляемой реплики>', 5432, 1, 'secondary');`

3. `confp --oneshot` чтоб подтянуть новые конфиги.

4. `stolonctl update --cluster-name stolon --store-backend etcdv3 --file /etc/stolon_init/stolon.conf`.

5. **Добавляем инстанс в кластер etcd:**
   ```bash
   etcdctl member add 1.test-stand-dev-pgsql.db-money.kc.idzn.ru --peer-urls=http://1.test-stand-dev-pgsql.db-money.kc.idzn.ru:2380
   ```

6. Добавляем шард (дисковый).

7. Добавляем шард (инстанс).

8. См. пункт 5 из переналивки реплики (иначе реплика не запускается).

## Удаление инстанса

1. Меняем мастера, если он там находится.
2. Стопаем инстанс.
3. Удаляем инстанс.
4. Удаляем шард. На каком-нибудь другом инстансе:
   ```bash
   etcdctl member list -w table    # видим id удаляемой ноды
   # (etcdctl member list -w json) для автоматики
   etcdctl member remove ID
   stolonctl removekeeper 1_teststanddevpgsql_dbmoney_kc_idzn_ru --cluster-name stolon --store-backend etcdv3
   stolonctl status --cluster-name stolon --store-backend etcdv3    # чтоб убедиться, что исчез
   # stolonctl status --cluster-name stolon --store-backend etcdv3 -f json - для автоматики
   ```

5. Обновляем параметры кластера (см. п.1 из добавления инстанса). + Меняем `zen.pgsql.backupNode`, если он попадает на удаленную ноду.

6. Делаем `confp --oneshot` на этой ноде, чтоб новая нода смогла делать бэкапы.

7. Для шардированного делаем те же манипуляции, что и написаны в пункте 1b, но их нужно делать в последнюю очередь, и команда будет вместо добавления:
   ```sql
   SELECT citus_remove_node('<hostname для удаления>', 5432);
   ```

## Переподнять постгрес, если у etcd нет кворума

**Ситуация:**
- Больше половины реплик лежит, у etcd нет кворума.
- Из-за этого stolon не делает никаких действий.
- Postgres продолжает работать, хоть и в read-only режиме.
- Если postgres по какой-то причине упадёт, его никто не поднимет. Так и задумано, потому что в такой ситуации не понятно, имеем ли право подниматься, а кэшировать у себя состояние тоже может приводить к проблемам.

**Решение:**

Вручную запустить postgres c помощью команды:
```bash
sudo -u postgres nohup /usr/lib/postgresql/14/bin/postgres -D /mnt/postgres/postgres -c unix_socket_directories=/tmp >/dev/null 2>&1 &
```

⚠️ **Важно:** после нормализации ситуации убить postgres с помощью
```bash
kill -INT $(head -1 /mnt/postgres/postgres/postmaster.pid)
```
Stolon-keeper его сам переподнимет под собой. Это важно, чтобы systemdшное завершение видело, что в нашем дереве есть процессы постгреса и не репортило, что юнит завершен раньше времени.

## Подписки: создать и удалить

В data transfer поддерживается debezium — отправляем туда: `https://docs.vk.team/datatransfer/pages/faq.html`. Нужна веская причина, чтобы использовать механику подписок.

UPD: оказалось, что пока в человеческом виде это не доступно, поэтому инструкция пока актуальна.

Создается подписка по готовому запросу от пользователя, например:
```sql
CREATE SUBSCRIPTION some_sub
  CONNECTION 'host=host1 port=5432 dbname=some-db-name user=replicator password=abcdefg'
  PUBLICATION userservice
  WITH (slot_name=userservice_repl, create_slot=false);
```

Чтобы удалить подписку, нужно 3 команды:
```sql
ALTER SUBSCRIPTION some_sub DISABLE;
ALTER SUBSCRIPTION some_sub SET (slot_name=NONE);
DROP SUBSCRIPTION some_sub;
```

## Подписки 16 постгрес — добавить роль пользователю для создания подписок

```sql
GRANT pg_create_subscription TO <username>;
```

## Выдача прав суперпользователя

Отметить в "Выдача прав суперпользователя (на время заезда)".

**Зачем обычно просят:** чтобы быстрее переносить, если куча пользователей/ролей, чтобы их не накликивать в UI.

На постоянку не выдаём (исключение — возможно только для коробочных решений, которые без суперпользователя не могут работать).

Выдать права какому-то пользователю (не root) с помощью:
```sql
ALTER USER username WITH SUPERUSER;
```

Вписать в табличку по ссылке, чтоб после заезда отозвать.

## Выдача прав для datatransfer

Не актуально, можно накликать через UI.

Для того, чтоб база смогла быть источником для DataTransfer, нужно (здесь `repluser` — имя пользователя, под которым хотят подключаться):

1. Выдать пользователю права на репликацию:
   ```sql
   ALTER USER repluser WITH REPLICATION;
   ```
2. Быть владельцем БД (на самом деле нужны только CREATE привилегии, но так проще):
   ```sql
   GRANT owner_name TO repluser;
   ```
3. Быть владельцем таблиц для добавления их в подписку:
   - делаем `\dt` в консоли psql
   - для каждого уникального владельца, которого видим (и который не совпадает с `repluser`), делаем:
     ```sql
     GRANT owner_name TO repluser;
     ```

## Установка расширений

### pg_cron

1. Проверить актуальность версии образа — должна быть 1.4.1 и выше (если нужно, обновить версию).
2. Добавить в `zen.pgsql.stolon.conf` в PMS:
   - `shared_preload_libraries` — дописать `pg_cron` (список через запятую)
   - `"cron.database_name": "ИМЯ БАЗЫ ДЛЯ УСТАНОВКИ РАСШИРЕНИЯ"`
   - `"cron.use_background_workers": "true"` (иначе через localhost не может подключиться)
3. Обновить конфиг в таблице `cluster_to_template`.
4. Запустить таску на операторе `postgresql.update-config`.
5. После завершения сделать на целевой базе:
   ```sql
   CREATE EXTENSION pg_cron;
   ```
6. Создать роль с правами на управление pg_cron, выдать целевому пользователю:
   ```sql
   CREATE ROLE mdb_cron NOLOGIN;
   GRANT USAGE ON SCHEMA cron TO mdb_cron;
   GRANT mdb_cron TO ЮЗЕРНЕЙМ;
   ```
7. Для проверки зайти под пользователем и проверить запросом:
   ```sql
   select * from cron.job;
   select * from cron.job_run_details order by start_time desc limit 5;
   ```

### pgaudit (если просят включить аудит)

1. Проверить актуальность версии образа — должна быть 1.4.1 и выше (если нужно, обновить версию).
2. Добавить в `zen.pgsql.stolon.conf` в PMS:
   - `shared_preload_libraries` — дописать `pgaudit` (список через запятую)
3. Обновить конфиг в таблице `cluster_to_template`.
4. Запустить таску на операторе `postgresql.update-config`.
5. После завершения сделать на целевой базе:
   ```sql
   CREATE EXTENSION pgaudit;
   ```
6. Указать нужные параметры (например, `ALTER DATABASE postgres SET pgaudit.log='read, write, ddl, role, function'`).
7. Запустить таску на операторе `postgresql.update-config` (чтобы pgbouncer переоткрыл серверные соединения с новыми параметрами).

### pg_partman

1. Установить расширение на целевой базе:
   ```sql
   CREATE SCHEMA partman;
   CREATE EXTENSION pg_partman SCHEMA partman;
   ```
2. Создать роль с правами на данную схему, выдать целевому пользователю:
   ```sql
   CREATE ROLE mdb_partman_user;
   GRANT ALL ON SCHEMA partman TO mdb_partman_user;
   GRANT ALL ON ALL TABLES IN SCHEMA partman TO mdb_partman_user;
   GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA partman TO mdb_partman_user;
   GRANT EXECUTE ON ALL PROCEDURES IN SCHEMA partman TO mdb_partman_user;
   GRANT mdb_partman_user TO ЮЗЕРНЕЙМ;
   ```

### Просто через CREATE EXTENSION ставятся

- `btree_gist`
- `pg_repack`
- `pgcrypto`
- `pgzstd`
- `pg_uuidv7`
- `timescale` — в `shared_preloaded_libraries` добавить `timescaledb`; `CREATE EXTENSION timescaledb VERSION '2.19.3';` (почему-то без указания версии пытается поставить версию, которой нет, как минимум на pg16; для pg15-16 образ больше или равен 3.1.4 — `VERSION '2.21.1'`).

### Для нас (внутренние)

- `pg_stat_statements` (`pg_read_all_stats`)
- `pgstattuple`
- `pageinspect`
- `pg_buffercache`

### vector

⚠️ **Важно:** версия образа 3.1.3 и выше.

```bash
# Зайти на мастер через скилл mcc-host-access (mcc ssh) <master host>
# подключаемся к бд template1
sudo -u postgres psql -h /tmp -p 5432 -U root -d template1    # проверяем что расширение ещё не стоит
\dx

# ставим vector
CREATE EXTENSION vector;

# ещё раз проверяем — теперь что вектор есть
\dx
```

Пример консоли (через скилл [`mcc-host-access`](../../mcc-host-access/SKILL.md)):
```
m.khlybova@m-khlybova ~ % <подключение к хосту 1.db.trend-media-infra-pgsql.kc.one-infra.ru через скилл mcc-host-access>
*** notice: using autodetected cloud kc
** Connected. Container is at your command.
1.db.trend-media-infra-pgsql.kc.one-infra.ru: /# sudo -u postgres psql -h /tmp -p 5432 -U root -d template1
psql (16.9 (Ubuntu 16.9-1.pgdg20.04+1))
Type "help" for help.

template1=# \dx
List of installed extensions
Name | Version | Schema | Description
---------+---------+------------+------------------------------
plpgsql | 1.0 | pg_catalog | PL/pgSQL procedural language
(1 row)

template1=# CREATE EXTENSION vector;
CREATE EXTENSION
template1=# \dx
List of installed extensions
Name | Version | Schema | Description
---------+---------+------------+------------------------------------------------------
plpgsql | 1.0 | pg_catalog | PL/pgSQL procedural language
vector | 0.8.0 | public | vector data type and ivfflat and hnsw access methods
(2 rows)

template1=# exit
1.db.trend-media-infra-pgsql.kc.one-infra.ru: /# exit
logout
*** Connection closed by remote host ***
m.khlybova@m-khlybova ~ %
```

## Увеличивается Replication Lag

Тут стоит разделять случаи, когда нет операции по изменению данных (`insert`/`update`/`delete`) и когда они есть. В первом случае увеличение `replay lag` — это нормальное явление. Так как оно вычисляется как разность между текущим временем и временем последней воспроизведённой транзакции на реплике. Так как изменений нет, то и это время не меняется. Например, на графиках видно, что `replay lag` растёт до 3 минут. При этом изменений за период роста метрики так же нет.

Во втором случае надо разбираться детально. В этом случае общего решения нет, и нужно смотреть на другие метрики. Например, WAL не успевает доставляться до реплик, так как мастер уперся в OUT сеть.

Стоит также отметить некоторую путаницу, возникающую с метрикой `lag`. Она рассчитывается, если значения `last_wal_receive_lsn` и `last_wal_replay_lsn` не равны. Соответственно, она может выглядеть как резкий пик (прошло много времени с последней воспроизведённой транзакции). Это может сбивать с толку и вызывать вопросы со стороны пользователей — "почему у меня в кластере периодически возникает большой replication lag".

**Код вычисления метрик `lag` и `replay lag`:**

```sql
SELECT
    CASE
        WHEN NOT pg_is_in_recovery() THEN 0
        WHEN pg_last_wal_receive_lsn () = pg_last_wal_replay_lsn () THEN 0
        ELSE GREATEST (0, EXTRACT(EPOCH FROM (now() - pg_last_xact_replay_timestamp())))
    END AS lag,
    CASE
        WHEN pg_is_in_recovery() THEN 1
        ELSE 0
    END as is_replica,
    GREATEST (0, EXTRACT(EPOCH FROM (now() - pg_last_xact_replay_timestamp()))) as last_replay
```

## Как включить аналитику запросов

1. Обновить кластер до версии образа 2.0.1 или выше (для pg14/15/16). Если кластер старый и там нет диска с логами, нужно сделать действия из инструкции "Обновление старых кластеров" (там же можно сразу нужную версию указать). По обычному сценарию запускаем таск `postgresql.update`.

## Копится WAL

(см. отдельный раздел Runbook — заполнить при появлении данных)

## Перевод кластера на ipv6

Тут есть 2 вида запросов.

### Добавить v6 сетевой интерфейс

Для всех видов сети (`lan`, `wan`, `wlan`).

Последовательность по хостам:
1. Сначала на каждой реплике.
2. Меняем мастер (руками).
3. Выполняем команду на текущем мастере:
   ```bash
   systemctl stop pgbouncer; systemctl stop stolon-keeper; stolonctl failkeeper $(hostname|sed 's/-//g'|sed 's/\./_/g') --cluster-name stolon --store-backend etcdv3; sleep 60;
   ```
   Инстанс перейдёт в `unavailable`, и должен появиться мастер в другом ДЦ. После этого выполняем пункт c.
4. Меняем на старом мастере.

### DRAFT: Удалить v4 сетевой интерфейс

⚠️ Здесь ещё не протестировано, не будет ли спецэффектов.

Важно: если кластер продовый, перед операцией уточнить у пользователя, поддерживает ли их клиентское приложение работу с ipv6.

Для всех видов сети (`lan`, `wan`, `wlan`) — аналогичная последовательность.

## Поднять лимит хранения слота репликации

**Ситуация:** слот репликации уходит в статус `lost`. Обычно это проявляется в том, что DataTransfer падает при рестарте из-за невозможности снять снепшот.

**Как проверить:** сделать `SELECT * FROM pg_replication_slots;` на целевой базе. Слот логической репликации должен быть в статусе `lost`.

**Как исправить:**

1. Исправить в PMS параметр `max_slot_wal_keep_size` на большее значение (по умолчанию 2 ГБ). Нужно учитывать, чтоб как минимум такое кол-во свободного места (+ запас) было на диске.
2. Запустить таску `update-config`.
3. Поправить шаблон в таблице `cluster_to_template` в базе backstage.

## Включение логирования для SOC

⚠️ **Важно:** предупредить пользователей о работах, т.к. будут перезагрузки + смена мастера.

В PMS-конфиге нужно заменить/добавить параметры:

- `"shared_preload_libraries"`: `'pgaudit'` — нужно добавить к существующей строке
- `"log_statement": "none"`
- `"pgaudit.log_parameter": "on"`
- `"pgaudit.log": "ROLE,DDL,MISC,FUNCTION"`
- `"log_line_prefix": "%m | %u | %d | %a | %c | %l | %e | %s | %h || "`
- `"log_connections": "on"` (или `"true"`)
- `"log_disconnections": "on"` (или `"true"`)

⚠️ **Важно:** не забыть актуализировать конфиг в backstage базе:
- `cluster_to_template`
- в настройках `cluster_version` включить `log_connections` в `true`

Установить `mdb.vector.kafka.additional.destinations`:
```json
[
  {
    "name": "oneme_postgres",
    "topic": "oneme_mdb.events.postgres.raw"
  }
]
```

С помощью оператора:
- сделать `update` на версию:
  - `3.4.0` — 14/15
  - `3.5.0` — 16
- запустить операторную таску `update-config`

Сделать `CREATE EXTENSION pgaudit;` во всех БД, которые видно в UI.

## Не проходит ALTER TABLE по таймауту блокировки

**Ситуация:** запрос добавления столбца или что-то в этом духе возвращает `cancelled due to lock timeout` (пример `https://u.internal.myteam.mail.ru/profile/AoLjyn_ojWAb12g4GQ`).

**Что происходит:**

- есть какие-то долгоиграющие сессии, которые мешают захватить эксклюзивную блокировку на таблицу
- часто такая сессия — autovacuum: хоть он и останавливается принудительно, может за `lock_timeout` по умолчанию (1с) не успеть завершиться

**Решение:**

Просим пользователей поднять `lock_timeout` **НА УРОВНЕ ТРАНЗАКЦИИ** (чтоб не аффектить остальные сеансы).

Здесь вместо `ALTER` нужно написать нужную вам команду. Стоит попробовать таймаут несколько секунд (5/10/15), но не сильно больше (разумный предел — минута), т.к. может получиться так: `ALTER`, висящий в очереди ожидания блокировки, блокирует получение лока последующими селектами, поэтому они не могут исполниться.

## Запуск нескольких экземпляров PgBouncer

Для увеличения/уменьшения количества запущенных экземпляров pgbouncer нужно изменить значение параметра `mdb.pgsql.pgbouncer.replicas` в PMS. Значение указывает на общее количество запущенных процессов. Значение по умолчанию — 1.

Чтобы изменение значения параметра `mdb.pgsql.pgbouncer.replicas` вступило в силу, нужно выполнить следующие действия:

```bash
confp --oneshot
systemctl daemon-reload
systemctl start pgbouncer-init
systemctl restart pgbouncer
```

Если `pgbouncer-init` упал с ошибкой (поднимали с 1 до большего кол-ва) — делаем ещё раз init:
```bash
systemctl start pgbouncer-init
```

Если нужно снизить — после этого надо ещё сделать для лишних:
```bash
systemctl stop pgbouncer@50002.socket
```
