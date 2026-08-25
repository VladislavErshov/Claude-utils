# Переналивка реплики постгреса

## Почему может происходить

Перезаливка (reinit) в штатном режиме происходит крайне редко и самопроизвольно не возникает. Однако в отдельных случаях она возможна:

1. **Перегрузка кластера по сети или другим ресурсам.** Происходит failover. У нового мастера недоступна сеть, поэтому бывший мастер не может подключиться к нему и выполнить pg_rewind → выполняется полная переналивка.

2. **Повторный подъем бывшего мастера.** После переключения бывший мастер начнет делать pg_rewind, если этот процесс прервать (например рестартнуть инстанс) повторно он это сделать не сможет и будет перезаливка.

3. **Добавление реплики.** Это частный случай переналивки (в данном случае наливки реплики данным), это ситуация штатная наливаться из бэкапа мы пока не умеем.

В описанных выше сценариях перналивка происходит автоматически, однако бывает такое что ее нужно вызвать руками. Два наиболее частых кейса:

- **Длительная остановка хоста (больше недели).** Если хост был остановлен больше недели то нужных валов в архиве уже нет и придется переливаться.
- **Некорректная миграция зачита дисков etcd.**

## Общий совет — как увеличить скорость наливки реплики

Для кластеров версии 3.1.0 и выше, для ускорения процесса перанливки можно использовать env `STOLON_PG_BASEBACKUP_MAX_RATE`, задачется в килобайтах/мегабайтах в секунду, например если указать `STOLON_PG_BASEBACKUP_MAX_RATE=25M` это будет 200 Мбит/с. Задать нужно в манифесте чтобы подтянулось (приведёт к рестарту, но для переналивающейся реплики это обычно не страшно).

Также для ускорения:
- На реплике поднять IN (через env).
- На мастере поднять OUT (= 2*IN + запас на пользовательский трафик).
- Захардкоженное ограничение 200 Мбит/с быстрее которого pg_basebackup не скачивает (но если 2 реплики качают, они будут 400 Мбит/с потреблять). Это ограничение не распространяется на передаваемый WAL.

## Простой случай (сломался только postgres)

### Симптомы

- Postgres поднимается, но в логах ошибка вида `could not receive data from WAL stream: ERROR: requested WAL segment 0000000B000007AB00000047 has already been removed`.
- Или более сложные ситуации: postgres не поднимается, но etcd жив.

### Диагностика etcd

```bash
etcdctl endpoint health
```
Если зависает — etcd сломался, см. **Сложный случай**.

### Процедура переналивки

Зайти на инстанс и выполнить:

```bash
systemctl stop pgbouncer
systemctl stop stolon-keeper

# Исключить инстанс в stolon. UID = hostname с подчёркиваниями вместо - и .
stolonctl removekeeper 1_teststanddevpgsql_dbmoney_kc_idzn_ru --cluster-name stolon --store-backend etcdv3

# Убедиться, что нода исчезла
stolonctl status --cluster-name stolon --store-backend etcdv3

# Удалить весь data-dir (иначе stolon попытается использовать старую версию данных)
rm -r /mnt/postgres/*

systemctl start stolon-keeper
systemctl start pgbouncer
```

### Как проверить, что переналивка идёт

```bash
tail -f /mnt/logs/dbms/stolon-keeper.log
# должны появиться строки вида:
# 5523/26931 kB (20%), 0/1 tablespace (/mnt/postgres/postgres/base/1/2658)

ps -eo pid,etime,comm,args | grep pg_basebackup | grep -v grep
# процесс pg_basebackup должен быть активен
```

### После завершения pg_basebackup

В `postgres.log` должны появиться:
```
LOG:  entering standby mode
LOG:  redo starts at ...
LOG:  consistent recovery state reached at ...
LOG:  database system is ready to accept read-only connections
LOG:  started streaming WAL from primary at ... on timeline N
```

В `stolonctl status` наш keeper должен показать `PG HEALTHY: true`.

### Частые ошибки при Простой случае

**Ошибка:** Переименовали `postgres/`, но не сделали `stolonctl removekeeper` + не удалили `dbstate`/`keeperstate`.
**Симптом:** keeper крутится в цикле:
```
INFO  database cluster not initialized
INFO  our db requested role is standby
INFO  our db role is none
```
**Фикс:** Выполнить полный цикл по инструкции выше. `dbstate`/`keeperstate` тоже должны быть удалены через `rm -r /mnt/postgres/*`.

**Ошибка:** Удалили `postgres/`, но etcd ещё помнит старый DB UID.
**Симптом:** `current db UID different than cluster data db UID`.
**Фикс:** `stolonctl removekeeper` ДО удаления data-dir.

## Сложный случай (если etcd разломался/его диск очистили)

Такая проблема может возникнуть при каких-то неполадках в onecloud:

- смерть железа
- кривая миграция на другой миньон при деплое
- появляется и не проходит ошибка вида `dzen::db-money.db.testing.money.prod/money-dev-pgsql/1 ( NORMAL on srvr796 ) reports not consistent for e.g. dzen::db-money.db.testing.money.prod/money-dev-pgsql/1/data NORMAL`
- 2 копии volume в статусе NORMAL

Можно удалить в sources volume и тогда всё заработает, но это не всегда работает, поэтому возможно придётся прибегать к полноценной переналивке.

### Шаги

1. **Остановить инстанс.**

2. **Удалить volume(s):** `shards → volumes`, удалить etcd/data/оба (пересоздадутся пустые).

3. **На живом инстансе**, заменив выделенное жирным на имя удаляемой ноды:
   ```bash
   stolonctl removekeeper 1_teststanddevpgsql_dbmoney_kc_idzn_ru --cluster-name stolon --store-backend etcdv3
   stolonctl status --cluster-name stolon --store-backend etcdv3

   etcdctl member list -w table    # видим id удаляемой ноды
   etcdctl member remove ID
   etcdctl member list -w table    # инстанс должен исчезнуть

   etcdctl member add 1.test-stand-dev-pgsql.db-money.kc.idzn.ru --peer-urls=http://1.test-stand-dev-pgsql.db-money.kc.idzn.ru:2380
   etcdctl member list -w table    # инстанс должен появиться в статусе unstarted
   ```

4. **Стартовать инстанс.** etcd не должен подниматься, инстанс повиснет в статусе `STARTING`, это ожидаемо.

5. **Зайти по ssh** (вместо последовательности команд можно запустить `systemctl start wipe-etcd`):
   ```bash
   systemctl stop etcd
   echo initial-cluster-state: existing >> /etc/etcd/etcd.conf

   rm -r /mnt/etcd/etcd
   systemctl start etcd     # тут немного повисит, но это ожидаемо
   ```

   После рестарта эти изменения исчезнут, но это ок.

## Самый сложный случай — развалился кворум etcd

### Ситуация

Выпали 2 из 3 миньонов, где расположены хосты. В такой ситуации у etcd нет кворума, соответственно переключения мастера не происходит.

Если etcd жив — это не случай этой инструкции.

### Пусть живой хост `1.db.amokrousov-test-16-mdbdev-pgsql.pc.one-infra.ru` и кластер из 3 хостов (наиболее частый случай)

1. **Снять дамп etcd:**
   ```bash
   etcdctl snapshot save snapshot.db
   ```
   Файл `snapshot.db` на всякий случай скачать на локальную машину через скилл
   [`mcc-host-worker`](../../mcc-host-worker/SKILL.md) (команда `scp`,
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

3. **Удалить из столона все остальные реплики,** чтоб при их возвращении с другими id не было конфликтов:
   ```bash
   stolonctl status --cluster-name stolon --store-backend etcdv3
   # текущий хост должен быть master (если нет - значит недостаточно подождали после переключения, он должен запромоутиться, т.к. единственный оставшийся в живых)

   stolonctl removekeeper 1_db_amokrousovtest16mdbdevpgsql_hc_oneinfra_ru --cluster-name stolon --store-backend etcdv3
   stolonctl removekeeper 1_db_amokrousovtest16mdbdevpgsql_kc_oneinfra_ru
   ```

4. **Регистрируем 2 ноду (например, HC):**
   ```bash
   etcdctl member list -w table     # убедиться, что там только одна нода
   etcdctl member add 1.db.amokrousov-test-16-mdbdev-pgsql.hc.one-infra.ru --peer-urls=http://1.db.amokrousov-test-16-mdbdev-pgsql.hc.one-infra.ru:2380
   etcdctl member list -w table     # добавленная нода появилась в статусе unstarted
   ```
   Запускаем ноду в HC. etcd не поднимается, нода висит в starting.

5. **Редактировать `/etc/etcd/etcd.conf`** — удалить из списка третью ноду (`1.db.amokrousov-test-16-mdbdev-pgsql.pc.one-infra.ru`). Иначе при регистрации нода будет ошибка, что количество не сходится:
   ```bash
   systemctl start wipe-etcd
   ```
   После этого нода должна подняться.

6. `confp --oneshot` — чтобы откатить локальные изменения конфига etcd (не обязательно, но для избежания путаницы).

7. **Регистрируем третью ноду** — такие же шаги, только `/etc/etcd/etcd.conf` править не нужно.

## Реплика не поднимается даже после полной переналивки

### Ситуация

Реплика полностью переналилась (в логах видно успешно завершившийся `pg_basebackup`), но не поднимается и в логах нет содержательных ошибок, есть какие-то ошибки по которым выглядит что WAL поломан.

### Причина

Очень редко (при хитрой комбинации нетсплитов и "удачных" таймингов переключений) можно поймать краевой случай, что после переключений мастера в бакете появляется `.history` файл от таймлайна, номер которого больше чем актуальный таймлайн в мастере. Реплика ищет самый старший по номеру таймлайн в бакете и пытается на него встать, но у неё это не получится (т.к. он не является потомком таймлайна на мастере).

### Как диагностировать и чинить

Вероятно нужно на каждой реплике сделать:

1. **Смотрим номер таймлайна на текущем мастере:**
   ```bash
   sudo -u postgres psql -h /tmp -p 5432 -U root -d postgres -c "SELECT timeline_id FROM pg_control_checkpoint();"
   ```

2. **Идём на реплику, смотрим какие history файлы там лежат:**
   ```bash
   ls -la /mnt/postgres/postgres/pg_wal | grep .history
   ```
   ⚠️ **ВАЖНО:** там номера в HEX, а в предыдущем запросе (и логах pg) — в десятичной системе счисления.

3. **Если есть history файл с большим номером, чем текущий таймлайн мастера — нужно:**
   - сохранить копию локально (там маленький текстовый файл)
   - удалить из бакета:
     ```bash
     aws s3api --endpoint-url https://s3.idzn.ru delete-object --bucket db-backups --key "pgsql/UUIDкластера/wal_005/0000001B.history.br"
     ```
     Нужно, т.к. если просто удалить локально, в следующий раз wal-g его снова подтянет.
   - удалить с хоста
   - форсировать полную переналивку реплики (по инструкции для простого случая)

## Дополнительно: что НЕ делать

- **Не зачищать диски etcd** просто так — хост не переподнимется, в postgresql на данный момент не реализовано поднятие новой реплики на пустых дисках без ручных вмешательств (на это есть задача MDBDEV-1372).
- **Не менять параметры postgresql на хостах без синхронизации с PMS и базой** — получим неожиданное поведение при дальнейшей эксплуатации. Если что-то делается на время, нужно создавать MDBSUP тикет с указанием что и когда вернуть, либо прописывать то что поменяли в шаблон.
- **Не менять параметры hardware на инстансах без синхронизации с базой.** Если во время инцидента что-то делали руками, обязательно нужно переносить настройки в нашу базу, если нужного пресета нет — выставляем больший.
