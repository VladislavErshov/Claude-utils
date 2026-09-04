---
name: mysql-cluster-inspector
description: Инспекция и дежурство по MySQL-кластерам MDB (Percona Server 8.0) — source/replica + Orchestrator (raft x3) + ProxySQL (HG1 writers / HG2 readers), сплит-брейн (два Source в UI, orchestrator видит два cluster-алиаса, proxysql пишет в оба), выпавшая из топологии реплика, purged binlogs, реплика лагает, orchestrator unavailable (сменился ip в raft), proxysql checksums не равны, восстановление реплики из wal-g бекапа. Канон процедур — дежурная страница Confluence «Дежурство MDB: MySql»; скилл хранит специфику, дополнения и грабли. Хосты: 1.db.<cluster>.<dc>.one-infra.ru / 1.orchestrator.<cluster>.<dc>.one-infra.ru. Доступ — через скилл `mcc-host-worker` (mcc sshexec). Используй при запросах «почему два Source», «реплика не подтягивается», «orchestrator видит два кластера», «проверь топологию MySQL», «подними реплику из бекапа».
allowed-tools: [bash, read_file, write_file, edit_file]
---

# Скилл инспекции MySQL-кластеров MDB

**Канон дежурной инструкции — Confluence «Дежурство MDB: MySql» (SSOT)**:
https://confluence.vk.team/pages/viewpage.action?pageId=1515973526
(разделы: Проблемы/действия с кластером, MySql, Orchestrator, ProxySql, восстановление
из бекапа). Вики живая — процедуры править там; здесь подключение, диагностика,
наши дополнения и грабли из реального инцидента (rb-reklama-vkfeed-adtech-mysql,
сплит-брейн 25.08–02.09.2026).

## Файлы скилла

- [commands/status.md](commands/status.md) — диагностические команды (instances,
  mysql, orchestrator API, proxysql, backup-list).
- [commands/split-brain-fence.md](commands/split-brain-fence.md) — фенсинг
  выпавшей ноды, разбор таблиц по GTID, перестройка из бекапа.
- [history/](history/) — разборы реальных инцидентов (формат `<тикет>-<дата>.md`).

## Архитектура

- **Percona Server 8.0** (порт 3306): один source + реплики, репликация GTID
  (auto-position), row-based, полусинхронная. Рабочая директория `/mnt/data/mysql`,
  логи `/mnt/logs/dbms/mysql.log`, бинлоги `/mnt/data/mysql/data/binlog.NNNNNN` (~1ГБ,
  ~1.6ГБ/день на нагруженном кластере, retention ~6–8 дней).
- **Orchestrator** — 3 ноды (raft, порт 9000; UI/API 3000, SSL, basic auth; бекенд
  sqlite `/mnt/data/orchestrator/`). Один Leader — только он делает recovery; все ноды
  делают discovery. При падении source обновляет PMS `mysql.replication.source.host`.
  **Важно:** нода, у которой сброшена репликация (`reset replica`), регистрируется как
  отдельный cluster-алиас (single-node topology) — корень сплит-брейнов.
- **ProxySQL** на db-хостах: admin-порт 6032, клиентские 6033/3001/3002. HG1 — writers
  (только source), HG2 — readers (все db-хосты). Core-ноды синкают конфиг чексуммами.
- **wal-g** (xtrabackup-push): ночные автобекапы с хоста из PMS `mysql.backup.node`
  (файл `/etc/backups/mysql_config.ini`, `backup_host=`). Список:
  `wal-g --config /etc/backups/wal_g_config.env backup-list`.

## Подключение (главные грабли)

```bash
# mysql-клиент НЕ в PATH неинтерактивного шелла — только полный путь:
/usr/local/percona/bin/mysql --defaults-extra-file=/etc/percona/.my-admin.cnf   # admin, все права
/usr/local/percona/bin/mysql --defaults-extra-file=/etc/percona/.my-proxy-admin.cnf -h 127.0.0.1 -P 6032   # proxy-admin
```

Orchestrator API (на лидере; лидер = Leader в `mcc instances` availability_details):

```bash
PASS=$(grep -o '"HTTPAuthPassword": *"[^"]*"' /etc/orchestrator/orchestrator.conf.json | sed 's/.*: *"//;s/"//')
TOKEN=$(echo -n "orchestrator:$PASS" | base64)
curl -sk -H "Authorization: Basic $TOKEN" https://localhost:3000/api/clusters      # алиасы
curl -sk -H "Authorization: Basic $TOKEN" https://localhost:3000/api/cluster/<alias>
curl -sk -H "Authorization: Basic $TOKEN" https://localhost:3000/api/raft-health
```

## Быстрая диагностика

1. `mcc --local -n infra -c <dc> instances "%<cluster>%"` — `availability_details`:
   `Node status: SOURCE/REPLICA`, `Healthy/State: Leader/Follower`, репликация.
2. На каждом db-хосте:
   ```sql
   select @@server_uuid, @@read_only, @@super_read_only;
   show replica status\G      -- пустой вывод = нода не реплицируется вообще
   show master status;        -- GTID-сеты для сверки между хостами
   ```
3. Сверка GTID-сетов между хостами: расхождение диапазонов одного стрима = потери.
   Маппинг стрим → хост даёт `select @@server_uuid` на каждом.
4. Orchestrator `/api/clusters` — **два и более алиасов = выпавшая нода**.
5. ProxySQL: `runtime_mysql_servers` (кто в HG1/HG2), `stats_mysql_connection_pool`
   (Queries по хостам — куда реально идёт трафик).
6. Скорость расхождения: `show master status` дважды с интервалом 30с на обоих хостах.

## Каталог проблем

### Сплит-брейн: два Source, orchestrator видит два алиаса (инцидент 2026-09-02)

Симптомы: в UI несколько db-хостов с ролью Source; availability PREFAIL «Insufficient
number of replicas» на обоих; raft orchestrator при этом полностью здоров.

Механика: репликация ноды сломалась (в т.ч. purged binlogs для подключающихся),
репликацию на ней сбросили → orchestrator зарегал её отдельным алиасом → proxysql
оставил её ONLINE в HG1 → writes идут ~50/50 в оба «source», GTID-деревья расходятся
(по ~100k trx на сторону за неделю).

Диагностика: пункты 2–6 выше + в логе выпавшей ноды `[Repl] Semi-sync source failed`,
`Cannot replicate to server ... purged required binary logs`.

**Фенсинг (порядок важен):**
1. ProxySQL (proxy-admin на живом source):
   ```sql
   UPDATE mysql_servers SET status='OFFLINE_SOFT'
    WHERE hostname='<выпавший>' AND hostgroup_id IN (1,2);
   LOAD MYSQL SERVERS TO RUNTIME; SAVE MYSQL SERVERS TO DISK;
   ```
2. На выпавшей ноде: `SET GLOBAL super_read_only=1; SET GLOBAL read_only=1;`
3. Грабля: watch-таска оператора через ~минуту возвращает ноду ONLINE в HG2 (ок,
   чтения), но HG1 (writers) не возвращает — сплит остановлен.
4. Проверка: `show master status` на фенсеной ноде дважды — позиция замерла.

**Разбор «какие таблицы расходились»** (для тикета/мерджа):
```bash
/usr/local/percona/bin/mysqlbinlog -vv --base64-output=DECODE-ROWS \
  --include-gtids='<uuid>:<start>-<end>' binlog.06[7-9][0-9] \
| sed -n 's/.*[Tt]able[_ ]\{0,1\}map: `\([^`]*\)`\.`\([^`]*\)`.*/\1.\2/p' \
| sort | uniq -c | sort -rn
```
Учитывать retention: бинлоги покрывают только последние ~6–8 дней; более раннее —
только в wal-g бекапе ноды.

**Восстановление ноды** (если данные теряемы): перестройка из бекапа каноничного
дерева — дока «Восстановление реплики из бекапа» (wipe `/mnt/data/mysql` → иерархия
data/tmp/redo-archive → `wal-g backup-fetch <ИМЯ>` → GTID_PURGED из
`xtrabackup_binlog_info` → правка `/etc/percona/init.py` → start). **Не брать LATEST
вслепую** — смотреть `backup-list` и брать бекап с backup_host; чейндж-таска может
дописать туда бекап фенсеной ноды. После старта: replica status (source = мастер),
`/api/clusters` (лишний алиас схлопнется), proxysql HG1.

### Orchestrator unavailable

rscheck «Orchestrator is dead», в логах `[WARN] raft: Remote peer ... does not have
local node ... as a peer` — на ноде сменился ip (часто v4↔v6), в raft-пирях других
нод остался старый. Проверка: `/api/raft-peers` на здоровых нодах. Лечение —
`systemctl restart orchestrator` на здоровых нодах с интервалом ~30с (⚠️ при таком
вариante возможен failover — дока, секция «Orchestrator unavailable»).

### Прочее (канон — дока, секции)

- Реплика лагает — `show replica status` (Read_Source_Log_Pos vs Relay_Source_Log_File),
  график MySQL ReplicaLag.
- Реплика не может выполнить транзакцию — mysqlbinlog по GTID с мастера, правка данных,
  `STOP/START REPLICA SQL_THREAD`.
- Операторная таска зависла «proxysql checksums are not equal» — синк через
  `stats_proxysql_servers_checksums` + LOAD ... TO RUNTIME с ноды-эталона.
- Бекап не снимается (>10000 партов) — `WALG_S3_MAX_PART_SIZE` в `mysql.wal-g.conf`.
- Изменение конфигов — PMS + шаблоны `cluster_to_template` в backstage, рестарт
  `confp --oneshot && systemctl restart <svc>`.

## Грабли сессий

- `pkill -f xtrabackup` через `mcc sshexec` убивает собственную сессию (паттерн
  совпадает с cmdline удалённого `bash -c`). Bracket-трюк: `pkill -f 'xtrabacku[p] --backup'`.
- Долгие команды (wal-g push/fetch, mysqlbinlog по ~13ГБ) — только через `nohup` в
  файл + опрос; sshexec-сессия может оборваться.
- Сложные команды с backtick/кавычками — base64-скрипт: `echo <b64> | base64 -d > /tmp/x.sh; bash /tmp/x.sh`.
