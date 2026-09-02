# Диагностические команды MySQL-кластера MDB

mcc — всегда `--local -n infra`, ретраи 3–5 с паузой (см. скилл mcc-host-worker).
Хосты: `1.db.<cluster>.<dc>.one-infra.ru`, `1.orchestrator.<cluster>.<dc>.one-infra.ru`.

## Состояние хостов (без ssh)

```bash
mcc --local -n infra -c <dc> instances "%<cluster>%" -f yaml
# availability_details: 'Node status: SOURCE/REPLICA ...', 'Healthy: True. State: Leader/Follower'
```

## MySQL на db-хосте (клиент НЕ в PATH — только полный путь)

```bash
MYSQL="/usr/local/percona/bin/mysql --defaults-extra-file=/etc/percona/.my-admin.cnf"
$MYSQL -N -e 'select @@server_uuid, @@read_only, @@super_read_only'
$MYSQL -e 'show replica status\G' | egrep 'Source_Host|Replica_IO_Running:|Replica_SQL_Running:|Last_IO_Error|Last_SQL_Error|Seconds_Behind'
$MYSQL -N -e 'show master status'        # binlog-файл, позиция, GTID-сеты
$MYSQL -N -e 'select @@global.gtid_executed'
```

Пустой `show replica status` + `read_only=0` = нода не реплицируется и writable.

## Orchestrator (на лидере)

```bash
PASS=$(grep -o '"HTTPAuthPassword": *"[^"]*"' /etc/orchestrator/orchestrator.conf.json | sed 's/.*: *"//;s/"//')
TOKEN=$(echo -n "orchestrator:$PASS" | base64)
curl -sk -H "Authorization: Basic $TOKEN" https://localhost:3000/api/clusters    # алиасы: >1 = выпавшая нода
curl -sk -H "Authorization: Basic $TOKEN" https://localhost:3000/api/raft-health
curl -sk -H "Authorization: Basic $TOKEN" https://localhost:3000/api/raft-peers
```

Логи: `/mnt/logs/dbms/` (mysql.log и др.), грепать `purged required binary logs`,
`Semi-sync`, `replica`.

## ProxySQL (admin 6032, на любом db-хосте)

```bash
PMYSQL="/usr/local/percona/bin/mysql --defaults-extra-file=/etc/percona/.my-proxy-admin.cnf -h 127.0.0.1 -P 6032"
$PMYSQL -e 'select hostgroup_id,hostname,port,status from runtime_mysql_servers'
$PMYSQL -e 'select hostgroup,srv_host,status,ConnUsed,ConnFree,Queries from stats_mysql_connection_pool'
```

HG1 — writers (должен быть только source), HG2 — readers (все db-хосты).
`Queries` по хостам в HG1 показывает, реально ли раздаётся write-трафик.

## Бекапы

```bash
/usr/local/bin/wal-g --config /etc/backups/wal_g_config.env backup-list
grep backup_host /etc/backups/mysql_config.ini     # штатная нода бекапа
```
