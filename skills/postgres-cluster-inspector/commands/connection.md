# Команды на PostgreSQL-хосте

Доступ к хостам — через скилл [`mcc-host-worker`](../../mcc-host-worker/SKILL.md).
Здесь — только специфика PostgreSQL.

## Полезные команды на хосте

### Быстрая проверка здоровья

```bash
# Все ключевые сервисы
systemctl status stolon-keeper stolon-proxy stolon-sentinel etcd pgbouncer --no-pager -l 2>&1 | grep -E "●|Active:|Main PID:" | head -40

# Процессы postgres/stolon/pgbouncer
ps -eo pid,etime,comm,args | grep -E "postgres|stolon-keeper|pgbouncer" | grep -v grep | head -20

# Порты
sudo ss -ltnp | grep -E ":5432|:6432|:2379|:2380"
```

### Stolon status

```bash
stolonctl --cluster-name stolon --store-backend etcdv3 --store-endpoints=127.0.0.1:2379 status
```

### Etcd health

```bash
etcdctl endpoint health
etcdctl member list -w table
```

### Подключение к postgres/pgbouncer

```bash
sudo -u postgres psql -h /tmp -p 5432 -U root -d postgres        # postgres
sudo -u postgres psql -h /tmp -p 6432 -U pgbouncer -d pgbouncer  # pgbouncer admin
```

### Чтение dbstate/keeperstate

```bash
cat /mnt/postgres/dbstate       # {"UID":"...","Generation":N,"Initializing":false,...}
cat /mnt/postgres/keeperstate   # {"UID":"...","ClusterUID":"..."}
```
