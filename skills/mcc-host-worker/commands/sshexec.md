# `mcc sshexec` — неинтерактивный запуск

`mcc sshexec <host> "<cmd>"` выполняет команду неинтерактивно, аргументы принимает.
Удобнее чем `expect + mcc ssh` для коротких команд и перебора хостов.

## Грабли

- **Требует namespace `-n infra`** — без него `NamespaceMissingException` даже на
  dev-кластерах (проверено на mcc 0.29.0, `2.broker.rescale-mdbdev-kafka.pc`). В отличие от
  scp, который на dev иногда проходит без флага, sshexec namespace обязателен.
- **Не использовать к cloud-ops узлам** — TLS handshake timeout.
- Для остальных кейсов — работает.

```bash
mcc --local sshexec -n infra <host> "hostname -f; echo OK"
```

## Запуск на нескольких хостах × ДЦ

### Redis Sentinel — рестарт на всех хостах

```bash
for ((i=1; i <= 3; i++)); do
  for dc in hc kc pc; do
    HOST="1.shard${i}-db.mdb-health-mdb-redis.${dc}.one-infra.ru"
    mcc sshexec "$HOST" "confp --oneshot; systemctl restart redis" --namespace infra
  done
done
```

### Redis Cluster — failover

```bash
mcc sshexec "$HOST" "redis-cli -c --user master -a <password> cluster failover" --namespace infra
```

### Redis Cluster — ERR Slot 10922 fix (FLUSHALL на всех хостах)

```bash
clouds=("ic" "nc" "zc")
for cloud in "${clouds[@]}"; do
  for ((i=1; i<=3; i++)); do
    instance="1.shard$i-db.video-api-stat-tkns-vkvideo-redis.$cloud.one-infra.ru"
    mcc sshexec $instance --namespace infra redis-cli --user master --pass \
      "$(awk '$1=="user" && $2=="master" { gsub(/^[^>]*>/, "", $0); gsub(/ .*/, "", $0); print }' /etc/redis/acl/users.acl)" \
      FLUSHALL
  done
done
```

### ClickHouse — очистка detached parts

```bash
cluster_name="zen-events-log"
project="zinfra"
clouds=("rc" "pc")
for cloud in "${clouds[@]}"; do
    for ((i=1; i<=15; i++)); do
        instance="1.shard${i}-db.${cluster_name}-${project}-ch.${cloud}.idzn.ru"
        mcc sshexec -n dzen "$instance" "find /var/lib/clickhouse/1/store -path '*/detached/*' -delete"
    done
done
```

### ClickHouse — SYSTEM RELOAD CONFIG на всех хостах

```bash
for ((shardN=1; shardN<=22; shardN++)); do
  instance="1.shard${shardN}-db.<cluster>-<project>-ch.$cloud.one-infra.ru"
  mcc sshexec "$instance" --namespace infra "confp --oneshot; clickhouse-client --user backup-admin --password \$(grep -oP 'password:\s*\K[^ ]+' /etc/rscheck/checkclickhouse.conf) --query 'SYSTEM RELOAD CONFIG'"
done
```
