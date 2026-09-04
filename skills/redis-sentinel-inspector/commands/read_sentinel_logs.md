# Чтение redis-sentinel.log

## Скачать логи со всех хостов кластера

Пользователь даёт список хостов вида `1.db.<cluster>-cfs-redis.<dc>.one-infra.ru`.
Скачать `/mnt/logs/dbms/` со всех хостов — через скилл
[`mcc-host-worker`](../../mcc-host-worker/SKILL.md) (`mcc scp`, шаблон массового скачивания — в скилле mcc-host-worker
для шаблона массового скачивания).

Скачаются: `redis.log`, `redis-sentinel.log`, `redis-server-systemd-service.log`.

⚠️ Путь именно `/mnt/logs/dbms` (с 's' в `logs`). Опечатка `/mnt/log/dbms` даёт
ошибку скачивания.

## Что искать в redis-sentinel.log

Лог — обычный текст, не JSON. Формат строки:
```
<pid>:X <date> <level> <message>
```
`X` означает sentinel mode (вместо `M` master / `C` RDB/AOF child).

### Спам "Failed to resolve hostname"

Маркер **зомби-хоста** в known-peers. Найти:

```bash
for H in ~/redis_logs/*; do
  echo "=== $H ==="
  grep "Failed to resolve hostname" "$H/redis-sentinel.log" | \
    awk '{print $NF}' | tr -d "'" | sort -u
  echo "last occurrence:"
  grep "Failed to resolve hostname" "$H/redis-sentinel.log" | tail -1
done
```

Если в выводе есть хост, который был удалён из инфраструктуры (DNS не резолвит) —
это кейс "забытый known-peer". См. `sentinel_reset.md` для лечения.

### Хронология known-peers для конкретного хоста

```bash
TARGET_HOST="1.db.<cluster>-cfs-redis.<dc>.one-infra.ru"
LOG=~/redis_logs/<sentinel-host>/redis-sentinel.log

# Когда хост был добавлен/удалён/ушёл в sdown
grep "$TARGET_HOST" "$LOG" | grep -E "(\+sentinel|\+slave|\+sdown|-sdown|\+reboot|\+reset-master|\+failover|\+promoted)" | head -40
```

Ключевые события:
- `+sentinel sentinel <myid> <host> 26379 @ <master> <master-host> 6379` — peer-sentinel добавлен
- `+slave slave <host>:6379 <host> 6379 @ <master> <master-host> 6379` — redis-реплика добавлена
- `+sdown sentinel <myid> <host> 26379 @ ...` — peer-sentinel перестал отвечать (субъективный down)
- `+sdown slave <host>:6379 <host> 6379 @ ...` — redis-реплика перестала отвечать
- `-sdown ...` — восстановился
- `+reboot slave <host>:6379 ...` — ребутнулся
- `+reset-master master <master-name> ...` — сброс state для мастера (после SENTINEL RESET)
- `Sentinel new configuration saved on disk` — sentinel записал state на диск

### Найти последнее добавление хоста

```bash
grep "$TARGET_HOST" "$LOG" | grep -E "(\+sentinel|\+slave)" | tail -10
```

### Найти последнее sdown

```bash
grep "$TARGET_HOST" "$LOG" | grep "+sdown" | tail -10
```

## Что искать в redis.log

`redis.log` — лог самого redis-сервера (не sentinel). Там:
- `Replication <master>|<slave>` события — роль при старте.
- `MASTER <-> REPLICA sync started` / `MASTER <-> REPLICA sync: Finished` — репликация.
- `configEpoch` / `clusterEpoch` — для cluster mode (не sentinel).

При разборе "зомби-хоста" `redis.log` обычно не нужен — всё в sentinel-логе.

## Проверка после SENTINEL RESET

После выполнения `SENTINEL RESET <master>` на всех хостах — подождать 30-60 сек и
проверить, что спам `Failed to resolve hostname` прекратился: ещё раз скачать
`/mnt/logs/dbms/redis-sentinel.log` со всех хостов (через скилл
[`mcc-host-worker`](../../mcc-host-worker/SKILL.md), `mcc scp`) и проверить:

```bash
for H in "${CLUSTER_HOSTS[@]}"; do
  echo "=== $H ==="
  tail -20 ~/redis_logs/$H/redis-sentinel.log | grep "Failed to resolve hostname" | tail -3
done
```

Если строк больше нет — RESET сработал, хост забыт. Если продолжает спамить —
sentinel всё ещё держит known-peer (возможно, RESET выполнили не на всех хостах,
либо state вернулся из persistence — тогда рестартовать sentinel после ручного
удаления строк `known-replica`/`known-sentinel` для мёртвого хоста из `sentinel.conf`).
