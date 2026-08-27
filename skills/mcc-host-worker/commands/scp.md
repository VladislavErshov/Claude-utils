# `mcc scp` — копирование файлов

## Базовый синтаксис

```bash
mcc --local scp <source> <dest>
mcc --local scp -n infra <source> <dest>   # при NamespaceMissingException
```

## Скачивание директории

```bash
mkdir -p ~/logs/<host>          # локальная директория ДОЛЖНА существовать
mcc scp "<host>:/mnt/logs/dbms/" ~/logs/<host>/
```

- Trailing `/` в source обязателен для директорий.
- Локальная директория назначения **должна существовать** заранее (`mkdir -p`).

## Скачивание одиночного файла — dest это директория

Локальный путь должен быть **директорией**, не путём к файлу:

```bash
mcc scp "$HOST:/etc/redis/sentinel.conf" ~/file.conf   # НЕ сработает: failed to open destination directory
mcc scp "$HOST:/etc/redis/" ~/redis_conf/              # работает (но качает всю директорию)
```

## Загрузка файла на хост — dest это директория

Если указать полный путь с именем файла, mcc создаст на хосте **директорию** с этим
именем и положит файл внутрь:

```bash
mcc scp local.py <host>:/etc/host_checker/checks/check_kafka.py   # НЕВЕРНО — создаст директорию check_kafka.py
mcc scp local.py <host>:/etc/host_checker/checks/                  # ВЕРНО
```

ClickHouse, заливка геобазы:

```bash
mcc scp /tmp/regions_hierarchy.txt <host>:/var/lib/clickhouse/1/regions    # неверно — создаст директорию regions
mcc scp /tmp/regions_hierarchy.txt <host>:/var/lib/clickhouse/1/regions/   # верно
```

## Файлы без расширения — качать директорию

`sysconfig`, `jaas.conf` при одиночном scp иногда падают с
`failed to read downloaded archive header: EOF`. Решение — качать всю директорию
целиком (там mcc отдаёт tarball и распаковывает сам). Для `sysconfig` путь принципиально
`/etc/sysconfig/kafka`, не `/opt/kafka/config/sysconfig`.

## Не перезаписывает существующий файл

```bash
mcc scp local.py <host>:/etc/host_checker/checks/   # файл не обновится, если уже есть
# Решение: rm + scp
expect -c '
set timeout 30
spawn mcc --local ssh <host>
expect "/# "
send "rm -f /etc/host_checker/checks/check_kafka.py\r"
expect "/# "
send "exit\r"
expect eof
'
mcc scp local.py <host>:/etc/host_checker/checks/
```

## Опечатки в пути → `EOF на tar header`

Частая опечатка: `/mnt/log/dbms` вместо `/mnt/logs/dbms` (с 's' в `logs`) — приводит к
`failed to read downloaded archive header: EOF`.

## Массовое скачивание логов со всех хостов кластера

```bash
CLUSTER_HOSTS=(
  1.db.<cluster>-cfs-redis.ec.one-infra.ru
  1.db.<cluster>-cfs-redis.kc.one-infra.ru
  1.db.<cluster>-cfs-redis.pc.one-infra.ru
)

mkdir -p ~/redis_logs
for H in "${CLUSTER_HOSTS[@]}"; do
  D=~/redis_logs/$H
  mkdir -p "$D"
  mcc scp "$H:/mnt/logs/dbms/" "$D/" 2>&1 | head -3
done
```

## Заливка файла на хост base64-чанками через expect (обход молчаливого scp и 414)

Когда `mcc scp` не подходит (молча не заливает / нужны не-мусорные пути), а `mcc sshexec`
падает `414 URI Too Long` (команда уходит в URL, лимит ~8KB) — заливаем base64-чанками
по ~800 символов через `mcc ssh + expect`. Генератор expect-файла (пути и хост подставить):

```bash
python3 -c "
import base64
b64 = base64.b64encode(open('/tmp/local_file','rb').read()).decode()
chunks = [b64[i:i+800] for i in range(0, len(b64), 800)]
lines = ['set timeout 120', 'spawn mcc --local ssh <host>', 'expect \"/# \"',
         'send \"rm -f /tmp/r.b64\\r\"', 'expect \"/# \"']
for i, c in enumerate(chunks):
    op = '>' if i == 0 else '>>'
    lines.append(f'send \"printf %s \\'{c}\\' {op} /tmp/r.b64\\r\"')
    lines.append('expect \"/# \"')
lines += ['send \"base64 -d /tmp/r.b64 > /tmp/remote_file && wc -c /tmp/remote_file\\r\"',
          'expect \"/# \"', 'send \"exit\\r\"', 'expect eof']
open('/tmp/upload.exp','w').write('\\n'.join(lines)+'\\n')
"
expect -f /tmp/upload.exp 2>&1 | tail -20
```

`wc -c` в конце — сверка размера. Использовался в kafka-reassign-partitions (заливка
reassign.json, 46 чанков на 27KB) и MDBSUP-4895/4899 — разборы в
`kafka-cluster-inspector/history/`.

## SCP RDB-бэкап (Redis)

```bash
mcc scp <source_master_host>:/mnt/redis/dump.rdb ./
mcc scp ./dump.rdb <target_master_host>:/mnt/redis
```

## SCP etcd snapshot (PostgreSQL)

```bash
mcc scp 1.db.<cluster>.pc.one-infra.ru:/snapshot.db .
```
