# Подключение к MDB PostgreSQL хостам

## Базовый шаблон mcc ssh + expect

`mcc ssh <host>` открывает интерактивный шелл, не принимает команду как аргумент. Чтобы выполнить команду неинтерактивно — оборачиваем в `expect`:

```bash
expect -c '
set timeout 60
spawn mcc --local ssh <host>
expect "/# "
send "uptime; echo ===DONE===\r"
expect "===DONE==="
send "exit\r"
expect eof
' 2>&1 | tail -40
```

## Несколько команд за один заход

Между командами делаем `expect` на маркер — это гарантирует, что следующая команда пойдёт после завершения предыдущей:

```bash
expect -c '
set timeout 90
spawn mcc --local ssh <host>
expect "/# "
send "uptime; echo ===UPTIME===\r"
expect "===UPTIME==="
send "free -h; echo ===MEM===\r"
expect "===MEM==="
send "df -h /mnt/postgres; echo ===DISK===\r"
expect "===DISK==="
send "exit\r"
expect eof
' 2>&1 | tail -120
```

## Ловушки Tcl в `send`

### 1. `[...]` — command substitution

Tcl пытается выполнить `[23]` как команду. Сломается:
```tcl
send "grep -E \"^2026-07-23 0[23]:\" /mnt/logs/dbms/postgres.log | tail -30; echo ===DONE===\r"
# → "invalid command name \"23\""
```

Работает:
```tcl
send "grep \"^2026-07-23 03:\" /mnt/logs/dbms/postgres.log | head -60; echo ===DONE===\r"
```

Или escape: `\[23\]`. Проще использовать отдельные grep для каждого значения.

### 2. Вложенные кавычки

`sudo -u postgres bash -c "..."` с вложенными двойными кавычками ломает парсер. Обход через heredoc:

```bash
expect -c '
set timeout 60
spawn mcc --local ssh <host>
expect "/# "
send "cat > /tmp/x.sh << \"EOF\"\r"
send "sudo -u postgres psql -h /tmp -p 5432 -U root -d postgres -c \"SELECT pg_is_in_recovery();\"\r"
send "EOF\r"
expect "/# "
send "bash /tmp/x.sh; echo ===DONE===\r"
expect "===DONE==="
send "exit\r"
expect eof
'
```

### 3. `sleep` между командами

`mcc ssh` может оборваться с `SSL Handshake is not finished` при быстром повторном подключении. Между заходами делать `sleep 2-3`:

```bash
sleep 3; expect -c '...' 2>&1 | tail -50
```

## mcc scp особенности

- Скачивание директории: `mcc --local scp "<host>:/path/" "<local_dir>/"` — локальная директория должна существовать заранее (`mkdir -p`).
- Скачивание файла: локальный путь — **директория**, не путь к файлу.
- **Загрузка файла на хост: путь назначения — только директория.** Если указать полный путь с именем файла (`mcc scp local.py <host>:/etc/host_checker/checks/check_kafka.py`), mcc создаст на хосте **директорию** с именем файла и положит файл внутрь. Правильно: `mcc scp local.py <host>:/etc/host_checker/checks/`.
- `SSL Handshake is not finished` — повторить через 1-2 сек (tunnel ещё не поднялся).
- `EOF на tar header` — опечатка в пути или файла не существует.

Для разовых команд на хосте проще `expect + mcc ssh`, чем scp.

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
