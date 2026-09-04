# `mcc ssh` + expect

`mcc ssh <host>` открывает **интерактивный** шелл. Аргументы команды не принимает
(`too many positional arguments`). Через stdin — `failed to get terminal size:
inappropriate ioctl for device`. `script -q /dev/null` — ввод искажается, символы
теряются. Единственный рабочий способ неинтерактивно выполнить команду — обернуть в
`expect` и отправлять строки после приглашения `/# `.

## Простой шаблон (одна команда)

```bash
expect -c '
set timeout 30
spawn mcc --local ssh 1.broker.<cluster>.<dc>.one-infra.ru
expect "/# "
send "uptime; echo ===DONE===\r"
expect "===DONE==="
send "exit\r"
expect eof
' 2>&1 | tail -40
```

- `mcc --local` — без self-update.
- `===DONE===` — sentinel для expect; без него ждёт весь `timeout`.
- `2>&1 | tail -40` — обрезает шум spawn + welcome-баннер mcc.
- `set timeout` под задачу: для `journalctl`, `kafka-share-groups.sh` — 60+.

## Несколько команд подряд

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

## Сложные команды — heredoc в файл

`send` ломается на вложенных кавычках (`sudo -u kafka bash -c "..."`). Решение —
записать скрипт heredoc-ом и запустить:

```bash
expect -c '
set timeout 60
spawn mcc --local ssh 1.broker.<cluster>.<dc>.one-infra.ru
expect "/# "
send "cat > /tmp/t.sh << \"EOF\"\r"
send "#!/bin/bash\r"
send "unset KAFKA_OPTS JMX_PORT\r"
send "/opt/kafka/bin/kafka-share-groups.sh --bootstrap-server \$(hostname -f):9092 --command-config /opt/kafka/config/client.properties --describe --all-groups --offsets 2>&1 | head -10\r"
send "echo ===DONE===\r"
send "EOF\r"
expect "/# "
send "bash /tmp/t.sh\r"
expect "===DONE==="
send "exit\r"
expect eof
' 2>&1 | tail -30
```

Нюансы:
- `<< "EOF"` — кавычки запрещают подстановку переменных **на локальной машине** внутри heredoc.
- expect всё равно интерполирует `\$(hostname -f)` — экранируем `$` как `\$`.
- После `EOF` ждём `/# ` (приглашение вернулось), потом `bash /tmp/t.sh`.

## `sudo -u <service>` и env

Чтобы получить env, который systemd даёт сервису, source'нуть EnvironmentFile:

```bash
sudo -u kafka bash -c 'set -a; . /etc/sysconfig/kafka; env | grep -iE "KAFKA_OPTS|JMX_PORT"'
```

## Параллельный запуск на нескольких хостах

С ограничением по параллелизму:

```bash
MAX_PARALLEL=10
for dc in hc kc pc; do
    for i in {1..75}; do
        host="$i.broker.<cluster>.$dc.one-infra.ru"
        (
            out=$(expect runner.exp "$host" 2>&1)
            # обработка $out
        ) &
        while [ $(jobs -r | wc -l) -ge $MAX_PARALLEL ]; do sleep 0.2; done
    done
done
wait
```

`runner.exp` — отдельный expect-скрипт, принимает host первым аргументом. Пример
(`kafka-cluster-inspector` для очистки `-stray` партиций):

```bash
cat << 'EOF' > runner.exp
set timeout 60
set host [lindex $argv 0]
log_user 0
if {[catch {spawn mcc ssh $host} reason]} { puts "0"; exit }
expect {
    -re "\[#$\]" {
        send "cd /mnt/data/log && du -sk *-stray 2>/dev/null | awk '{s+=\$1} END {print s+0}' && rm -rf *-stray && echo 'DONE' && exit\r"
        expect {
            -re "(\[0-9\]+).*DONE" { puts "$expect_out(1,string)" }
            timeout { puts "0" }
        }
    }
    timeout { puts "0" }
    eof { puts "0" }
}
catch {close}
catch {wait}
EOF
```

## Проверка после deploy

```bash
expect -c '
set timeout 60
spawn mcc --local ssh <host>
expect "/# "
send "systemctl status share-group-lag-exporter --no-pager | head -10; echo ===1===\r"
expect "===1==="
send "curl -s localhost:23570/metrics | head -10; echo ===2===\r"
expect "===2==="
send "tail -10 /mnt/logs/dbms/share-group-lag-exporter.err.log; echo ===3===\r"
expect "===3==="
send "exit\r"
expect eof
' 2>&1 | tail -60
```

## Промпт

`/# ` (слэш-решётка-пробел). Regex для expect: `/#\s*` или просто строка `/# `.
`\[#?\$\]` — НЕ срабатывает.

## `mcc ssh` обрывается при быстром повторном подключении

`SSL Handshake is not finished`. Между заходами `sleep 2-3`.
