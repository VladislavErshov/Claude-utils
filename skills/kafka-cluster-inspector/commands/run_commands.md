# Выполнение команд на хосте через `mcc ssh` + `expect`

`mcc ssh <host>` открывает **интерактивный** шелл в контейнере. Аргументы команды он не
принимает — падает с `too many positional arguments`. Передача через stdin тоже не работает
(`failed to get terminal size: inappropriate ioctl for device`).

Единственный рабочий способ неинтерактивно выполнить команду — обернуть `mcc ssh` в `expect`
и отправлять строки после приглашения `/# `.

## Простой шаблон (одна команда)

```bash
expect -c '
set timeout 30
spawn mcc ssh 1.broker.<cluster>.<dc>.one-infra.ru
expect "/# "
send "uptime; echo ===DONE===\r"
expect "===DONE==="
send "exit\r"
expect eof
' 2>&1 | tail -40
```

- `set timeout 30` — если команда повиснет, expect отвалился бы. Ставь под задачу
  (для `journalctl`, `kafka-share-groups.sh` — 60+).
- `===DONE===` — sentinel, по нему expect понимает что можно выходить. Без него придётся
  ждать `timeout`.
- `2>&1 | tail -40` — обрезаем шум от spawn + welcome-баннер mcc.

## Несколько команд подряд

Просто несколько `send`/`expect` пар:

```bash
expect -c '
set timeout 30
spawn mcc ssh 1.broker.<cluster>.<dc>.one-infra.ru
expect "/# "
send "systemctl status kafka-broker --no-pager | head -5; echo ===1===\r"
expect "===1==="
send "tail -20 /mnt/logs/dbms/kafka-broker.err.log; echo ===2===\r"
expect "===2==="
send "exit\r"
expect eof
' 2>&1 | tail -80
```

## Сложные команды (кавычки, пайпы, sudo -u)

`send` ломается на вложенных кавычках. Особенно `sudo -u kafka bash -c "...внутри кавычки..."`.
Рабочий паттерн — heredoc в файл на хосте, потом `bash /tmp/x.sh`:

```bash
expect -c '
set timeout 60
spawn mcc ssh 1.broker.<cluster>.<dc>.one-infra.ru
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

Важно:
- `<< "EOF"` — кавычки вокруг `EOF` запрещают подстановку переменных **на локальной машине**
  внутри heredoc. Но `expect` всё равно интерполирует `\$(hostname -f)` — поэтому экранируем
  `$` как `\$`, чтобы bash на хосте выполнил `$(hostname -f)`.
- После `EOF` ждём `/# ` (приглашение вернулось), потом `bash /tmp/t.sh`.

## sudo -u kafka и env

Чтобы получить env, который systemd даёт сервису, нужно source'нуть EnvironmentFile:

```bash
sudo -u kafka bash -c 'set -a; . /etc/sysconfig/kafka; env | grep -iE "KAFKA_OPTS|JMX_PORT"'
```

Это даёт тот же `KAFKA_OPTS` и `JMX_PORT=9000`, которые systemd подставляет в сервис.

## Когда `mcc scp` не работает

На некоторых хостах `mcc scp` падает с `SSL Handshake is not finished` (tunnel ещё не
поднялся). В этом случае:
- либо повторить `mcc scp` через 1-2 сек
- либо сделать через `expect + mcc ssh` + heredoc (как выше)

## Что НЕ работает

- `mcc ssh <host> '<command>'` — `error: too many positional arguments`.
- `mcc ssh <host> -- bash -lc '...'` — то же.
- `mcc ssh <host> << 'EOF' ... EOF` (stdin) — `failed to get terminal size: inappropriate ioctl for device`.
- `script -q /dev/null mcc ssh <host> << EOF` — подключается, но ввод через stdin
  искажается (команды обрезаются, символы теряются). Не использовать.
- `mcc scp` с пробросом портов для SSH-агента — не предусмотрен интерфейсом mcc.

## Проверка после deploя

Чтобы проверить что сервис работает после передеплоя образа:

```bash
expect -c '
set timeout 60
spawn mcc ssh 1.broker.<cluster>.<dc>.one-infra.ru
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
