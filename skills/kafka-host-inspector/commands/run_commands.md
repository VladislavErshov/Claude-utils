# Команды на Kafka-хосте

Подключение к хосту и выполнение команд — через скилл
[`mcc-host-access`](../../mcc-host-access/SKILL.md) (`mcc ssh` + expect, см.
`commands/ssh.md` для базового шаблона). Здесь — только специфика Kafka.

## sudo -u kafka и env

Чтобы получить env, который systemd даёт сервису, нужно source'нуть EnvironmentFile:

```bash
sudo -u kafka bash -c 'set -a; . /etc/sysconfig/kafka; env | grep -iE "KAFKA_OPTS|JMX_PORT"'
```

Это даёт тот же `KAFKA_OPTS` и `JMX_PORT=9000`, которые systemd подставляет в сервис.

## Сложные команды (heredoc-трюк для kafka-утилит)

Команды с вложенными кавычками (`sudo -u kafka bash -c "...внутри кавычки..."`) лучше
писать в файл на хосте через heredoc, потом `bash /tmp/x.sh`. Шаблон на хосте:

```bash
cat > /tmp/t.sh << "EOF"
#!/bin/bash
unset KAFKA_OPTS JMX_PORT
/opt/kafka/bin/kafka-share-groups.sh --bootstrap-server $(hostname -f):9092 --command-config /opt/kafka/config/client.properties --describe --all-groups --offsets 2>&1 | head -10
echo ===DONE===
EOF
bash /tmp/t.sh
```

Heredoc-трюк загружается через `mcc ssh + expect` (см. [`mcc-host-access`](../../mcc-host-access/SKILL.md),
`commands/ssh.md` и `commands/pitfalls.md` — Tcl-эскейпы, промпт `/# `).

## Проверка после deploy

На хосте выполнить:

```bash
systemctl status share-group-lag-exporter --no-pager | head -10
curl -s localhost:23570/metrics | head -10
tail -10 /mnt/logs/dbms/share-group-lag-exporter.err.log
```

Подключение к хосту — через скилл [`mcc-host-access`](../../mcc-host-access/SKILL.md).
