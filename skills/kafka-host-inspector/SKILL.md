---
name: kafka-host-inspector
description: Методы работы с хостами MDB Kafka (broker / controller / cruise-control) — подключение через mcc ssh + expect, особенности mcc scp, шаблоны выполнения команд на хосте, путеводитель по путям на хосте (логи, конфиги, SSL, systemd, rscheck, host_checker, prometheus, cruise-control). Список хостов задаёт пользователь (формат 1.broker.<cluster>.<dc>.one-infra.ru / 1.controller.<cluster>.<dc>.one-infra.ru / 1.cruise.<cluster>.<dc>.one-infra.ru). Используй когда нужно выполнить команду на Kafka-хосте неинтерактивно, скачать/залить файл, найти где лежит конфиг или лог, или разобраться с нюансами mcc ssh/scp.
allowed-tools: [Bash, Read, Write, Edit, Grep, Glob]
---

# Скилл работы с хостами MDB Kafka

Скилл-помощник для подключения и выполнения команд на хостах Kafka-кластера под управлением
mdb-data. Не содержит диагностики — только методы работы с хостами (mcc ssh + expect,
mcc scp особенности, путеводитель по путям).

Диагностику кластера (логи, KRaft quorum, известные проблемы) — см. `kafka-cluster-inspector`.
Метрики и Jolokia MBean'ы — см. `kafka-metrics-investigator`.

## Формат хостов

```
{1,2,3,...}.broker.<cluster>.<dc>.one-infra.ru     — Kafka broker
{1,2,3,...}.controller.<cluster>.<dc>.one-infra.ru — KRaft controller
1.cruise.<cluster>.<dc>.one-infra.ru               — Cruise Control (один на кластер, может не быть)
```

ДЦ — любые (`hc`, `pc`, `uc`, `kc`, `ec`, `dc`, `rc`, ...). Формат хоста не зависит от ДЦ.

Пользователь даёт список хостов. Скилл не угадывает хосты — только работает с тем, что дал юзер.

## Что нужно

- **mcc** (`/Users/vl.ershov/Documents/mcc/mcc`, есть в PATH) — доступ к хостам.
- **Всегда `mcc --local`** (`-l`) для `ssh`/`scp` — без него mcc на каждый вызов тянет свежую
  версию с cloud-мастера (self-update: медленно + мусор в выводе). Флаг подавляет это.
- **`mcc scp`** — для копирования файлов/директорий (см. ниже особенности).
- **`mcc ssh` + `expect`** — для удалённого выполнения команд. `mcc ssh` интерактивный
  и не принимает command как аргумент (`mcc ssh <host> <cmd>` → `error: too many positional
  arguments`), но через `expect` можно отправлять команды построчно. Шаблон — в
  `commands/run_commands.md`. Не работает передача через stdin или `bash -c "..."`.

## mcc scp особенности

- Скачивание директории: `mcc scp "<host>:/path/" "<local_dir>/"` — локальная директория должна
  существовать заранее (`mkdir -p`).
- Скачивание файла: локальный путь — **директория**, не путь к файлу.
- **Загрузка файла на хост: путь назначения — только директория.** Если указать полный путь
  с именем файла (`mcc scp local.py <host>:/etc/host_checker/checks/check_kafka.py`), mcc создаст
  на хосте **директорию** с именем `check_kafka.py` и положит файл внутрь. Правильно:
  `mcc scp local.py <host>:/etc/host_checker/checks/` — файл скопируется с тем же именем.
  Если целевой файл уже существует и не перезаписывается — удалить его заранее через
  `expect + mcc ssh` (`rm -f /path/to/file`) и затем scp по директории.
- `SSL Handshake is not finished` — повторить через 1-2 сек (tunnel ещё не поднялся).
- `EOF на tar header` — опечатка в пути или файла не существует.

## mcc ssh + expect — выполнение команд

`mcc ssh <host>` открывает интерактивный шелл. Чтобы выполнить команду неинтерактивно,
оборачиваем в `expect` и шлём команду после приглашения `/# `:

```bash
expect -c '
set timeout 30
spawn mcc --local ssh <host>
expect "/# "
send "uptime; echo ===DONE===\r"
expect "===DONE==="
send "exit\r"
expect eof
' 2>&1 | tail -40
```

Ограничения:
- Сложные кавычки внутри `send` ломают парсер — лучше писать команду в файл на хосте
  через `cat > /tmp/x.sh << "EOF" ... EOF` и затем `bash /tmp/x.sh`.
- `sudo -u kafka bash -c "..."` с вложенными кавычками почти всегда ломается —
  использовать heredoc-трюк.
- `mcc scp` нестабилен на некоторых хостах (`SSL Handshake is not finished`) — для разовых
  команд быстрее `expect + mcc ssh`, чем scp.

Подробности и готовые шаблоны — `commands/run_commands.md`.

## Хосты и пути

| Что | Путь на хосте |
|---|---|
| Логи сервисов | `/mnt/logs/dbms/` (kafka-broker/controller/exporter, cruise-control) |
| Конфиги Kafka | `/opt/kafka/config/` (server.properties, client.properties) |
| SSL | `/opt/kafka/ssl/` (server.keystore.jks, server.truststore.jks) |
| Systemd | `/etc/systemd/system/kafka-*.service`, `cruise-control.service` |
| rscheck | `/etc/rscheck/` (kafka.conf.j2, modules/checkkafka.py) |
| host_checker | `/etc/host_checker/` (checks/check_kafka.py) |
| Prometheus JMX | `/opt/prometheus/` (kafka-2_0_0.yml, cruise-control.yml) |
| Cruise Control | `/opt/cruise-control/` (config/, libs/, dependant-libs/) |

## Структура скилла

- `SKILL.md` — этот файл.
- `commands/run_commands.md` — выполнение команд на хосте через `mcc ssh` + `expect` (шаблоны: одна команда, несколько подряд, сложные через heredoc, sudo -u kafka + env).
