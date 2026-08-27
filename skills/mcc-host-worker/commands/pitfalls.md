# Известные грабли mcc

## Tcl/expect

### `[...]` ломает send

Tcl интерпретирует `[...]` как command substitution. `grep -E "0[23]:"` →
`invalid command name "23"`. Решения:
- переписать grep без character class: `grep "^2026-07-23 03:"` вместо `grep -E "0[23]:"`,
- escape: `\[23\]`.

### `$VAR` в send-строках

Tcl интерполирует `$VAR` как tcl-переменную. Экранировать как `\$VAR`. Особенно в
heredoc-командах:

```bash
send "cat > /tmp/x.sh << EOF\nBS=...\n... \$BS ...\nEOF"
```

### Python с list comprehensions

Python-код с `[x for x in ...]` ломает `expect -c '...'` (tcl `[...]`). Решение —
закодировать python-скрипт в base64 локально, отправить через
`echo '<b64>' | base64 -d > /tmp/script.py`:

```bash
cat /tmp/reassign.json | base64 | tr -d '\n' > /tmp/reassign.json.b64
# в expect:
send "echo '<base64-строка>' | base64 -d > /tmp/reassign.json\r"
```

### Вложенные кавычки в `sudo -u <svc> bash -c "..."`

Ломают парсер. Обход — heredoc в файл + `bash /tmp/x.sh`.

### `$(date +%s)` в send

tcl/expect не разворачивает `$(...)`. Использовать фиксированный суффикс или
expect-экранирование.

## ANSI-коды ломают grep

`grep` подсвечивает совпадения ANSI-кодами `[01;31m[K`. Фильтровать:

```bash
sed -E 's/\x1b\[[0-9;]*[mK]//g'
```

Просто `[m` без `K` не помогает — нужно убирать оба.

## `SSL Handshake is not finished` / `Too early`

Туннель к minion-у не успел подняться. Просто повторить команду (бывает через 1-2
ретрая). Между заходами `sleep 2-3`. Иногда `Too early` — повторить через 5 сек.

Альтернатива при нестабильном scp — base64 через ssh.

## `mcc scp` — `EOF на tar header`

`failed to read downloaded archive header: EOF` — файл не существует по указанному
пути, либо опечатка в пути (`/mnt/log/dbms` вместо `/mnt/logs/dbms`).

## `mcc scp` (загрузка НА хост) молча не заливает файл

Симптом: scp выходит без ошибок, но файла на хосте нет (проверено на прод-Kafka,
MDBSUP-4899). Заливка — только base64 через `mcc ssh + expect`, чанками по ~800 символов
(генератор expect-файла — [scp.md](scp.md) → «Заливка файла base64-чанками»; разборы —
`kafka-cluster-inspector/history/MDBSUP-4895-2026-08-26.md`, `MDBSUP-4899-2026-08-27.md`).

## `mcc sshexec` — `414 URI Too Long` на длинной команде

sshexec кладёт команду в URL — base64-пейлоад уже на ~8KB отваливается
(`expected 101 Switching Protocols, got 414 URI Too Long`). Для больших данных —
expect + mcc ssh (генератор чанковой заливки — [scp.md](scp.md)).

## Команды через mcc показывают ресурсы миньона, а не целевого хоста

`nproc`, `lscpu`, `cat /proc/cpuinfo`, `cat /proc/uptime`, `cat /proc/loadavg`, выполненные
через `mcc ssh`/`mcc sshexec` на хосте, возвращают ресурсы **mcc-миньона** (прокси-узла),
а не самого хоста. Не считать по ним число ядер/uptime/load целевого хоста — брать их из
метрик (Prometheus/VictoriaMetrics, `one_cloud_cpu_cores_value`), mdb-data spec /
OneCloud API (`mcc instances`) или UI mdb-data. Как проявляется на Kafka —
`kafka-metrics-investigator/commands/check_metrics.md` (грабля nproc).

## Tcl expect — `[...]` в send = command substitution

Квадратные скобки в команде (`grep -oE '[0-9,]+'`, `\x1b[[0-9;]`) роняют expect
(`invalid command name "0-9,"`). Сложные команды — heredoc-скриптом на хосте
(см. commands/ssh.md), не инлайн в send.

## `NamespaceMissingException`

На scp/sshexec добавить `-n infra`. На dev-кластерах (mcc v0.29.0) scp обычно работает
и без флага.

## `mcc sshexec` таймаутит к cloud-ops узлам

TLS handshake timeout — не использовать `sshexec` к cloud-ops. Только
`expect + mcc ssh`.

## `Task Instance ... is not scheduling on a minion, please start it first`

Хост остановлен в облаке (плановые работы / перезагрузка / падение cloud-мастера).
Фикс: зайти в облако, нажать start. Если RUNNING но UNAVAILABLE долго — смотреть логи.

## `Container not found` / `not scheduling on a minion` при сетевых проблемах

Хост временно потерял сеть (ping 100% loss, порты 22/9093 timeout,
`mcc ssh` → `Container not found`). Восстанавливается сам через ~5 минут.
Инфраструктурная проблема, не KRaft/сервисная.

## Self-update мусор в выводе

```
*** notice: using autodetected cloud kc
Self-update failed, try to use --local flag to use local copy: failed to apply update: library method failed: open /usr/local/bin/.mcc.new: permission denied
```

Без `--local` mcc пытается self-update на каждый вызов — медленно и шумно. Решение:
всегда `mcc --local`.

## `mcc ssh` обрывается при быстром повторном подключении

`SSL Handshake is not finished`. Между заходами `sleep 2-3`.

## JMX exporter (8080) — curl --max-time 5 даёт 000

JMX exporter отдаёт ~674KB, при `--max-time 5` curl не успевает скачать и возвращает
`000` — выглядит как мёртвый, но на самом деле жив. Минимум `--max-time 10`.

## `localhost` не работает для bootstrap-server

`kafka-topics.sh --bootstrap-server localhost:9092` падает с
`SslAuthenticationException: No subject alternative DNS name matching localhost found`.
Сертификат брокера не содержит `localhost` в SAN. Использовать FQDN брокера.

## Промпт

`/# ` (слэш-решётка-пробел). Regex для expect: `/#\s*` или просто строка `/# `.
`\[#?\$\]` — НЕ срабатывает.
