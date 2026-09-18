# mdb-front sc (Backstage, dzen): хост 2 без обратного TCP до PG-хостов → KnexTimeoutError, crash-loop (2026-09-09)

## Симптом

- `2.mdb-nodejs.mdb-front.sc.idzn.ru` (Backstage mdb-backend, ns **dzen**,
  minion srvs1023, WAN v6 `2a00:b4c0:6c1::4e:0`) в crash-loop:
  `stderr.log` — `KnexTimeoutError: Knex: Timeout acquiring a connection.
  The pool is probably full` в `applyDatabaseMigrations` на старте
  (Migrator.latest). Приложение не слушает 7007 → rscheck
  `app-http-availability` fail → облако флапает RUNNING/STARTING и
  переразвертывает хост (runid менялся в процессе разбора; в это время
  `mcc ssh` отваливается `container state improper` / `not scheduling on
  a minion` / `Container ... is not found`).
- Конфиг: `/app/app-config.mdb.production.yaml`, подключение из env:
  `POSTGRES_HOST=1.db.mdb-etp-pgsql.rc.wan.idzn.ru`, `POSTGRES_PORT=7432`,
  `POSTGRES_USER=backstage`. env читается из `/proc/<pid>/environ`
  (`tr '\0' '\n'` или `grep -az POSTGRES /proc/[0-9]*/environ`).

## Root cause — сеть, не БД

- PG-кластер `mdb-etp-pgsql` (Stolon: pc=мастер, rc, uc) здоров:
  `pg_isready -h 127.0.0.1 -p 7432` — accepting, мастер pc.
- С хоста 2 все WAN-прокси `1.db.mdb-etp-pgsql.{rc,pc,uc}.wan.idzn.ru:7432`
  — TCP виснет (не refused). С соседа `1.mdb-nodejs.mdb-front.sc.idzn.ru`
  те же проверки проходят. Egress хоста 2 живой (https → 200).
- **tcpdump на 1.db.mdb-etp-pgsql.rc** (`-ni any host 2a00:b4c0:6c1::4e:0`):
  SYN от хоста 2 **приходит**, SYN-ACK **уходит** — и до хоста 2 не доходит
  (тот ретраит тот же SYN). Асимметрия обратного пути rc→sc, специфичная
  для адреса хоста 2.
- Порты 2379/6432/9187 на том же PG-хосте с хоста 2 тоже виснут →
  проблема не в порте 7432, а в связности PG-хосты → хост 2 в целом.

## Отправлено в поддержку

Сообщение в чат `One-cloud: поддержка и обратная связь`
(1094761@chat.agent, 2026-09-09, msg_id 7683573201214333375): просьба
проверить SG/plait хоста 2 в сравнении с соседом, с дампом
«SYN доходит, SYN-ACK теряется».

## Методические грабли разбора

- **tcpdump на целевом хосте при коннекте с проблемного** — самый быстрый
  способ локализовать асимметрию пути: SYNs пришли/SYN-ACKs ушли, дальше
  сеть. Параллельные expect-сессии: дамп в фоне (`&` + файл), коннект из
  второй сессии.
- **`/proc/net/sockstat` в porto-контейнере показывает общесистемные
  цифры миньона** (`TCP alloc 11547`), а `/proc/net/{tcp,tcp6}` — только
  netns контейнера (~83 сокета). `alloc` из sockstat — не признак утечки
  сокетов контейнера, не вести по нему расследование.
- `ss`/`iptables`/`nc` в контейнерах mdb-front/pg часто отсутствуют;
  слушать порты: `awk 'NR>1 && $4=="0A" {split($2,a,":"); print a[length(a)]}'
  /proc/net/tcp /proc/net/tcp6` (hex). TCP-пробы: `timeout 5 bash -c
  'echo > /dev/tcp/<host>/<port>'`.
- **Stolon-PG**: 7432 — stolon-proxy (не pgbouncer), `systemctl is-active
  postgresql` = inactive — норма (keeper управляет postgres сам);
  `pg_isready` без `-h` ходит через unix-сокет и может отвечать
  «no response» при живом TCP.
- `mcc logs` (через мастер) работает даже когда `mcc ssh` не открывается
  в фазах редеплоя — stderr/stdout/systemd/rscheck стримы читать им.
  `@console` у mdb-nodejs контейнера нет (только stdout.log/stderr.log).
- Tcl/expect повторное подтверждение: в `expect -c '...'` перед `$`, `[`,
  `"` ровно ОДИН бэкслеш (`\$`, `\[`, `\"`); `\\$` = бэкслеш + подстановка
  = рантайм-ошибка tcl на send. Сложное — сразу heredoc-скриптом.
