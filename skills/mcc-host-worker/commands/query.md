# `mcc instances` / `status` / `logs` — интроспекция без ssh

Эти команды читают состояние из cloud-master и **не требуют захода на хост** (нет
expect, нет промпт `/# `). Все требуют namespace: без `-n <ns>` →
`NamespaceMissingException`. Для MDB-хостов namespace — `infra`.

⚠️ **Облака привязаны к ДЦ** (`~/.mccloud/clouds_info.json`, `defaultCloud` обычно `ic`):
без `-c <dc>` `instances`/`status` ищут только в дефолтном облаке и падают
`EntityNotFoundException: No instances found` для кластеров в других ДЦ.
Для хостов в hc/kc/pc/rc/dc/ec/... всегда добавляй `-c <dc>`:
```bash
mcc --local -n infra -c rc instances "%.broker.<cluster>.rc%" -f yaml
```

## `mcc instances` — перечисление хостов по паттерну

Заменяет хардкод-циклы `{1..75}` — реальный список инстансов из master.

```bash
mcc --local -n infra instances "%.broker.<cluster>%" -f xargs:host
```

Вывод (проверено на `rescale-mdbdev-kafka`):

```
1.broker.rescale-mdbdev-kafka.pc.one-infra.ru
2.broker.rescale-mdbdev-kafka.pc.one-infra.ru
3.broker.rescale-mdbdev-kafka.pc.one-infra.ru
```

- Паттерн: `%` — wildcard, `{1-2,3-15}.focky%%` — диапазоны, можно IP или runid.
- `-F "<js>"` — доп. фильтр JS-кодом: `-F "state=='RUNNING'"`, `-F "minion=='srvd1234'"`.
- `-o "<js>"` — трансформация результата (`-o "host"`).
- `-f xargs:host` — плоский список для скармливания в цикл; `-f table` / `yaml` / `json`.
- Грабля: `-o "host" -f table` падает `don't know how to make table from []string` —
  для одного поля используй `-f xargs:host`, не `table`.

Скормить результат в expect-цикл:

```bash
for host in $(mcc --local -n infra instances "%.broker.<cluster>%" -f xargs:host); do
    echo "=== $host ==="
    # expect-обёртка из ssh.md
done
```

## `mcc status` — состояние сервиса/инстанса

```bash
mcc --local -n infra status "broker.<cluster>%" -f table
```

Даёт `state` (RUNNING/STOPPED), `outcome`, `availability` (RESERVED/…), кто и когда
submitted/updated. Полезно перед ssh — понять, жив ли хост, не остановлен ли сервис.
`--type` (namespace/service/storage/shard/…) обычно автодетектится, `-p 1min|5min|15min` —
период утилизации.

Грабли:
- `mcc status "<FQDN-хоста>"` падает EntityNotFoundException — status принимает имя **сервиса**
  (`controller.<cluster>`), не FQDN. По FQDN хоста используй `mcc instances "<полный FQDN>"`
  (с `%`-паттерном или без) — покажет state/outcome/outcome_text конкретного инстанса.
- `mcc status "<service>"` может показывать сервис целиком (state STARTING и пр.), а не
  конкретные хосты — для состояния хостов бери `instances`.

## Мусорные progress-сообщения облака (НЕ диагностический признак)

`progress`/`reported_progress` у инстанса и вытекающие `availability=RESERVED/PREFAIL` могут
нести plait-предупреждения, которые **не отражают состояние хоста** — хост при этом жив:

- `there are NNNNN subnets for peer plv6-i-sg_onecloud-infra_**.db.production.mdb.prod: recommended value is 5000`
- `No IPs matched by pl-i-sg_ehot_balancer-...` / `pl*-*-sg_onecloud-dzen_vmagent...`

Это шум plait (много субнетов у пира / нет IP в plait-группах). MDBSUP-4938: uc-контроллер
с таким progress'ом был фактически жив — `state=RUNNING`, IP выданы (v4+v6), minion отвечал.
Источник истины о живости хоста: `state`, `ip`/`network`, `minion`. На progress при
диагностике зависших операций **не опираться** (но: waiter'ы mdb-processing могут сами
ждать availability — тогда шум plait блокирует операцию, см. jira-mdbsup-solver).

## Маркеры лежащего хоста (LOST_MINION)

`mcc instances` по конкретному FQDN:

```
state=FINISHED  outcome=LOST_MINION  outcome_text="Unreported by minions"
               или "rejected required storage's minion: not running"
```

Миньон (VM) умер, диск/сервис остались в облаке. В UI MDB симптомы — метрики хоста
`unknown`. Лечение — полный флоу пересоздания хоста: [commands/lifecycle.md](lifecycle.md).

## `mcc log-streams` / `mcc logs` — логи контейнера через master

Без ssh/scp достать логи. Сначала список потоков:

```bash
mcc --local -n infra log-streams "<host>"
```

Типичные потоки: `@console` (stdout+stderr), `rscheck.log`, `confp.log`, `systemd.log`,
`bash.log`, `vault-pki.log`, `logrotate.log`, `rsyslogd.log`.

```bash
mcc --local -n infra logs "<host>" "confp.log" --lines 50
```

Грабли:
- **`logs` работает в режиме follow** (не завершается сам, стримит новые строки). В
  скриптах запускай в фоне и убивай, либо ставь общий таймаут — иначе висит.
- `@console` есть не на всех хостах: `Log file associated with stream '@console' ...
  doesn't exist`. Тогда бери именованный поток из `log-streams`.
- `--lines -1` — с начала файла; `--container <name>` — если в поде несколько контейнеров.
- Для больших/архивных логов Kafka (`kafka-broker.out.log` и т.п.) — по-прежнему scp,
  этот механизм для стрим-потоков контейнера.
