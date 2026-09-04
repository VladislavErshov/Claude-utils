# Lifecycle хоста: stop → delete volumes → start (+ purge)

Пересоздание хоста с новыми дисками (битый конфиг на диске, LOST_MINION, сломанный
sysconfig и т.п.). Порядок обязателен: сначала stop сервиса, иначе delete/start падают.

Все команды: `mcc --local -n infra -c <dc> <cmd>` (без `-n infra` →
NamespaceMissingException; `status` по имени сервиса может требовать `-c <DC>`).

Типовой вход в этот флоу — `LOST_MINION` (маркеры и грабли:
[query.md](query.md#маркеры-лежащих-хостов-lost_minion)): миграция volume'ов с
мёртвого минионa невозможна (`No devices found to make MIGRATING`), остаётся
пересоздание. ⚠️ Если лежащий хост — текущий мастер БД (Redis: `sentinel
get-master-addr-by-name`), сначала failover на живого соседа — дроп дисков мастера =
потеря данных.

## 0. Диагностика перед работой

```bash
# инстансы кластера/сервиса (state, availability, minion)
mcc --local -n infra -c <dc> instances "1.controller.<cluster>.<dc>.one-infra.ru" -f yaml
# volumes сервиса (uuid, state, minion) — для delete
mcc --local -n infra -c <dc> tool_status --type storage "<queue>/<role>" -f yaml
```

- `mcc status`/`instances` по паттерну может отдавать EntityNotFoundException, хотя хосты
  есть (бывает у сервисов не в корне namespace). Рабочий обход: `instances "%"` с JS-фильтром:
  ```bash
  mcc --local -n infra -c pc instances "%" -F "String(host).indexOf('<cluster>')>=0" -f xargs:host
  ```
  В фильтре переменная называется `host` (строка) — `fqdn` нет, `host.includes` падает
  (`host.includes is not a function`, т.к. это Java-строка, не JS).
- fullQueue/queue кластера берётся из прод-БД: `SELECT params->>'fullQueue' FROM one_cloud_meta WHERE cluster_id='<uuid>';`
- ⚠️ Перед delete проверить в прод-БД, что нет RUNNING-операций по кластеру.

## 1. `stop` сервиса

```bash
mcc --local -n infra -c <dc> stop "<service>"   # service = controller.<cluster> и т.п.
```

Поллим до STOPPED/FINISHED:
```bash
mcc --local -n infra -c <dc> instances "<FQDN>" -f yaml | grep -E "^  state:"
```

## 2. `delete` volumes

⚠️ Storage общий на все реплики роли — **не** удалять весь storage, только volumes
нужного инстанса, по списку UUID из `tool_status` (шаг 0). По UUID `--state` НЕ указывать
(`ValidationException: State should not be used specifying volume by uuid`).

```bash
mcc --local -n infra -c <dc> delete "<uuid1>,<uuid2>" -f yaml
```

### Уравнение-подтверждение (автоматизация)

mcc требует решить уравнение `Input evaluated value for (attempt 0/3): 9-7` — оно
**меняется между запусками**, ответ через `echo | mcc` НЕ проходит (mcc молчит в pipe).
Рабочая автоматизация — **pexpect** (Tcl expect из скилла не взялся):

```python
import pexpect, sys
cmd = 'mcc --local -n infra -c <dc> delete <uuid1>,<uuid2> -f yaml'
p = pexpect.spawn('/bin/zsh', ['-c', cmd], timeout=60, encoding='utf-8')
p.logfile_read = sys.stdout
p.expect(r'attempt 0/3\): (\d+)\s*(mod|[-+*/%])\s*(\d+)')   # бывает и `N mod M` (MDBSUP-4923)
a, op, b = p.match.groups()
ans = int(a) % int(b) if op == 'mod' else int(eval(f"{a}{op}{b}"))
p.sendline(str(ans))
p.expect(pexpect.EOF, timeout=90)
```

Ответ: "Volumes contain data. New empty volume will be created." → "A total of N shards
deleted to free up ...". Диски удаляются вместе с данными (бэкап KRaft-meta при
необходимости — ДО delete).

После delete volumes/инстансы storage становятся `state: NEW`.

## 3. `start` сервиса

```bash
mcc --local -n infra -c <dc> start "<service>"
```

Облако аллоцирует новые volumes по манифесту storage и деплоит инстанс (возможно на
другом миньоне). Поллим `instances`/`tool_status --type task` до RUNNING (~минуты,
стадии DEPLOYING: Waiting sandbox / Configuring ACL). Возможны отскоки
FINISHED→DEPLOYING — это нормальный цикл деплоя, ждать дальше.

## 4. `purge` storage — освободить квоты кластера

После запуска сервиса вызвать в storage purge ALL — иначе старые записи держат квоты
(vCPU/MEM/disk) кластера:

```bash
mcc --local -n infra -c <dc> purge "<queue>/<role>" all
```

(например `... purge "trg-190191-...-kafka.datatransfer.db.production.mdb.prod/controller" all`;
`mcc purge <queue>/<role> <minion>` — точечный вариант. "none to purge" = уже чисто.)

## 5. Верификация

```bash
mcc --local -n infra -c <dc> instances "<FQDN>" -f yaml   # state: RUNNING, availability уходит из UNAVAILABLE
mcc -n infra sshexec -n infra "<FQDN>" "systemctl is-active kafka-<role>"
```

В UI MDB метрики хоста уходят из unknown; в логе сервиса — маркер успешного старта
(Kafka: `Kafka Server started`).

## Грабли

- `mcc wait` не имеет `--state`; синтаксис `mcc wait <name> <value>` — проще поллить.
- Состояние `state: FINISHED` у instances после stop — норма (это «остановлен»).
- KRaft/Sentinel-кворумы: на время работ кворум деградирует (2/3) — допустимо, но
  не затягивать.
- history-кейсы: MDBSUP-4867 (LOST_MINION, удаление volumes), MDBSUP-4832 (пустой
  /etc/sysconfig/kafka после upsertSysconfig → пересоздание хоста рендерит конфиг заново).
