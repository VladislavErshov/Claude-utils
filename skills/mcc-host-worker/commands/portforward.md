# Проброс портов, Teleport SSH-node, JVM-диагностика, lifecycle

Команды, которые раньше скилл считал «не покрытыми mcc». Все требуют namespace
(`-n infra`).

## `mcc tp-port-forward` — проброс порта на локальную машину (Teleport)

Проброс TCP-порта инстанса на localhost через Teleport TCP App. **Закрывает старое
ограничение** «проброс портов интерфейсом не предусмотрен» — теперь предусмотрен.
Требует доступ к https://tp.odkl.io.

```bash
mcc --local -n infra tp-port-forward <host>:9092 --local-port 9092
```

- `<instance:port>` — FQDN + порт сервиса (Kafka `9092`, Postgres `6432`/`5432`, и т.п.).
- `-p/--local-port` — локальный порт (если не задан — выберет сам).
- `-u/--username` — кому выдать доступ (по умолчанию login из client-сертификата).
- `-o/--create-only` — только создать Teleport App без форвардинга.

После форвардинга можно ходить локальным клиентом (`kafka-*.sh --bootstrap-server
localhost:<local-port>`, `psql -h localhost -p <local-port>`, `redis-cli`) — но помни
про SAN сертификата: для TLS-сервисов имя `localhost` может не совпасть с SAN, тогда
нужен `--create-only` + собственный туннель или обращение по FQDN.

## `mcc tp-create-ssh-node` — временная SSH-нода

```bash
mcc --local -n infra tp-create-ssh-node <host>:22
```

Создаёт временную VM SSH-node для доступа (порт по умолчанию 22). Тоже через Teleport
(tp.odkl.io). Альтернатива `mcc ssh`, когда нужен полноценный ssh-клиент/агент.

## JVM-диагностика (Kafka/Java-сервисы)

Снимают дампы с Java-процесса в контейнере без ручного захода:

```bash
mcc --local -n infra jstack <host>            # thread dump; --force/-F — через ptrace
mcc --local -n infra jmap   <host>            # heap histogram
mcc --local -n infra profile <host>           # CPU / Allocation профиль
mcc --local -n infra perf   <host>            # perf по всем процессам контейнера
```

`<instance_name>` — полное имя инстанса (FQDN хоста). Полезно для Kafka broker/controller
(JVM) при разборе залипаний/GC/CPU.

## Lifecycle инстанса: start / stop / restart

pitfalls.md советует «зайти в облако, нажать start» — то же самое делается из mcc:

```bash
mcc --local -n infra start   "<host-prefix или service>"     # напр. 1.broker.<cluster>
mcc --local -n infra stop    "<host>" [--now]                 # --now = без graceful
mcc --local -n infra restart "<pattern>" [-m <min_running>] [-p <pause>]
```

- `restart` принимает паттерн — может задеть **несколько** инстансов/сервисов. Указывай
  точный FQDN, чтобы не рестартнуть весь кластер.
- `-m/--min_running` — сколько реплик держать живыми во время rolling-рестарта,
  `-p/--pause` — пауза между репликами. Применяется только к текущей операции.
- `stop --now` — форс без graceful shutdown; для Kafka/БД предпочтителен обычный `stop`.

> ⚠️ start/stop/restart и `jstack --force`/`perf` — операции с побочными эффектами на
> живой сервис. Согласовывай с пользователем перед запуском на прод-хостах.
