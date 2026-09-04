# Известные проблемы ClickHouse-кластеров

**Канон — Confluence «Дежурство MDB: Clickhouse», секция «Проблемы»** (SSOT):
https://confluence.vk.team/pages/viewpage.action?pageId=1348619034

Покрывает: broken parts при старте (`force_restore_data` + лимиты
`max_suspicious_broken_parts(_bytes)` в PMS), чистку detached parts (SQL генерации
drop-команд + удаление файлов), «логи в трёх местах», реплика долго переналивается
(`distributed_replica_error_half_life/cap` + `<priority>` реплик в конфиге),
part intersects previous part, сломалась схема на реплике (`restore_ch.log`,
`SYSTEM DROP REPLICA ... FROM ZKPATH`), сброс пароля default (vault + sha256 в PMS).
Вики живая — править там; ниже только наши разборы и дополнения.

## Хост не поднялся после работ в облаке

**Симптом**: хост в UI `unknown`/`UNAVAILABLE`, подключение через скилл
[`mcc-host-worker`](../../mcc-host-worker/SKILL.md) (`mcc ssh`) возвращает:
```
*** ERROR (ServiceValidationException): Task Instance <host> is not scheduling on a minion,
please start it first
```

**Причина**: инстанс остановлен в облаке (плановые работы / перезагрузка / падение cloud-мастера).

**Фикс**: зайти в облако, нажать **start**. Чаще всего помогает.

Если хост в `RUNNING`, но `UNAVAILABLE` долгое время → смотреть логи CH/Keeper.

## Не доступен Keeper (ZooKeeper)

**Симптомы**:
- В UI 2+/3 keeper-хостов `RUNNING UNAVAILABLE`.
- CH в `clickhouse-server.err.log`: `Code: 999. Coordination::Exception: Keeper server rejected
  the connection during the handshake. Possibly it's overloaded, doesn't see leader or stale`.
- `system.zookeeper_connection` пустая / ошибка.
- Запросы висят, копятся `TOO_MANY_SIMULTANEOUS_QUERIES` (Maximum: 1000).

**Причина**: нет Raft-кворума среди Keeper-нод. Подробнее про диагностику —
`../history/incident_2026_07_keeper_split.md`.

**Что проверить**:
1. `systemctl is-active mdb-clickhouse-keeper` на каждом кипере — процесс может быть active,
   но Raft-подсистема не активна (`server is not active yet` в логах).
2. `clickhouse-keeper-client -h 127.0.0.1 -p 2181 -q ruok` — должно вернуть `imok`.
3. В логе кипера `clickhouse-keeper.log`:
   - `Election timeout, initiate leader election` — кворум не собирается.
   - `failed to send prevote request: peer N (...:9444) is busy` — зависший Raft-state к peer N.
   - `[VOTE REQ] ... decision: X (deny)` — кандидат отстаёт по логу (`log term: req X / mine Y`, X<Y).
   - `KeeperTCPHandler: Ignoring user request, because the server is not active yet` — кипер
     отклоняет клиентские коннекты, т.к. сам ещё не выбрал лидера.

**Фикс**:
- По дежурной доке: «2+/3 RUNNING UNAVAILABLE — обычно помогает **рестарт всех**».
- Один из киперов остановлен в облаке → сначала поднять его в UI.
- Рестарт зависшего кипера: `systemctl restart mdb-clickhouse-keeper`.
- Если рестарт не помогает — дропнуть диск Keeper и рестарт (крайняя мера, Keeper поднимет
  лог с других нод). См. `../history/incident_2026_07_keeper_split.md`.

**Network check** (если есть подозрение на сеть, а не на Raft-state):
```bash
# С keeper-хоста:
getent hosts 1.keeper.<cluster>.<dc>.one-infra.ru           # DNS
ping -c 2 <peer-keeper-hostname>                             # L3
timeout 3 bash -c "echo > /dev/tcp/<peer>/<raft_port>"       # L4 к raft_port 9444
```
Если всё OK, а в логе `peer N is busy` — это **Raft-state**, не сеть.

## TOO_MANY_SIMULTANEOUS_QUERIES — это симптом, не причина

`Code: 202. DB::Exception: Too many simultaneous queries. Maximum: 1000.` в
`clickhouse-server.err.log` почти всегда = следствие недоступности Keeper. Запросы,
которым нужны ZK-операции (большинство запросов к Replicated-таблицам), висят и копятся.

**Что делать**: не повышать лимит, а чинить корень — Keeper. Проверить `system.zookeeper_connection`,
логи Keeper. См. раздел «Не доступен Keeper» выше.

## Чистка detached parts по всему кластеру (mcc-перебор)

SQL генерации drop-команд — в вики (секция «Чистим detached parts»). Почистить весь
кластер файлово: перебрать хосты × ДЦ через скилл
[`mcc-host-worker`](../../mcc-host-worker/SKILL.md) (`mcc sshexec`; шаблон перебора —
в скилле). Команда на хосте:
`find /var/lib/clickhouse/1/store -path '*/detached/*' -delete`. Параметры шаблона:
`cluster_name`, `project`, `clouds`, `shard_start..shard_end`, шаблон хоста
`1.shard${i}-db.${cluster_name}-${project}-ch.${cloud}.<domain>.ru`.
