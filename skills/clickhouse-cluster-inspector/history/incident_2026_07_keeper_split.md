# Инцидент 2026-07: Keeper split-brain в кластере sellgate-media-infra-ch

Кластер: `sellgate-media-infra-ch`
Хосты (на момент инцидента):
- `1.keeper.sellgate-media-infra-ch.hc.one-infra.ru` (Raft id 3)
- `1.keeper.sellgate-media-infra-ch.kc.one-infra.ru` (Raft id 2)
- `1.keeper.sellgate-media-infra-ch.pc.one-infra.ru` (Raft id 1)
- `1.shard1-db.sellgate-media-infra-ch.{hc,kc,pc}.one-infra.ru` — CH-шард

Версия: ClickHouse 24.3.14.35.

## Симптомы

- В UI mdb-data: `1.keeper...kc.one-infra.ru` — `unknown/unknown` (всё unknown),
  два других кипера — `AVAILABLE` но `UNKNOWN` (роль не определена).
- CH-шард `1.shard1-db...kc.one-infra.ru` — `AVAILABLE MASTER`, но в
  `clickhouse-server.err.log` копится:
  ```
  Code: 202. DB::Exception: Too many simultaneous queries. Maximum: 1000.
  (TOO_MANY_SIMULTANEOUS_QUERIES)
  ```
- `clickhouse-keeper-client -h 127.0.0.1 -p 2181 -q ruok` на hc и pc возвращает:
  ```
  Code: 999. Coordination::Exception: Keeper server rejected the connection during
  the handshake. Possibly it's overloaded, doesn't see leader or stale: while receiving
  handshake from ZooKeeper. (KEEPER_EXCEPTION)
  ```
- `mcc ssh 1.keeper...kc.one-infra.ru` →
  `Task Instance ... is not scheduling on a minion, please start it first` — kc-кипер
  остановлен в облаке.

## Диагностика

### 1. Проверка systemd на киперах

```bash
expect -c '
set timeout 30
spawn mcc --local ssh 1.keeper.sellgate-media-infra-ch.hc.one-infra.ru
expect "/# "
send "systemctl is-active mdb-clickhouse-keeper; ...\r"
'
```
- hc: `active` (с 2025-04-24)
- pc: `active` (с 2025-04-24)
- kc: host stopped в облаке

Процесс жив, но Raft-подсистема не активна.

### 2. Логи Keeper (`/mnt/logs/dbms/clickhouse-keeper.log`)

**hc (id 3)** — крутится в candidate, не может стать лидером:
```
2026.07.23 13:53:41.450790 <Information> RaftInstance: [PRE-VOTE INIT] my id 3, my role
  candidate, term 922834, log idx 7019939, log term 1, ...
2026.07.23 13:53:41.450795 <Warning> RaftInstance: failed to send prevote request:
  peer 2 (1.keeper...kc...:9444) is busy
2026.07.23 13:53:41.452682 <Information> RaftInstance: [PRE-VOTE RESP] peer 1 (O),
  term 922834, ...  ← pc ответил на pre-vote
2026.07.23 13:53:41.455647 <Information> RaftInstance: [VOTE RESP] peer 1 (X),
  ... granted 1, responded 2, num voting members 3, quorum 2  ← pc denied
2026.07.23 13:53:42.026582 <Warning> KeeperTCPHandler: Ignoring user request,
  because the server is not active yet
```

**pc (id 1)** — тоже candidate, не может отправить pre-vote на hc:
```
2026.07.23 13:54:10.328609 <Information> RaftInstance: [PRE-VOTE INIT] my id 1,
  my role candidate, term 922854, log idx 11371148, log term 5, ...
2026.07.23 13:54:10.328614 <Warning> RaftInstance: failed to send prevote request:
  peer 3 (1.keeper...hc...:9444) is busy
2026.07.23 13:54:10.328621 <Warning> RaftInstance: failed to send prevote request:
  peer 2 (1.keeper...kc...:9444) is busy
2026.07.23 13:54:10.804244 <Information> RaftInstance: [PRE-VOTE REQ] my role candidate,
  from peer 3, log term: req 1 / mine 5   ← получил pre-vote от hc
2026.07.23 13:54:10.804277 <Information> RaftInstance: pre-vote decision: O (grant)
2026.07.23 13:54:10.807630 <Information> RaftInstance: [VOTE REQ] my role follower,
  from peer 3, log term: req 1 / mine 5
2026.07.23 13:54:10.807639 <Information> RaftInstance: decision: X (deny), term 922855
```

### 3. Анализ состояния

| Keeper | log idx | log term | role |
|---|---|---|---|
| pc (id 1) | 11,371,148 | 5 | candidate |
| hc (id 3) | 7,019,939 | 1 | candidate |
| kc (id 2) | — | — | stopped в облаке |

- hc отстаёт от pc по логу (idx 7M < 11M, term 1 < 5).
- pc denies vote для hc: `log term: req 1 / mine 5` (по Raft, voter с более свежим логом
  отказывает кандидату с устаревшим).
- pc не может отправить pre-vote на hc: `peer 3 is busy`. **При этом**:
  - DNS резолвится: `getent hosts 1.keeper...hc...` → `fd00:b4c4:c111:101:1:0:1995:0`
  - Ping работает: `0% packet loss, ~1.8ms`
  - TCP к `hc:9444` (raft-порт) проходит: `bash -c "echo > /dev/tcp/.../9444"` OK
  - hc → pc работает (pc получает pre-vote от peer 3).
  
  Это **асимметрия**: pc→hc "is busy" на уровне Raft, при живой сети. Классический симптом
  зависшего internal Raft-connection-state в ClickHouse Keeper 24.3.

### 4. Timeline

Проблема идёт **минимум с 2026-07-09 17:49** — в `clickhouse-keeper.log.1.gz` на pc уже
9 июля те же числа (hc idx 7,019,939 / term 1, pc idx 11,371,148 / term 5). Т.е. кластер
киперов мёртв уже 2+ недель, не свежий сбой.

### 5. Warning про "Election Timer is never started"

Пользователь упоминал warning:
```
<Warning> RaftInstance: Election Timer is never started but is requested to stop,
protential a bug
```
Известный баг ClickHouse Keeper 24.3. На момент разбора в логах pc этого warning'а не было
(`grep -c "Election Timer is never started" /mnt/logs/dbms/clickhouse-keeper.log*` = 0 по
всем ротациям), но поведение полностью с ним сходится. Возможно был в логах раньше или на
другом хосте.

## Корневая причина

Два фактора:
1. **kc-кипер остановлен в облаке** (cloud-level, не CH-проблема). Кворум 2/3 достаточен,
   но pc и hc не могут договориться.
2. **Зависший Raft-state на pc** в отношении peer 3 (hc): pc считает соединение к hc "busy",
   хотя сетевой TCP к `hc:9444` проходит. Это не сетевая проблема, а internal state машины
   Raft-подсистемы Keeper. В результате pc не может собрать 2 голоса (свой + hc) и стать
   лидером.

Следствие: нет лидера → `Keeper server rejected the connection during the handshake` для
CH-клиентов → запросы к Replicated-таблицам висят → `TOO_MANY_SIMULTANEOUS_QUERIES`.

## Фикс

Дежурная дока («Дежурство MDB: Clickhouse»):
- «Хост не поднялся после работ в облаке» → зайти в облако, нажать start.
- «Не доступен зукипер — Иногда бывает, что 2+/3 RUNNING UNAVAILABLE. В таком случае обычно
  помогает рестарт всех.»

**Что сработало в этом инциденте**:
1. Поднять kc-кипер в облаке (start).
2. `systemctl restart mdb-clickhouse-keeper` на pc — не сбросил зависший Raft-state.
3. **Дропнуть диск Keeper + рестарт** — помогло. Keeper поднимает лог с других нод кворума
   и возвращается в строй.

⚠️ Дроп диска Keeper — крайняя мера. После дропа нода стартует с пустым логом и подтягивает
состояние с лидера. Без живого лидера в кворуме это не сработает — поэтому сначала поднять
kc (или убедиться что 2/3 живы и могут выбрать лидера).

## Шаблон проверки при подобных симптомах

1. `mcc ssh` на каждый keeper — проверить, что хост вообще запущен в облаке.
2. `systemctl is-active mdb-clickhouse-keeper` — процесс жив?
3. `clickhouse-keeper-client -h 127.0.0.1 -p 2181 -q ruok` — должно вернуть `imok`.
   Если `KEEPER_EXCEPTION` → нет лидера.
4. `tail /mnt/logs/dbms/clickhouse-keeper.log` — grep по:
   - `Election timeout, initiate leader election`
   - `failed to send prevote request: peer N is busy`
   - `[VOTE REQ] ... decision: X (deny)` + `log term: req X / mine Y`
   - `KeeperTCPHandler: Ignoring user request, because the server is not active yet`
5. Если "is busy" при живой сети (ping/TCP-9444 OK) → зависший Raft-state → рестарт keeper.
6. На CH-стороне: `system.zookeeper_connection`, `count() FROM system.processes`,
   `clickhouse-server.err.log` на `TOO_MANY_SIMULTANEOUS_QUERIES` и `KEEPER_EXCEPTION`.

## Чек-лист «quick check» для будущего

```bash
# Один скрипт на keeper-хосте:
echo "=== systemd ==="; systemctl is-active mdb-clickhouse-keeper
echo "=== ruok ==="; clickhouse-keeper-client -h 127.0.0.1 -p 2181 -q ruok 2>&1 | tail -2
echo "=== raft state ==="; tail -200 /mnt/logs/dbms/clickhouse-keeper.log \
  | grep -E "PRE-VOTE INIT|VOTE REQ|decision:|is busy|BECOME LEADER|BECOME FOLLOWER" | tail -15
```
