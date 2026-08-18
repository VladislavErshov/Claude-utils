# MDBSUP-4166: Offline partitions из-за удалённого broker 22026

**Дата**: 2026-07-24
**Кластер**: `spfrclustermdb-oneme-kafka` (Kafka 3.8.0, KRaft, 75 брокеров, RF=3, min.insync.replicas=2)

## Симптом

Grafana (range 1h): `offline=27`, `under-repl=14`, `at-min-isr=14`. Все broker-хосты AVAILABLE.
MBean на отдельных брокерах = 0 (метрика локальная, проблема кластерная).

`kafka-topics --unavailable-partitions` (через FQDN, не localhost) — 9 партиций с `Leader: none`:
```
Replicas: 22026,...    Isr: 22026
```
Broker 22026 (`26.broker...pc`) удалён из mdb-data, mcc не подключается. Был preferred leader и единственной ISR-репликой. `unclean.leader.election.enable=false` → controller не выбрал лидера из не-ISR → партиции offline.

## Фикс

**Этап 1 — unclean election**:
```bash
kafka-leader-election.sh --bootstrap-server <fqdn>:9092 --admin.config client.properties \
  --election-type unclean --all-topic-partitions
```
(`--admin.config`, не `--command-config`.) Все 9 партиций получили лидера.

**Этап 2 — reassign** для убирания 22026 из Replicas (23 партиции):
```bash
kafka-reassign-partitions.sh --bootstrap-server <fqdn>:9092 --command-config client.properties \
  --reassignment-json-file /tmp/reassign.json --execute
```
Без `--throttle` (с throttle падал с `TimeoutException` на `incrementalAlterConfigs`). `--verify` сразу = `completed` для всех.

## Грабли

1. `localhost:9092` → `SslAuthenticationException` (нет `localhost` в SAN). Только FQDN.
2. `--under-min-isr` убран в Kafka 3.8. Использовать `--at-min-isr-partitions` / `--under-replicated-partitions`.
3. `kafka-leader-election.sh` — `--admin.config`; `kafka-topics.sh` / `kafka-reassign-partitions.sh` — `--command-config`.
4. `--execute --throttle N` → timeout на `incrementalAlterConfigs`. Без `--throttle` работает.
5. `grep "^Topic:"` не матчит строки партиций (табуляция перед `Topic:`). `grep "Replicas:.*<id>"`.
6. tcl/expect + Python `[...]` ломается (command substitution). Python-скрипт отправлять через base64.

Полный разбор — `kafka-reassign-partiotions/history/MDBSUP-4166.md`.
