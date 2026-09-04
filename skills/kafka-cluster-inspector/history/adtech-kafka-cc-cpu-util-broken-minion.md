# adtech-kafka (ok/vk-events): BROKEN minion ломает BROKER_CPU_UTIL → ребаланс CC падает

**Дата**: 2026-08-27
**Кластеры**: `ok-events-adtech-kafka`, `vk-events-adtech-kafka` (брокеры kc=21001, pc=22001, hc=20001, ec=23001/24001; вне mdb-data — `mcc instances '*broker.<cluster>*'` не находит, искать через `instances "%" -F "String(host).indexOf(...)"`)
**CC-хосты**: `1.cruise.{ok,vk}-events-adtech-kafka.pc.one-infra.ru`

## Симптом

Ребалансировка CC падает на обоих кластерах: `NotEnoughValidWindowsException: 0 valid windows`
(ок-events 96/131=74% покрытия, vk-events 85/130=65% — оба < minValidEntityRatio=0.95).

## Диагностика

1. CC-лог: `Skip generating metric sample for broker 21001 ... missing [BROKER_CPU_UTIL]`
   (маркер из MDBSUP-4614 — виновник назван в WARN-строке).
2. Маппинг brokerId→host — НЕ по стандартной таблице ДЦ (21001 здесь = **kc**, не ic!):
   надежнее через `kafka-broker-api-versions.sh --bootstrap-server <FQDN>:9092` с broker-хоста
   (localhost:9092 падает `SSL handshake ... no SAN localhost`; command-config
   `/opt/kafka/config/client.properties`).
3. Корень: на брокерах 21001 JVM `OperatingSystemMXBean` = `SystemCpuLoad=-1.0,
   ProcessCpuLoad=-1.0` (Jolokia 7777, `java.lang:type=OperatingSystem`) — JVM не может
   посчитать CPU load. Остальные брокеры (hc/pc/ec) отдают валидные значения при тех же
   образе/JVM 17.0.15/Kafka 3.8.0/ядре — т.е. проблема **миньона**, не конфига.
4. Рестарт `kafka-broker` НЕ чинит (проверено) — только переезд хоста.

## Фикс — миграция хоста (mcc lifecycle, см. mcc-host-worker/commands/lifecycle.md)

`stop "broker.<cluster>"` → `delete <uuid_data>,<uuid_logs>` (pexpect; промпт бывает с
`mod`, не только +-*/) → `start` → облако аллоцирует volumes заново, инстанс уходит на
**другой минион** → `purge "<queue>/broker" all` ("none to purge" = ок).

- ok-events: srvk4472 → **srvk7696**; vk-events: srvk4457 → **srvk7451**.
- Диски брокеров вайпаются — RF=3 перекрывает, брокер ре-реплицируется.
- После миграции `SystemCpuLoad` валиден сразу; CC набирает 5/5 окон 100% за ~25 мин
  (5 окон × 5 мин). После — `POST /rebalance?dryrun=true` отдаёт proposal (200).

## Грабли CC этой версии (2.x MDB build)

- `/rebalance` **не поддерживает** `min_valid_partition_ratio`/`min_valid_windows`
  (обход из MDBSUP-4614 не работает): `Unrecognized endpoint parameters`.
- `goals` передавать **без скобок**: `goals=A,B,C` (с `[...]` → `Goals [[X]] are not
  supported`); без hard goals нужен `skip_hard_goal_check=true`.
- С мягкими goals + skip_check dryrun может пройти даже при 74% покрытия (если есть ≥1
  окно), но это не полный ребаланс.

## Как применять

При `NotEnoughValidWindowsException` + `Skip ... missing [BROKER_CPU_UTIL]`, если рестарт
брокера не вернул `SystemCpuLoad` (Jolokia) из -1 — не копать JVM/Porto глубже, а
**мигрировать хост** через mcc lifecycle. Битые миньоны ловятся сравнением MBean с
соседями кластра.
