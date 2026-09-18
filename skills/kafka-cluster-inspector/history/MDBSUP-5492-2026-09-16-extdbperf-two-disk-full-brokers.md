# MDBSUP-5492 — extdbperf: ДВА мёртвых брокера с диском 100% во время CC-ребаланса

Дата: 2026-09-16/17
Тикет: https://jira.vk.team/browse/MDBSUP-5492
Кластер: `extdbperf-oneme-kafka` (oneme, id `6a9e8ad1-10c8-41f2-8012-5d559dfdc1d4`), KRaft 3.8,
75 брокеров в 3 ДЦ: KC=21xxx (1.broker.kc), PC=22xxx (1.broker.pc), EC=23xxx (1.broker.ec), CC в PC.
Похоже на: MDBSUP-5279 (переполнение диска брокера), но без strays — данные легитимные.

## ⚠️ Важные поправки к скиллу (id ↔ ДЦ)

В этом кластере ID-базы ДЦ **не как в таблице SKILL.md** (там uc→22xxx):
- **KC → 21xxx** (1.broker.kc = 21001), **PC → 22xxx** (1.broker.pc = 22001), **EC → 23xxx** (1.broker.ec = 23001).
- В host_state этого кластера ДЦ = ec/kc/pc (никакого uc).
- Проверять ID обязательно: `kafka-broker-api-versions.sh --bootstrap-server <host>:9092` показывает
  `host:9092 (id: NNNNN rack: <dc>)` — сразу и ID, и rack. CC REST — порт **8080**, не 9090.

## Симптом

Тикет: 1.cruise.pc «недоступен», 1.broker.kc — переполнен диск, 4.broker.kc — UNKNOWN.
По факту: **два мёртвых брокера** — 21001 (KC-1, упал 18:19) и 22001 (PC-1, упал 19:46,
в тикете НЕ упомянут) — оба «Shutdown broker because all log dirs failed» при диске 100%.
4.broker.kc (21004) — **ложная тревога**: жив, пишет/читает (перезапущен ~19:00, ложно UNKNOWN в UI).
CC (1.cruise.pc) — жив, `isProposalReady=true`; в 17:59 МСК шёл execution ребаланса — на нём диски и доехали.

## Диагностика

- `df -h /mnt/data` 100% на обоих; `du -sk /mnt/data/log/*-stray` = 0 (нет мусора!).
- Топ по размеру: 5 партиций `oneme_tech_ClientPerformanceLoginEvents` по ~119GB (retention.ms=36ч,
  retention.bytes=-1) — легитимные данные, кластеру реально не хватает объёма (автор тикета ждёт +5 брокеров/ДЦ).
- ISR партиций с мёртвыми репликами сузился до 1 (при min.insync.replicas=2 → NOT_ENOUGH_REPLICAS на прод).
- `kafka-reassign-partitions --verify` на схему с мёртвым брокером → `Unknown broker id 22001` —
  быстрый способ обнаружить второй мёртвый брокер, которого нет в тикете.

## Фикс (reassign → чистка → старт)

1. Reassign 5 партиций LoginEvents, заменив мёртвые ID: 21001→21002/03/05/06/07, 22001→22002/03/04/05.
   JSON: `"replicas":[21002,22002,23017]` — **ID числами, не строками** (иначе parsePartitionReassignmentData падает).
   Без command-config клиент не ходит: 9092 = SASL_SSL/PLAIN; `/etc/kafka/kafka-console-consumer.properties`
   в образе нет — собрать `/tmp/admin.properties` из broker.properties (`^ssl.*` + `sasl.jaas.config` из
   `cruise.control.metrics.reporter.sasl.jaas.config`). `broker.properties` как command-config НЕ годится —
   CC-репортер в `metric.reporters` роняет AdminClient (`NumberFormatException` в CruiseControlMetricsReporter).
   Клиенту нужен heap: `KAFKA_HEAP_OPTS='-Xms256m -Xmx2g'` (на метаданных кластера дефолтный OOM-ится).
2. Синк 5×2 реплик ≈ 1.07TB занял ~1.2ч (реальная скорость ~40MB/s/поток, лидеры в EC).
3. После ISR=3: `rm -rf` каталогов переезжающих партиций на дисках мёртвых
   (KC-1: все 5 → 82%; PC-1: 95/98/99/101 — P104 там не жил → 86%). Брокер лежит → переезд не чистит его диск.
4. `systemctl start kafka-broker` на обоих → `Successfully registered broker` (epoch), `Kafka Server started`.
5. CC rebalance сразу в исполнение (без dry-run, по требованию дежурного):
   `POST :8080/kafkacruisecontrol/rebalance?dryrun=false` → 149 переносов, ~4TB, balancedness 42.6→86.5,
   `INTER_BROKER_REPLICA_MOVEMENT_TASK_IN_PROGRESS`, URP=1 в начале.

## Грабли / уроки

1. **ID-базы ДЦ кластера проверять по факту** (api-versions с rack), таблица в SKILL.md — ориентир.
2. Reassign с мёртвым брокером в retained-репликах падает `Unknown broker id` — это и диагностика второго мёртвого.
3. Мёртвый брокер с полным диском нельзя чистить через CC/restart — только reassign его реплик на живых,
   дождаться ISR и удалить каталоги руками.
4. `mcc instances -c <dc>` по несуществующему для кластера ДЦ (uc) молча пуст — не признак отсутствия брокеров.
5. UNKNOWN в UI ≠ мёртвый брокер: сверять с фактической активностью в kafka-broker.out.log (rolled segments).
6. `.deleted`-файлы и strays проверять, но при легитимном переполнении они не помогут — только reassign.
