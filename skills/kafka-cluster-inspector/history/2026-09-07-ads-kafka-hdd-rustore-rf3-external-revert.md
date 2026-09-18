# 2026-09-07 ads-kafka-hdd-rustore-kafka.ec: RF→3 + minISR→2, rustore_backend откатывается внешней силой

## Задача

Поднять RF всех топиков до 3 и min.insync.replicas до 2 на прод-кластере
`ads-kafka-hdd-rustore-kafka.ec.one-infra.ru` (MDB Kafka 3.8, KRaft, 5 ДЦ:
ec×4 + pc×4 + kc×4 + hc×2 + rc×2 = 16 брокеров, CC на `1.cruise...rc`).

## Маппинг node.id → ДЦ (реальный, таблица из скилла НЕ подошла)

| ДЦ | node.id |
|----|---------|
| ec | 24001-24004 |
| pc | 22001-22004 |
| kc | 21001-21004 |
| hc | 20001-20002 |
| rc | 23001-23002 |

⚠️ Скилловая таблица «dc→20000, ic→21000...» тут не работает — префиксы назначены иначе.
Реальный путь: `grep node.id /opt/kafka/config/broker.properties` на каждом хосте.
`server.properties` — стоковый шаблон-заглушка (broker.id=0, zookeeper), реальный конфиг — `broker.properties`.

## Что сделано

1. `kafka-reassign-partitions --execute` на 110 партиций 5 топиков с RF=2
   (rustore_backend 25, chekhov-sdk-events 20, rustore_payments 1, 2×CC-внутренних по 32).
   3-я реплика — баланс по брокерам и ДЦ, без ручного троттлинга.
2. `kafka-configs --alter --add-config min.insync.replicas=2` на 12 бизнес-топиков.
3. Рестарт kafka-broker.service на 1.broker.hc (20001) для сброса fetch-сессий.

## Итог

- 14/15 топиков: RF=3, full ISR, minISR=2 — ✅.
- `rustore_backend`: minISR=2 стоит (после re-apply в 23:13 держится), **RF откатывается к 2**.

## Инцидент: внешний откат reassignment (не решён)

- **20:45:48** — контроллер (14001, 1.controller.ec) разом применил PartitionChangeRecord
  по ВСЕМ партициям rustore_backend с **точными исходными RF=2 assignments**
  (реплики+лидеры как до моих правок). Повторный откат — между 21:20 и 23:13.
- Исключены: KRaft auto-revert (применяется только к партициям с деградированным ISR —
  p0-p24 с полным ISR не должен был откатываться), Cruise Control (user_tasks — только
  GET /state с localhost), Temporal (по кластеру 53ea5f26 после 19:27 ничего), рестарты
  брокеров в 20:45 (нет регистраций).
- Не исключены: mdb-data topic-syncer (желаемое состояние в его БД — проверить в
  mdb-data UI/API), ручные действия администратора через kafkactl/super-юзер.
- Авторизационных логов на брокерах нет (INFO), клиента в 20:45:48 не идентифицировать.

## Побочные находки

- **KRaft auto-revert reassignment**: если на момент reassignment ISR партиции не содержит
  «оригинальную» реплику — контроллер молча откатывает партицию к исходному assignment.
  rustore_backend p14/17/18 шли с ISR=1 (hc 20001/20002 давно не догоняли) — первый прогон
  для них был обречён. Порядок правильный: **сначала починить ISR (догон/рестарт отставшего
  брокера), потом reassign**.
- Заливка reassign.json: `mcc scp` молча не сработал, python3 на хосте нет → base64-чанки
  через `mcc ssh + expect` (проверенный путь из скилла scp).
- `kafka-broker-api-versions` показал все 16 зарегистрированных брокеров живыми; SAN
  сертификата не содержит `localhost` — bootstrap только по FQDN.
- rc_1 (23001): на миньоне «no space left on device» для exec-контейнеров mcc (сам брокер
  жив, в ISR был). Утром 19:14 прошла resizeInstanceKafkaBroker rc_1 — возможно, связано.
- ConsumerLag fetcher'а: `kafka.server:type=FetcherLagMetrics,clientId=ReplicaFetcherThread-*-<leaderId>,name=ConsumerLag,partition=...,topic=...`
  через Jolokia :7777 — так мерил догон после рестарта hc1 (1.17M/2.3M → 0 за ~15 мин).

## Следующий шаг (когда продолжим)

Проверить в mdb-data (UI/API/БД) желаемое состояние топика rustore_backend: если там
RF=2 — поднимать через штатную операцию изменения RF, иначе ручной reassign будет
откатываться снова. Tunnel backstage_plugin_mdb (:53480) топиков mdb-data не содержит.

---

# Дополнение 2026-09-08: фикс скорости догонки через PMS + падение 22001 (миграция)

## Что выяснено и сделано

### Узкое место догонки реплик — replica.fetch.max.bytes (дефолт 1MB)
- Симптомы: сеть/потоки/диски не упёрты (лидер читал свой hdd всего 217MB/s, фолловер
  5MB/s), но лаг не сокращался. Реально: фетчер репликации ходит ~1 раз/сек и забирает
  `replica.fetch.max.bytes`=1MB → ~1MB/s на поток.
- Метрики для диагноза (Jolokia :7777):
  `kafka.server:type=FetcherStats,clientId=ReplicaFetcherThread-<N>-<leader>,name=BytesPerSec`
  (read по wildcard `*`, т.к. прямые имена содержат brokerHost/brokerPort и не читаются
  напрямую) + RequestsPerSec. FetcherLagMetrics — только по партициям.
- Фикс: в PMS `ads-kafka-hdd-rustore-kafka.clouds` → `kafka.broker.properties` добавлена
  строка `replica.fetch.max.bytes=16777216` (после replica.fetch.wait.max.ms=500), запись
  через update.do, верификация байт-в-байт. На хостах: `confp --oneshot` + рестарт
  kafka-broker. Применено пока только на 21002/21004 (2.broker.kc, 4.broker.kc).
  Эффект: p14/p17 догнали и вошли в ISR за минуты (порция выросла ~10MB/запрос).

### Артефакт метрик после входа в ISR (не считать багом кластера)
- После входа реплики в ISR её `FetcherLagMetrics.ConsumerLag` замирает/отдаёт None,
  URP=0, а дашборды могут рисовать «растущий лаг» — это мёртвые MBean-ы. Истину:
  `--under-replicated-partitions` (был пуст).

### at-min-isr=25 на кластере — почему
- 23 партиции rustore_backend откатаны к RF=2 (откат 20:45), minISR=2 остался →
  ISR=2=RF=2=minISR → «at min isr». URP при этом пуст. Лечится только RF=3.

### Падение 22001 (1.broker.pc) 2026-09-08 ~11:49 МСК
- Хост **на миграции** (mcc migrate, ~8 часов по словам владельца) — загадка «кто
  остановил» решена: облачная миграция, не атака.
- Последствия: p14/p17 ISR=1/3, p18 ISR=1/2 (RF=2) → продьюс на 3 партициях
  rustore_backend заблокирован (NotEnoughReplicas) до возврата хоста. Лидерство ушло
  на 20001 (hc). `--unavailable-partitions` пуст — проверять ISR руками.
- Reassign RF→3 для 23 партиций НЕ применён: первый execute упал «existing partition
  assignment» (висела запись p14/p17), повтор с `--additional` упал «Unknown broker
  id 22001». JSON `/tmp/reassign.json` на 1.broker.ec жив.

## Чеклист продолжения (после миграции 22001)
1. Убедиться, что 22001 вернулся (api-versions: 16 id) и ISR p14/p17/p18 ≥2.
2. `kafka-reassign-partitions --execute --additional --reassignment-json-file /tmp/reassign.json`
   на 1.broker.ec → RF=3 на 23 партициях.
3. Раскатать 16MB-конфиг на остальные 14 брокеров (PMS уже содержит; нужен confp+рестарт,
   по одному) — иначе догон снова ~1MB/s.
4. Дождаться ISR=3 везде, проверить at-min-isr=0 и minISR=2 на rustore_backend.
5. Разобраться с первоисточником отклка 20:45 (подозрение: чей-то скрипт/UI-действие;
   Temporal/CC/ops исключены).
