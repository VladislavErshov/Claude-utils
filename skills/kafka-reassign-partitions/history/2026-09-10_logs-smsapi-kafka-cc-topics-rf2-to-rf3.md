# logs-smsapi-kafka — at-min-isr из-за CC sample-топиков RF=2/minISR=2, поднятие RF 2→3

**Дата**: 2026-09-10
**Кластер**: `logs-smsapi-kafka` (мульти-ДЦ, 3 брокера в kc/pc/ec + по контроллеру в каждом
ДЦ, CC в ec), Kafka 3.8.0, KRaft. Прод.
**Тикета нет** — разбор по запросу владельца кластера.

## Симптом

В Grafana постоянный ненулевой at-min-isr (≥64), всплески under-min-isr при любом
рестарте любого брокера.

## Диагностика

- Кластер собран по ДЦ через `mcc -c <dc> instances "%logs-smsapi-kafka%"` — брокеров
  три: kc/pc/ec (пользователь дал только ec-хост).
- **ID брокеров не по таблице префиксов из скилла kafka-cluster-inspector**: реальные
  `node.id` из `/opt/kafka/config/broker.properties`: kc=21001, pc=22001, ec=23001
  (по таблице ожидалось pc=23001 — неверно; таблица только пример, ID привязаны к
  конфигу облака).
- Rack-awareness в порядке: `broker.rack` = ДЦ у всех, каждая RF=3 партиция имеет по
  реплике в каждом ДЦ; баланс реплик 980/1002/987. Косметика: перекос лидеров на
  22001 (459 против 276/276) — preferred leader election не гонялся.
- **Корень**: два CC sample-store топика созданы самим Cruise Control с дефолтом
  `sample.store.topic.replication.factor=2` при minISR=2:
  - `__KafkaCruiseControlPartitionMetricSamples` — 32 партиции, RF=2, minISR=2
  - `__KafkaCruiseControlModelTrainingSamples` — 32 партиции, RF=2, minISR=2
  При RF=2/minISR=2 партиция в норме всегда ISR=2=minISR → перманентный at-min-isr;
  при рестарте брокера → under-min-isr (запись CC-сэмплов падает NotEnoughReplicas).
  Остальные системные в порядке: `__consumer_offsets` и `__CruiseControlMetrics`
  RF=3/minISR=2.

## Что сделано

1. Свежий `kafka-topics --describe` обоих топиков → парсинг Replicas.
2. Генератор (python локально): в каждую партицию добавлялся недостающий третий брокер
   `replicas + [missing]` — preferred leader первым сохранён, после реассигна на каждом
   брокере ровно 64 реплики. Распределение «кто отсутствовал»: 21001→31, 23001→24,
   22001→9. Расхождение порядка ISR vs Replicas (ISR с лидером первым) — не отставание,
   сравнивать множества, не списки.
3. Залика json на 1.broker...ec — **`mcc scp` молча не залил** (файла нет на хосте) →
   base64-чанки по 800 символов через expect (26 чанков, 15.4KB), md5 сверен.
4. `--execute --throttle 104857600` — успешно по всем 64 (в этом кластере с throttle
   не упал, в отличие от MDBSUP-5067).
5. Ход: 23/64 через ~1.5 мин, 64 completed / 0 in progress через ~3 мин.
6. **Throttle снялся автоматически**: финальный `--verify` (64 completed) убрал
   throttled.replicas-конфиги; последующий `--delete-config` вернул
   `Invalid config(s): leader.replication.throttled.replicas,...` — это норма
   (удалять уже нечего), не ошибка.
7. Финал: оба топика `ReplicationFactor: 3`, ISR 3/3 на всех 64 партициях,
   `UnderMinIsrPartitionCount=0`.

## Грабли сессии

- `localhost:9092` на брокере — TLS-ошибка `No subject alternative DNS name matching
  localhost found`: bootstrap только по FQDN (`hostname -f`).
- CLI-тулки пишут AdminClientConfig-dump в stdout — фильтровать `grep -E 'Topic:'` /
  `2>/dev/null` недостаточно для INFO-строк.
- Длинные команды в `mcc ssh` через expect искажаются (вставки, перенос, съедание
  `$VAR` самим expect'ом — экранировать `\$`), `mcc sshexec` на этом хосте периодически
  падал `OCI runtime error`. Надёжный путь для длинного — скрипт залить base64-чанками
  и запустить `bash /tmp/script.sh`.
