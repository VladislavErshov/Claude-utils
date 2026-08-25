# kafka-stats-conv-adtech-kafka: вечный "at min ISR" на CC-топиках при живых брокерах

**Дата**: 2026-08-24
**Кластер**: `kafka-stats-conv-adtech-kafka` (брокеры ic=21001/21002, uc=22001/22002, hc=24001/24002; controllers kc/pc/ec)
**Хост входа**: `1.broker.kafka-stats-conv-adtech-kafka.ec.one-infra.ru` (node.id=24001)

## Симптом

В Grafana у части партиций "at min ISR", при этом все брокеры живы
(BrokerState=3, зарегистрированы, UnderReplicatedPartitions=0).

## Диагностика

1. `mcc instances` **не находит кластер** в mdb-data — но хост жив и sshexec работает.
   Отсутствие в mdb-data ≠ мёртвый кластер (не спешить с выводами по MDBSUP-4166).
2. Jolokia на брокере: `UnderReplicatedPartitions=0`, `UnderMinIsrPartitionCount=0`,
   `BrokerState=3` — локально брокер чист, проблема "кластерная".
3. `kafka-topics --describe --under-replicated-partitions` — **пусто**.
   `--at-min-isr-partitions` — **только** внутренние CC-топики:
   `__KafkaCruiseControlPartitionMetricSamples` (32 парт.) и
   `__KafkaCruiseControlModelTrainingSamples` (32 парт.), у всех **полный ISR**.

## Корень

CC-топики созданы с **RF=2**, а `min.insync.replicas` наследует broker-static = 2.
ISR == min.insync.replicas == RF **постоянно** → метрика "at min ISR" горит вечно,
хотя данных потерь нет. Конфигурационный косметический дефект, не деградация.

## Фикс (по решению пользователя: RF→3, min.isr=2)

1. `--generate` с `--broker-list` (без broker-list падает `Missing required argument`)
   — вернул текущий RF=2; python-скриптом добавили третью реплику из недостающего ДЦ
   (ic/uc/hc round-robin), сохранив порядок и preferred leader.
2. `--execute` **без throttle** → throttle-конфиг не ставится, чистить нечего.
3. `--verify` — 64/64 `Reassignment of partition ... is completed` (reassign ушёл за ~30с).
4. `kafka-configs --alter --add-config min.insync.replicas=2` на оба CC-топика.
5. Проверка: RF=3, ISR 3/3, `--at-min-isr-partitions` → **0 партиций**.

## Грабли

- `client.properties` лежит в `/opt/kafka/config/`, НЕ в `/etc/kafka/` (там только
  get-user-info.sh). `--command-config /opt/kafka/config/client.properties`.
- Клиентские скрипты Kafka на этом хосте пишут AdminClientConfig-лог в stdout —
  фильтровать `grep -E "^\s*Topic:"` / `grep -c`, а не полагаться на чистый вывод.
- `mcc sshexec` иногда роняет вывод с `Error: non zero exit code: 1: OCI runtime error`
  после полезных строк — перечитать файл-результат на хосте (`tail /tmp/...`),
  данные при этом на месте.
- Смена RF — только через reassign (полный новый список replicas), см.
  `kafka-reassign-partitions/SKILL.md`; `kafka-configs replication.factor` не работает.
