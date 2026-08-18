# MDBSUP-4649 — Растущий follower lag на брокере (панель «Broker Max Lag»)

**Дата**: 2026-08-18
**Кластер**: `vkvideo-recommender-kafka` (KRaft, pc)
**Хост**: `2.broker.vkvideo-recommender-kafka.pc.one-infra.ru`

## Симптом

В Grafana на панели «Broker Max Lag» (PromQL `sum(kafka_server_replicafetchermanager_maxlag{mdb_kafka_cluster="$cluster"}) by (instance)`)
на брокере 2 растёт лаг за сутки+. Лаг не упирается в лимиты сети, CPU процесса и heap не
вышли за разумные пределы — стандартные подозреваемые (сеть/нагрузка) отступают.

## Что проверили (всё «ОК», но лаг растёт)

| MBean / метрика | Значение | Вердикт |
|---|---|---|
| `kafka.server:name=BrokerState,type=KafkaServer` | 3 (Running) | OK |
| `kafka.server:type=ReplicaManager,name=UnderReplicatedPartitions` | 0 | нет выпавших из ISR |
| `kafka.server:type=ReplicaManager,name=UnderMinIsrPartitionCount` | 0 | OK |
| `kafka.server:type=ReplicaManager,name=IsrShrinksPerSec` / `IsrExpandsPerSec` | 0 / 0 | ISR стабилен |
| `kafka.server:type=BrokerTopicMetrics,name=FailedFetchRequestsPerSec` | 0 | OK |
| `kafka.server:type=BrokerTopicMetrics,name=FailedProduceRequestsPerSec` | 0 | OK |
| `kafka.network:type=SocketServer,name=NetworkProcessorAvgIdlePercent` | 0.69 | network threads 69% idle — не исчерпаны |
| `kafka.server:type=KafkaRequestHandlerPool,name=RequestHandlerAvgIdlePercent` | 0.90 | io threads 90% idle — не исчерпаны |
| `kafka.cluster:type=Partition,name=LastStableOffsetLag` (по 70 партиям) | 0 | LSO-лага нет |
| ERROR/WARN в `kafka-broker.out.log` (без `mdb-tos`) | 0 | OK |

Т.е. **классические признаки репликационной деградации отсутствуют** — но лаг в Grafana
продолжает расти.

## Корень проблемы

Метрика `kafka_server_replicafetchermanager_maxlag` на брокере = **675 219 сообщений**.
Per-partition разбивка (`kafka_server_fetcherlagmetrics_consumerlag_clientid_replicafetcherthread_*`)
показала 38 партиций с ненулевым лагом, топ:

```
recommender-vkvideo-neuralrank-features-log partition=32  → 675 224
recommender-vkvideo-neuralrank-features-log partition=33  → 641 696
...
```

`ReplicaFetcherThread_0_23001` … `_0_23008` — 8 fetcher-тредов. Но фактическое значение
`num.replica.fetchers` в `kafka.broker.properties` = **1**. Расхождение объясняется тем,
что треды нумеруются по источнику-лидеру (`_0_<brokerId>`), а число одновременных fetcher'ов
на одного лидера = `num.replica.fetchers` = 1.

При `num.replica.fetchers=1` один fetcher-тред на пару (этот брокер → лидер X) обслуживает
все партиции, чьим лидером является X, последовательно в одном запросе. При высокой
нагрузке на топик `recommender-vkvideo-neuralrank-features-log` fetcher не успевает
обойти все партиции за `replica.fetch.wait.max.ms` — lag копится. Брокер ещё в ISR
(`UnderReplicatedPartitions=0`), но на грани: ещё немного и `replica.lag.time.max.ms`
(30s по умолчанию) будет превышен → выпадение из ISR → рост under-replicated.

## Фикс

В pms `kafka.broker.properties` поднять `num.replica.fetchers` с 1 до 4:

```properties
num.replica.fetchers=4
```

Применить через `confp --oneshot` + поочерёдный рестарт брокеров:
```bash
confp --oneshot
systemctl restart kafka-broker
```

После рестарта — `kafka_server_replicafetchermanager_maxlag` начал падать, лаг пришёл в
норму в течение ~часа (догонял накопленное).

## Грабли

1. **MBean `ReplicaFetcherManager,name=MaxLag` не виден через Jolokia (7777)** —
   `InstanceNotFoundException`. Зато экспонируется через JMX exporter (8080):
   ```bash
   curl -s --max-time 15 http://localhost:8080/metrics | grep '^kafka_server_replicafetchermanager_maxlag'
   ```
   Per-partition лаг:
   ```bash
   curl -s --max-time 15 http://localhost:8080/metrics \
     | grep '^kafka_server_fetcherlagmetrics_consumerlag' | grep -v ' 0.0$' | sort -k2 -n -r | head -20
   ```
   Если сразу смотреть через Jolokia — MBean «не существует», ложный вывод что лага нет.
   Подробности — `kafka-metrics-investigator/commands/check_metrics.md` → «Follower lag».

2. **Панель «Broker Max Lag» ≠ consumer group lag.** Это разные метрики с разных
   exporter'ов: follower lag идёт с JMX (8080), consumer group lag — с kafka-exporter (23569).
   Перепутать легко — сначала уточнить, какой именно лаг растёт. Таблица соответствия —
   `kafka-metrics-investigator/commands/check_metrics.md` → «Дедупликация лагов в Grafana».

3. **`NetworkProcessorAvgIdlePercent` / `RequestHandlerAvgIdlePercent` могут быть высокими
   (много idle), но лаг всё равно растёт** — thread pool'ы сетевых/io тредов не исчерпаны,
   а `num.replica.fetchers` узкое место. То есть проверка тред-пулов из `troubleshooting.md`
   «Метрики по io/network тредам» необходима, но не достаточна — отдельно проверять именно
   fetcher-лаг через 8080.

4. **Треды `ReplicaFetcherThread_0_<brokerId>` не равны `num.replica.fetchers`.** Нумерация
   тредов: `_0_<sourceBrokerId>`, число тредов на одного источника = `num.replica.fetchers`.
   8 тредов в выводе с разными brokerId ≠ «8 fetcher-тредов всего».

5. **Сопутствующий шум в `kafka-broker.err.log` от `mdb-tos-evaluator`** (не причина лага):
   ```
   [mdb-tos] ... WARN [mdb-tos-evaluator] catchup scan failed -> empty (prod) : java.util.ConcurrentModificationException
   ```
   TOS agent нездоров, но CPU не вырос — смотри память `tos_agent_high_cpu.md`, это отдельная
   история.
