# 2026-09-08: Client Connections пустой для брокеров на Kafka 4.3 (vkontakte-mdb8273)

## Симптом

Кластер `vkontakte-mdb8273-kafka` (Kafka **4.3.0**, 21 брокер × pc/rc/uc + контроллеры).
В Grafana-панели **Client Connections** (`kafka_server_socketservermetrics_connections`)
видны только серии контроллеров (`listener="CONTROLLER"`, `apache-kafka-java 4.3.0` —
это коннекты самих брокеров). У брокеров панели Connections пустые, при этом хосты
AVAILABLE и обслуживают трафик (на 2.broker.pc — 420 ESTABLISHED на :9092).

## Причина

На 4.3-брокерах MBean'ы `kafka.server:type=socket-server-metrics,...` **не создаются
вообще** — регрессия/изменение брокерного кода 4.3 (Java-rewrite SocketServer), не проблема
кластера и не конфиг exporter'а:

- `/metrics` (574KB, полный дамп): ноль `kafka_server_socketservermetrics*`;
- Jolokia `search kafka.server:type=socket-server-metrics,*` → пусто;
- полный скан JVM (1902 MBean, подстрока «onnection») → только
  `kafka.network:name=ExpiredConnectionsKilledCount,type=SocketServer`;
- при этом в `/opt/prometheus/kafka-broker.yml` правила для
  `kafka_server_socketservermetrics_connections` **есть** — метрика вернётся сама,
  когда MBean'ы починят в образе 4.3.

Сетевые метрики 4.3-брокера переехали в домен `kafka.network`:
`SocketServer` (NetworkProcessorAvgIdlePercent, MemoryPoolAvailable/Used,
ExpiredConnectionsKilledCount), `Processor` (IdlePercent), `Acceptor`
(AcceptorBlockedPercent) — connection-count и per-client-software разбивки среди них нет.

Контроллер того же jar 4.3.0 (kafka.Kafka + controller.properties) старые MBean'ы
регистрирует → поэтому в панели «только контроллеры».

## Сверка версий (эмпирика)

| Версия | Хост | `kafka_server_socketservermetrics_connections` |
|---|---|---|
| 3.8.0 | broker (auction-realtime-adtech-kafka.pc) | **есть** (22 серии: librdkafka 2.8.0, sarama, apache-kafka-java) |
| 4.3.0 | broker (vkontakte-mdb8273, 1.broker.pc и 2.broker.pc) | **нет** |
| 4.3.0 | controller (vkontakte-mdb8273.pc) | **есть** (listener=CONTROLLER) |

## Что сделано в дашборде (grafana-plot-creator, dashboard-test.json)

Панели **Client Connections** и **Active Connections** — union-выражение:

```promql
sum by (mdb_kafka_cluster, instance, client_software_name, client_software_version) (
  kafka_server_socketservermetrics_connections{mdb_kafka_cluster="$cluster",instance=~"$instance"})
or on (mdb_kafka_cluster, instance)
sum by (mdb_kafka_cluster, instance) (
  rate(kafka_network_requestmetrics_requestspersec{mdb_kafka_cluster="$cluster",
       instance=~"$instance",request=~"Produce|FetchConsumer"}[$__rate_interval]))
```

- 3.8 и контроллеры 4.3: рисуется настоящая метрика соединений (левая часть приоритетна);
- 4.3 брокеры: подставляется клиентский RPS Produce+FetchConsumer
  (`kafka_network_requestmetrics_requestspersec` — проверено, что есть на 3.8 и 4.3,
  имена запросов `Produce`/`FetchConsumer`/`FetchFollower` совпадают в обеих версиях);
- когда починят образ 4.3, union сам переключится на настоящую метрику.
- Descriptions панелей не менялись (по решению владельца дашборда).

## Дальше (image-side)

Реальные per-client соединения у 4.3-брокеров появятся только после фикса
регистрации MBean'ов в образе ubuntu20-kafka-4.3 (docker-images) — кандидат в MDBDEV.

## Грабли инструментария

- `mcc sshexec` к этому кластеру стабильно рвётся (`Connection closed by remote host`) —
  работать через `mcc ssh` + expect, короткие команды, тяжёлое — в файл на хосте.
- Tcl/expect: `[]` в python/grep-паттернах → экранировать или избегать
  (`.get()`, циклы со счётчиком, `sed` вместо `[a-z]`-классов).
- `/opt/kafka/config/server.properties` на брокере — нерендеренный шаблон
  (`process.roles=broker,controller`, `advertised.listeners=localhost`) — НЕ диагностический
  признак; реальный конфиг — `broker.properties` (ExecStart).
