# maildwh1-mail-dwh-kafka — хронический URP/follower-лаг: помогло num.replica.fetchers=6

Дата: 2026-09-04. Кластер `maildwh1-mail-dwh-kafka` (prod, mail DWH): по 1 брокеру в
kc/pc/rc, CC — `1.cruise.maildwh1-mail-dwh-kafka.kc.one-infra.ru`.
IDs по стандартной таблице: kc=20001, pc=21001, rc=22001.

## Симптом

- В UI mdb-data Cruise Control `UNAVAILABLE`, «Proposal not ready».
- Брокер rc в UI — все метрики N/A (после его рестартов).
- URP-волны: на pc до 107 партиций после рестартов (207 партиций в кластере, RF=3).
- rc как follower отставал на миллионы сообщений: `kafka_server_replicafetchermanager_maxlag`
  до ~1.9 млн; per-partition лаг по `mailru_splash` (p0/p3) рос, по
  `mailru-mailapi-services-log` сливался.

## Диагностика

| Что | Значение | Вердикт |
|---|---|---|
| Jolokia `BrokerState` на всех брокерах | 3 (Running) | брокеры живы, «N/A» в UI — от рестартов |
| `UnderMinIsrPartitionCount` | 0 везде | риска потери данных/доступности нет |
| Рестарты брокеров (journalctl) | kc 13:34, pc 14:14, rc 14:21, вторая волна ~14:30 | URP-спайки — следствие поочерёдных рестартов |
| Controller-кворум (Jolokia на контроллерах) | ActiveControllerCount=1 (pc), Fenced=0, Offline=0 | кворум здоров |
| Лидерство `LeaderCount` | 69/69/69 | перекоса лидеров нет |
| Продьюс `mailru_splash` (`bytesinpersec`) | kc 781 / pc 746 / rc 485 МБ/с | ~2 ГБ/с клиентского трафика на один топик |
| `replicationbytesinpersec` | 11–15 ГБ/с на брокер в пик лаг-сторма | репликация душит network-треды |
| `NetworkProcessorAvgIdlePercent` | kc 0.19, pc 0.23, rc 0.36 | network-процессоры под завязку |
| Per-partition `FetcherLagMetrics` (только JMX 8080) | весь лаг с лидера 21001 (pc) | узкое место — фетчеры follower'ов |

## Корень

Дефолтного `num.replica.fetchers=1` не хватало: на нагруженном DWH-кластере с меж-ДЦ
репликацией (kc/pc/rc — разные ДЦ) фетчеры follower'ов не успевали за продьюсом →
хронический follower-лаг, реплики выпадали из ISR (URP). Рестарты брокеров (в т.ч. под
CC-фикс) добавляли волны URP и холодный page cache на лидерах.

## Фикс

**`num.replica.fetchers: 1 → 6`** через PMS-конфиг + поочерёдные рестарты брокеров
(`confp --oneshot && systemctl restart kafka-broker`, строго по одному хосту с ожиданием
`Kafka Server started`). Применено на всех трёх брокерах.

Подтверждение: в JMX появились треды `replicafetcherthread_0..5_<leaderId>`, URP сошлась
(107→26→18→13→0), follower-лаг по `mailru-mailapi-services-log` слился. По словам владельца
кластера — увеличение до 6 помогло.

## Что НЕ сработало / грабли

- **`kafka-configs.sh --alter --entity-type brokers` для `replica.fetch.max.bytes`,
  `replica.fetch.response.max.bytes`, `replica.socket.receive.buffer.bytes`** →
  `InvalidRequestException: Cannot update these configs dynamically` — read-only,
  применяются только рестартом (через PMS + confp).
  NB: `num.replica.fetchers` динамически менять тоже нельзя при скачке >2× —
  `Dynamic thread count update validation failed ... value should not be greater than
  double the current value` (см. known_issues, MDBSUP-5067).
- `kafka-configs.sh --command-config /opt/kafka/config/broker.properties` падает в
  `CommonClientConfigs.metricsReporters` — в broker.properties прописан
  `metric.reporters=CruiseControlMetricsReporter`, которого нет в classpath CLI.
  Правильный конфиг для CLI — **`/opt/kafka/config/client.properties`** + `unset KAFKA_OPTS JMX_PORT`.
- `kafka-topics.sh` без `unset KAFKA_OPTS JMX_PORT` → `NumberFormatException: Cannot parse null string`.
- CLI-вывод под нагрузкой часто обрезается («Connection closed by remote host») —
  надёжный паттерн: писать в файл на хосте и читать файл отдельным вызовом.
- MaxLag/ConsumerLag **не видны через Jolokia 7777** (InstanceNotFoundException) —
  только `curl localhost:8080/metrics | grep kafka_server_replicafetchermanager_maxlag`
  и `kafka_server_fetcherlagmetrics_consumerlag` (см. `kafka-metrics-investigator`).

## Если лаг вернётся

- Кандидаты (read-only, только через PMS + рестарт): `replica.fetch.max.bytes=16MB`,
  `replica.fetch.response.max.bytes=64MB`, `replica.socket.receive.buffer.bytes=8MB`
  (WAN BDP), fetchers 6→8–12.
- CC: OOM-история в `cruise-control.err.log` при Xmx4096m — если повторится, поднимать heap;
  «Proposal not ready» после стабилизации брокеров закрывается самим прогревом (~30 мин,
  5 окон × 5 мин).
