# MDBSUP-3603 — reactions-prod-vk-reactions-kafka: лаг консумеров, перекос лидерства, ребаланс через CC

Дата: 2026-09-11. Kafka, ns vk-reactions, кластер `reactions-prod-vk-reactions-kafka`
(`cba5131e-a3e2-4bb5-bd4f-8d20563d317f`), 3 ДЦ (ic, nc, zc) × 2 брокера + 3 контроллера,
cruise в nc. Автор тикета — Ким Никита, тип «Аномалии в метриках/графиках». 201 партиция,
RF=3, 9 топиков.

## Суть тикета

Лаг консумер-групп растёт при изменении числа тредов: `io=8/network=3` → лаг до 1к,
`24/48` (17.06) → хуже 1к. Наше `24/8` (18.06) не помогло (22.06 откат). Вывод 23.07:
корень — нагрузка от консумеров; рекомендации `fetch.min.bytes`/`fetch.max.wait.ms` ↑,
либо масштабирование (+1 брокер в ДЦ). 10.09 клиент: Network Processor Usage
стабилизировал, Request Handler — нет; планирует CPU + поднятие тредов.

## Диагностика 11.09 (срезы до ребаланса)

Панели дашборда → метрики (JMX exporter 8080):

| Панель | PromQL | MBean |
|---|---|---|
| Average Network Processor Usage, % (id 63) | `1 - kafka_network_socketserver_networkprocessoravgidlepercent` | `kafka.network:type=SocketServer,name=NetworkProcessorAvgIdlePercent` |
| Average Request Handler Usage, % (id 64) | `1 - kafka_server_kafkarequesthandlerpool_requesthandleravgidlepercent_total` | `kafka.server:type=KafkaRequestHandlerPool,name=RequestHandlerAvgIdlePercent` |

Живые значения — Jolokia 7777 (`/jolokia/read/<mbean>`, у handler-метрики брать
`OneMinuteRate`/`FifteenMinuteRate`, это idle). До ребаланса: network 15–51%,
handler 13–66% (🟠 только 2.zc), RequestQueueSize 0–2. Конфиг на всех брокерах
`num.network.threads=6, num.io.threads=16` (в broker.properties ключи задублированы,
значения равны) при 56–64 ядрах — эксперимент 24/48 откачен, запас по потокам большой.

**CPU хостов** — через `vmstat 1 2` по sshexec (⚠️ Jolokia `java.lang:type=OperatingSystem
ProcessCpuLoad` на этих хостах врёт ~0.0 — не использовать): ic ~20% util, nc/zc 40–46%.

**Трафик** (`kafka.server:type=BrokerTopicMetrics` MessagesInPerSec): ic 550–840 msg/s,
nc/zc 2640–3380 msg/s.

**Распределение** (kafka-topics --describe + awk, broker id: ic=2000x, nc=2100x, zc=2200x):

| Брокер | Лидеров | Реплик | CPU util | msg/s in |
|---|---|---|---|---|
| 20001 (1.ic) | 12 | 110 | ~20% | 844 |
| 20002 (2.ic) | 18 | 42 | ~19% | 547 |
| 21001 (1.nc) | 39 | 118 | ~40% | 3381 |
| 21002 (2.nc) | 58 | 114 | ~44% | 3371 |
| 22001 (1.zc) | 37 | 125 | ~43% | 3360 |
| 22002 (2.zc) | 37 | 94 | ~46% | 2644 |

**Причина перекоса**: клиентский трафик идёт на лидера (produce/fetch + TLS + чтение),
а лидерство было перекошено — ic держал 25% реплик, но только 15% лидерств (12+18=30 из
201); 21002 перелидерован (58 против 39). Placement размазан 42–125 реплик/брокер.
Похоже, реплики когда-то переставляли без последующего preferred leader election, CC
выравниванием не занимался (self-healing весь выключен: `self.healing.enabled=false`).

## Cruise Control: proposal и запуск

- REST CC: `webserver.http.port=8080` на cruise-хосте (НЕ 9090), префикс —
  **`/kafkacruisecontrol/`** (см. грабли ниже), обязателен header `User-Agent`.
- `/state`: proposal ready, все 8 целей ready, **RackAwareDistributionGoal violated**
  (детект каждые ~15 мин, status IGNORED), balancedness 67.6, RIGHT_SIZED.
- `/proposals`: 57 меж-брокерных перемещений (59807 МБ) + 22 смены лидерства,
  RackAwareDistributionGoal FIXED, остальные capacity-цели NO-ACTION. После: лидеры
  30–37, диски 16–19%.
- Запуск: `POST /kafkacruisecontrol/rebalance?dryrun=false&json=true` (одобрено
  пользователем). User-Task `492c5407-b08f-49aa-aaaf-148fd2da7cca`. Волнами по 10
  движений/брокер, вся задача — ~35 мин (реплики + затем лидерство),
  finished 53788/59807 МБ к моменту окончания.

## Результат после ребаланса

- Лидеры: **33/30/30/37/36/35** (было 12–58) — точно как предсказывал CC.
- Реплики: 135/66/87/114/105/96 (выровнялись частично — rack-aware на 9 топиках жёстко
  ограничивает; 2.ic остался легче всех).
- CPU util: 2.nc 44→36%, 2.zc 46→39%, 1.zc 43→40%, 2.ic 19→29% (взяла свою долю).
  Разброс сузился 19–46% → 19–40% (часть — репликационный хвост, осядет за ~час).
- RackAware-нарушение закрыто по факту (ground truth: 0 партиций с дублем ДЦ, реплики
  по ДЦ 201/201/201, balancedness 100.0), но детектор CC продолжает переоткрывать
  anomaly RackAwareDistributionGoal каждые 5 мин (status IGNORED, self-healing off) —
  детектор не закрыл старую аномалию; на кластер не влияет, косметика.
- Финальный срез CPU (~40 мин после ребаланса): ic 23/23%, nc 39/39%, zc 34/40% —
  перекос «1 vs 2 брокер» внутри каждой ДЦ исчез, разброс 23–40%.
- CC так же поймал METRIC-аномалии `BROKER_CONSUMER_FETCH_LOCAL_TIME_MS_999TH` на
  22001/20001 — согласуется с лаг-симптоматикой тикета.

## Грабли дня (важно!)

1. **Префикс CC API — `/kafkacruisecontrol/`, не `/kafacruisecontrol/`** («kafa» без k).
   Опечатка даёт 404 от Jetty default servlet — выглядит как «CC сломан», хотя CC жив.
   Диагностика: accesslog в `/mnt/logs/dbms/cruise-control.out.log` (grep
   `PublicAccessLogger`) — там реальные URI и статусы. Прежде чем рестартовать CC —
   сверить URI.
2. **Рестарт CC не понадобился** — а он был сделан по ходу диагностики (безвреден, но
   прогрев ~15 мин). Сначала accesslog, потом драма.
3. Логи CC: systemd unit `cruise-control`,
   `StandardOutput=append:/mnt/logs/dbms/cruise-control.out.log`; `/opt/cruise-control/logs`
   НЕ существует (log4j ./logs не создаётся) — там логов нет, не искать.
4. `kafka-topics.sh` / `kafka-broker-api-versions.sh` не в PATH неинтерактивного шелла —
   полный путь `/opt/kafka/bin/...`. SASL_SSL: обязательно `--command-config
   /opt/kafka/config/client.properties`, bootstrap — FQDN (не localhost).
5. Describe/подсчёт лидеров: foreground `timeout 50 /opt/kafka/bin/kafka-topics.sh ...
   --describe | awk -F'\t' ...` влезает в sshexec (кластер 201 партиций — секунды).
   setsid/nohup/base64-обёртки не понадобились; stderr прятать не надо — им сразу видно
   «kafka-topics.sh: not found».
6. Broker id кластера: ic=2000x, nc=2100x, zc=2200x (уточнять через
   `kafka-broker-api-versions.sh | grep 'id:'`).

## Причина упора в CPU: пресет 16 vCPU + слепой к CPU CC

- Пресет брокеров **c.large = 16 vCPU** (cpu_optimized), при этом гость видит 56–64 ядра
  миньона → облачный CPU 100% на 2.zc/2.ic = упор в лимит пресета, гостевой vmstat при
  этом «спокойный» (23–40% от видимых ядер). Не путать два взгляда.
- CC слеп к CPU: `getProcessCpuLoad()` в JDK внутри Porto-контейнера отдаёт ~0
  (системный баг MDBDEV-2029), capacity.json `"CPU": "100"` → CC видит 0–1.4% загрузки,
  CPU-цели никогда не срабатывают, статус RIGHT_SIZED при реальном насыщении.
- Применён конфиг-фикс capacity (вариант A из истории MDBDEV-2029): PMS
  `kafka.cruisecontrol.capacity.json` CPU `"100"` → `"12.5"` (16×100/Z=128) + confp +
  рестарт CC. Верификация: NumCore=0.125, CpuPct 9.0/16.4% (не нули). Детали и оговорки
  (PCL нестабилен, ic ±12.5%, modify затрёт) —
  `kafka-config-inspector/history/2026-08-21_MDBDEV-2029-cruise-cpu-metrics.md`.
- **Эксперимент откачен в тот же день**: срез `/load` показал инвертированную картину —
  CC «видел» 8.5–16.5% на лёгких ic и 0.15–0.27% на реально упёртых nc/zc (PCL там
  константный 0). Конфиг-фикс даёт шкалу, но не чинит метрику → откат (PMS `"100"` +
  confp + рестарт, NumCore=1.0, isProposalReady ok). Целевое решение по CC — вариант B:
  патч cruise-control-metrics-reporter (CPU из ΔgetProcessCpuTime).
- Следующий шаг по тикету: ресайз пресета брокеров (16→32 vCPU) — операция кластера
  в mdb-data, по согласованию с клиентом. До ресайза масштабирование мерить только
  по облачным метрикам (одна_vm one_cloud_cpu_percent), не по CC.

## Хвост / дальше

- Коммент в тикет с результатами (до/после) — черновик согласован, НЕ отправлен.
- Клиенту: мониторить лаг после ребаланса; `fetch.min.bytes`/`fetch.max.wait.ms` на
  консумерах остаются в силе.
- Решение с клиентом: ресайз пресета брокеров 16→32 vCPU (это и есть его план
  «докинуть CPU»); треды network/io можно не трогать (узкое место — квота VM).
- Несимметричный placement (66–135 реплик) — приемлем, при желании отдельный прогон
  `TopicReplicaDistributionGoal`.
- По MDBDEV-2029: тикет остаётся в Need Info; следующий шаг — вариант B (свой
  metrics-reporter jar, механизм сборки есть).
