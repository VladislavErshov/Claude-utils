# MDBSUP-3644 — CPU throttling на «пустом» кластере crash-ratelimit-kafka (Kafka 3.8)

Дата: 2026-09-15. Связь: MDBDEV-3145 (та же механика, 4.3; **закрыт 2026-09-15** — проблема
версия-независима, решение уходит в живые задачи / MDBSUP-3644), методика — `2026-08-21-MDBDEV-3145-dsp-notices-cpu-throttled-4.3-vs-3.8.md`.

## Кластер

`crash-ratelimit-kafka` (кластер `crash`, project ratelimit): Kafka **3.8.0** (docker 2.4.4),
preset **c.micro = 4 vcores / 8 GB**, 42 брокера (dc 12 / kc 11 / rc 12 / sc 7) + 3 controller + cruise(dc).
Heap 2G (уже 25% RAM), G1, Java 17. Диски данных — **HDD** (ROTA=1, 6×16.4T на dc).

## Замеры 2026-09-15 (дельты за ~53с, методика dsp-notices)

| Хост | Контейнер, ядер | Broker JVM | prometheus-http | Трафик |
|---|---|---|---|---|
| 1.broker.dc | **1.31** | 1.19 | **0.34** | 2.0 MB/s in + 3.3 out, replication 5.3 in, 23K msg/s, Fetch 1035/s, FF 700/s, Produce 262/s |
| 1.broker.kc | **1.18** | 1.07 | 0.20 | Fetch 1611/s, FF 1232/s, Produce 705/s |
| 1.broker.sc | **0.34** | 0.24 | **0.195 = 57% CPU брокера** | ~0: только connection churn ApiVersions ~4.7/s |

- Тред-брейкаудаун dc: prometheus-http 0.34 > data-plane ~0.52 (16 тредов) > ReplicaFetcher ~0.22.
- vector.service = **0.013 ядра** — гипотеза «виноват вектор» (Д. Дужинский) опровергнута.
- share-group-lag-exporter (python3) = 0 CPU на 3.8 (`kafka-share-groups.sh` отсутствует).
- GC: young GC каждые ~9.6с (252094 шт / 28 дней), суммарно 0.07% CPU — сам по себе дешёвый, но бёрстит.
- Топик `crash_reports_chafka` = реальный пайплайн (BytesOut cumulative 2.99TB/28д = ~1.2 MB/s) — кластер «без нагрузки» с июня оброс трафиком, но троттлинг был и до него.
- Burst-профиль (2с разрешение, dc): фон ~0.95 → каждые ~10с всплеск до ~1.8 ядра на ~4с (скрейп Prometheus 10s + young GC).

## Механизм троттлинга (главное)

1. **JVM видит все CPU хоста** (cpuset 0-27,56-83 = 56 на dc; 64 на kc/sc), флагов
   `-XX:ActiveProcessorCount` / `UseContainerSupport` в cmdline НЕТ, `cpu.cfs_quota_us=-1`
   (Porto рулит квотой выше cgroup — изнутри не видна).
2. Следствие: **ParallelGCThreads = 38 на dc (56 CPU) и 43 на kc/sc (64 CPU)** — дефолт 8+(N-8)*5/8.
3. Каждый young GC (каждые ~10с при 2G heap) поднимает 38–43 GC-треда одновременно →
   мгновенный спрос на десятки ядер при квоте 4 vcores → **Porto-троттлинг-бёрсты**,
   которые UI показывает как «CPU throttled 27–82%» при среднем потреблении 0.3–1.3 ядра.
4. Фоновый мониторинг-налог: JMX exporter (javaagent 0.19.0, /metrics 490–623KB) = **0.2 ядра
   на каждом брокере, на idle-брокере sc = 57% всего CPU** — объясняет троттлинг «совсем без нагрузки»
   в июне (налог + CC-metrics топик + replication churn были с рождения кластера).

Тот же механизм объясняет dsp-notices-msk (MDBDEV-3145): heap 2G/G1/без ActiveProcessorCount —
версия-независимая проблема конфигурации JVM, а не 4.3 сама по себе.

## Рекомендации (НЕ применялись, только после одобрения)

1. **`-XX:ActiveProcessorCount=<vcores>`** в KAFKA_OPTS образа (sysconfig/confp-шаблон ubuntu20-kafka)
   — ParallelGCThreads/JIT-треды масштабируются под квоту. Ядро фикса, для всех MDB Kafka-кластеров.
2. Обновить jmx_prometheus_javaagent 0.19.0 (0.2 ядра налога на тихом брокере).
3. Эксперимент на одном брокере: hot-reload sysconfig + рестарт, сверить UI throttled до/после.

## Хосты

dc: 1.(dc/pc/rc)... — brokers 1–12 (dc, rc), 1–11 (kc), 1–7 (sc); controllers dc/kc/rc; 1.cruise.dc.
Проверенные: 1.broker.dc (56 CPU, 38 GC threads), 1.broker.kc (64 CPU, 43), 1.broker.sc (64, 43).

## Продолжение 2026-09-15 (вечер): кластер не пустой + перекос

- Кластер `crash` имеет реальный пайплайн crash-репортов → ClickHouse: топики
  `crash_reports*`, `crash_alerts/audit/free/perf/statuses`, + 8 consumer-групп
  (`kafka-ch-crash-reports-consumer-v3`, `chafka_group_id`, `clickhouse-tracer-*`...).
  Тезис тикета «нет записи и чтения» к текущему моменту неверен (был верен в июне, в эру создания).
- Разброс CPU по брокерам (12с cpuacct-окно): **2.broker.dc = 2.4 ядра (60% квоты)**,
  5.broker.rc = 1.6, 1.broker.dc = 1.31, kc/dc прочие 0.3–0.5 — сильный лидерский перекос.
- Троттлинг-эра июня (UI CPU 66–82% = 2.6–3.3 vCPU/брокер по Porto-utilization) — реальные
  цифры эры создания (bootstrap-репликация, CC-metrics, ребалансы групп), не артефакт метрики.
- Idle-нагрузка пустого брокера разложена (30с тред-дельты, 1.broker.sc): prometheus-http
  (JMX exporter) 0.123 ядра — доминирует; mdb-tos 0.002; data-plane ~0.002×20 тредов. GC ~0.003.
- Облачные Porto-метрики: `mcc -n infra status <queue>` → demand/allocated/utilization;
  методика — `commands/cloud_metrics.md`. Utilization ≈ cpuacct в среднем по шарду.
- «Throttled» в UI — интерпретация как спрос сверх гарантии (overqueue): спарки 38–43
  GC-тредов + сетевые пулы пробивают 4 vcores при среднем 1–2.4 ядра. Точная формула
  Porto vcores_overq не подтверждена (открытый вопрос; уточнить у команды облака).

## Кейс-подтверждение 2026-09-15: v-analytics-vkvideo-kafka (4.3, 60 брокеров, трафика НЕТ)

Живое воспроизведение «пустого» кластера (UI-сетка: все 60 брокеров 12–24% CPU равномерно,
produce/fetch ≈ 0, установленных коннектов на 9092 = 0). Замер 1.broker.ic: контейнер
**0.44 ядра** на пустом брокере. Полная раскладка:
1. **JMX exporter (prometheus-http) = 0.171 ядра (39%)** — скрейп /metrics.
2. **share-group-lag-exporter (4.3-only) = ~0.1–0.15 ядра** — поймано ловушкой новых PID:
   2 дочерних JVM (`-Xmx256m`, ~3с жизни) каждые ~35–60с при нулевом числе share groups.
   Класс для pgrep: cmdline содержит `kafka-share-groups`-скрипт → exec java; ловить по
   появлению новых PID `java` (pgrep -x java diff), не по имени класса.
3. broker JVM остальное ~0.1: KRaft-фон, GC (38 тредов — heap тут 4G), connection churn
   ~7 ApiVersions/с (TLS-handshake на каждый, коннекты живут <100мс, в ss не ловятся).
4. kafka_exporter (Go) 0.029, vector 0.011.

Итого ~30+ vCPU постоянного фона на кластер из 60 брокеров без единого клиента.
Оба главных виновника совпадают с разбором MDBDEV-3145 (dsp-notices).
Методика ловли спавнов: pid-diff ловушка в `~/opencode/mdb3644/pidtrap.sh` (сессия 2026-09-15).

## Контроль 2026-09-17: без изменений, фикс не выкачен

- 1.broker.dc: 56 CPU, **38 GC-тредов**, ActiveProcessorCount=нет, контейнер ~1.0 ядра; 1.broker.sc: 64 CPU, **43 GC-треда**, APC=нет, контейнер <0.5 ядра. Очереди: dc util 16.7/54 (31%), sc 6.9/28 (25%).
- Образ всё ещё 2.4.4 → MR !145 сюда не относится (это фикс share-lag-спавнов для 4.3; на 3.8 экспортер и так ~0). Для этого кластера актуальны прежние кандидаты: `ActiveProcessorCount` + апгрейд JMX exporter.

### Профиль по процессам 2026-09-17 (дельты 30с, milli-ядер)

- **1.broker.sc (idle)**: контейнер **275**; java 161, из них prometheus-http (JMX exporter) **125 = 45% контейнера**; kafka_exporter (Go) **50**; vector 11; rscheck 9; data-plane ~1×20 тредов (connection churn); mdb-tos 3. Мониторинг-стек (125+50+11+9) = **71% CPU пустого брокера**; полезная работа JVM ≈ 36 mc.
- **1.broker.dc (нагружен)**: контейнер **1273**; java 1150 (prometheus-http **266**, ReplicaFetcher ~190, data-plane ~400 на 16 тредов); kafka_exporter 36; vector 12; rscheck 10.
- Новое: **kafka_exporter стабильно 0.036–0.05 ядра/брокер** независимо от нагрузки (раньше на этом кластере не выделяли; в v-analytics было 0.029). Вывод прежний: главный паразит — JMX exporter, второй — kafka_exporter; на 42 брокеров суммарно ~0.25 ядра мониторинг-налога только по этим двум.

## Финал 2026-09-16: фикс в docker-images (MR !145, MDBDEV-3145)

- Патч: `_build_env()` экспортера отдаёт детям флаги из `.jvm-opts` файла
  (`-XX:TieredStopAtLevel=1 -XX:+UseSerialGC -XX:CICompilerCount=1
  -XX:ActiveProcessorCount=2 -XX:+PerfDisableSharedMem -XX:SharedArchiveFile=...`),
  который генерирует build.d 35-share-groups-cds.sh вместе с CDS-архивом
  (офлайн-тренинг ShareGroupCommand на мёртвом эндпоинте, таймаут 2с — грузит
  полный AdminClient/TLS-стек). Единственный источник флагов — build.d, python читает файл.
- Замеры: спавн 4.8 → 1.9 CPU-s (−60%); расширенные флаги дают ещё −9%;
  полнота CDS-тренинга влияет сильнее самих флагов (wall 1.6с полный vs 2.5с --help).
- Хот-деплой на 19.ic/19.zc v-analytics (скрипт чанками base64 по 2400 символов —
  scp молча падает, sshexec >431 на 5К): дети с флагами пойманы ловушкой PID,
  C2 в спавн-окнах → 0%. Откат: `*.bak-3644` + restart сервиса.
- python3-строка в профилях = rscheck-python + host_checker (не только share-lag).
- ciCompilerCount=1 валиден с TieredStopAtLevel=1 на Java 17 (проверено на хосте).
