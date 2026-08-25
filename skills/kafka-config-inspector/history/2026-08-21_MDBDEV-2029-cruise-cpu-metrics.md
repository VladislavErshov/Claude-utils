# MDBDEV-2029 — Cruise Control неправильно читает CPU в Kafka-кластерах

Дата: 2026-08-21. Тестовый кластер: `test-cruise5-mdbdev-kafka` (dc/ic/pc/uc, 8 брокеров, cruise в dc).

- Jira: https://jira.vk.team/browse/MDBDEV-2029
- МР backstage !234 (branch `ershov/MDBDEV-2029-Fix-config-for-correct-read-CPU-usages-in-Cruise`)
- МР docker-images !54 (тоже branch `ershov/MDBDEV-2029-...`)

## Симптом

CC показывал `CpuPct: 0.0` и `NumCore: 1.0` при реальной загрузке контейнера 15–30% от 2 vCPU.

## Причина (две независимые половины бага)

1. **JVM брокера видела все 64 ядра миньона** (`AvailableProcessors: 64`, cgroup v1 `cpu.cfs_quota_us=-1`,
   Porto-лимит снаружи). Репортёр CC шлёт `getProcessCpuLoad()` — долю от ядер, которые видит JVM.
   Брокер, жгущий свои 2 vCPU на 100%, отчитывался ~3%.
2. **capacity.json имел `"CPU": "100"`** (строка). Семантика CC
   (`BrokerCapacityConfigFileResolver`): строка → `capacity[CPU]=<число>`, `numCpuCores=1`(default).
   Map-формат `"CPU": {"num.cores": "N"}` → `capacity[CPU]=100.0` (DEFAULT_CPU_CAPACITY_WITH_CORES),
   `numCpuCores=N`. В `NumCore` в `/load` попадает именно `numCpuCores`.

⚠️ Строковый `"CPU": "2"` = capacity 2% — НЕВЕРНО. Только map-формат с `num.cores`.

Дополнительно найдено: capacity.json на cruise-хосте был рассинхронизирован с PMS
(DISK 8192 на хосте vs 51200 в PMS) — render не применялся до confp --oneshot.

## Что применили на тестовом кластере (вручную, через PMS + confp)

1. PMS `kafka.cruisecontrol.capacity.json` (`test-cruise5-mdbdev-kafka.clouds`):
   `"CPU": "100"` → `"CPU": {"num.cores": "2"}`; DISK вернули 8192 (реальный диск).
2. PMS `kafka.sysconfig`: в `KAFKA_OPTS` добавили `-XX:ActiveProcessorCount=2` (первой строкой).
3. На cruise: `confp --oneshot && systemctl restart cruise-control`.
4. На всех 8 брокерах: `confp --oneshot && systemctl restart kafka-broker` (параллельно, тестовый кластер).

## Результат (сходимость подтверждена на 2.broker.dc, brokerId 20002)

| Метрика | До | После |
|---|---|---|
| JVM `AvailableProcessors` | 64 | 2 |
| CC `/load` `NumCore` | 1.0 | 2.0 |
| CC `/load` `CpuPct` | 0.0 | 0.081% (окно 5 мин) |
| JVM `ProcessCpuLoad` (12 сэмплов/120s) | ~0.2% от 64 ядер | 0.214% — сходится с CpuPct |
| cgroup контейнера (cpuacct.usage/120s) | — | 0.585 ядра = 29.2% от 2 vCPU |

Разница CC (~0.2%) vs cgroup (~29%) — НЕ баг: репортёр CC шлёт только процесс брокера JVM.
Остальная нагрузка контейнера — kafka-exporter, share-group-lag-exporter (спавнит дочерние JVM
каждые 60с), TOS agent (javaagent в KAFKA_OPTS; кратковременно может использовать все ядра
миньона → ProcessCpuLoad может скакнуть выше 2 ядер, это ожидаемо и кратковременно).

Методика сверки на хосте:
```bash
A=$(cat /sys/fs/cgroup/cpu,cpuacct/cpuacct.usage); sleep 120; B=$(cat .../cpuacct.usage)
# дельта / 1e9 / 120 = ядра контейнера
curl -s http://localhost:7777/jolokia/read/java.lang:type=OperatingSystem/ProcessCpuLoad
# сэмплы каждые 10с → avg; сверять с CC /kafkacruisecontrol/load (CpuPct, окно 5 мин)
```
CC REST: `curl -sk -u cruise:*** https://<cruise>:1443/kafkacruisecontrol/load?json=true`
(порт проброшен mcc tp-port-forward; у CC в конфиге secured + basic auth cruise).

## 2026-08-22: проверка на нагруженном кластере dsp-notices-msk-adtech-kafka → НЕ СОШЛОСЬ, откат

Кластер: 6 брокеров (dc/pc/rc × 1,2), 4 vCPU, 64-ядерные миньоны, JDK 17.0.15 (Ubuntu),
Porto, cgroup v1 quota=-1. Применили тот же фикс (num.cores=4 + APC=4), брокеров
рестартовали по одному с паузой 10с.

На загруженном 1.broker.rc (JVM реально жрёт ~1.5 ядра по /proc-дельтам и
process_cpu_seconds_total — оба метода согласны):

| Источник | Значение |
|---|---|
| /proc дельты, process_cpu_seconds_total | ~1.5 ядра (факт) |
| CC CpuPct | 1.05% от 4 ядер = 0.042 ядра |
| getProcessCpuLoad() (Jolokia) | ~0.55% = 0.02 ядра |

`getProcessCpuLoad()` занижает в ~30–70×. Числа указывают на двойную нормализацию:
1.6/(64×4) ≈ 0.6% ≈ наблюдаемое. Т.е. JDK делит на физические ядра хоста (64, не container-aware,
JDK-8226575 не полностью закрыт для этой среды) и ещё на APC. При этом `getProcessCpuTime()`
(process_cpu_seconds_total) работает ПРАВИЛЬНО — сломана именно load-функция.

На тихом cruise5 сходимость была хорошей только потому, что брокер ~0.004 ядра — занижение
маскировалось нулями.

`cruise.control.metrics.reporter.kubernetes.mode=true` НЕ помогает: у Porto quota=-1,
`getContainerProcessCpuLoad` при NO_CPU_QUOTA возвращает значение как есть.

**MSK-кластер полностью откачен** (PMS байт-в-байт к исходному: CPU:"100" строка,
sysconfig без APC; круиз+6 брокеров confp+restart, AvailableProcessors снова 64).

## Вывод и варианты фикса (форка CC-сервера у нас НЕТ, сервер стоковый)

`ActiveProcessorCount` + `num.cores` чинят NumCore/capacity/goals, но НЕ чинят
CPU-метрику репортёра (getProcessCpuLoad сломан на уровне JDK в Porto-среде).

1. **Обновить JDK в образе** (сейчас Ubuntu 17.0.15) — container-awareness getProcessCpuLoad
   правили в поздних 17.x/21+. Проверить тестовым образом с JDK 21 на cruise5 под нагрузкой.
2. **Свой cruise-control-metrics-reporter.jar** — репортёр отдельный jar в /opt/kafka/libs/,
   CC-сервер остаётся стоковым. Патч: утилизация из дельт getProcessCpuTime()/(interval×cores).
3. **PrometheusMetricSampler на сервере CC** (параметр cruisecontrol.properties, шаблон в
   backstage): BROKER_CPU_UTIL из rate(process_cpu_seconds_total) у VictoriaMetrics.
   Минус — доступ круиза к VM API.

Порядок: (1) дешёвая проверка JDK → если нет, (2) патч репортёра.
Перед выбором — воспроизвести баг под нагрузкой на cruise5 (залить трафик, убедиться
в занижении на тесте, а не только на проде).

## 2026-08-22 (вечер): конфиг-ONLY фикс найден и подтверждён точной сходимостью

### Root cause (точный)

`getProcessCpuLoad()` в JDK (17.0.15 И 21.0.7, оба проверены) считает
`ΔProcessCpuTime / ΔTotalCpuTime(/proc/stat)`. В Porto-контейнере `/proc/stat` —
**глобальный по миньону** и не виртуализирован (нет lxcfs). Знаменатель Z = число
CPU в `/proc/cpuinfo` миньона: dc/pc-миньоны = 128, ic-миньоны = 112 (гетерогенно
по железу!). APC (ActiveProcessorCount) на знаменитель НЕ влияет (проверено probe:
PCL одинаков при APC=1/2/4/8/64/none).

`getProcessCpuTime()` при этом работает ПРАВИЛЬНО всегда (process_cpu_seconds_total
сходится с /proc-дельтами 1-в-1). JDK 21 НЕ чинит (проверено на живом брокере —
занижение то же ~50-100x; МР docker-images !127 с JDK 21 остаётся как эксперимент).

### Фикс без правки кода

capacity.json строковый формат `"CPU": "<vCPU×100/Z>"` задаёт capacity напрямую.
Z=128 (типовой миньон): 2 vCPU → "1.5625"; 4 vCPU → "3.125".

### Верификация на cruise5 (стационарная нагрузка ~3.3 MB/s: 3 продюсера lz4,
### топик zstd (брокер сам жмёт), batch 256K, throughput -1, acks=1,
### retention.ms=60000 + segment.ms=60000 + file.delete.delay.ms=0)

| Источник | Значение |
|---|---|
| Хост /proc/PID/stat (120s)         | 0.573 ядра = 28.7% |
| JVM process_cpu_seconds_total      | 0.573 ядра = 28.7% |
| CC /load CpuPct (окно 5 мин)       | **28.27%** ✅ |

Расхождение факт↔CC = 1.5%. CpuCapacityGoal с порогом 70% от 1.5625 срабатывает
при 70% реальной загрузки 2 vCPU — goals честные.

NumCore в /load при строковом CPU показывает capacity/100 (0.015625) — косметика.

### Грабли, пойманные в этот день

1. **modify-флоу затирает ручные правки capacity.json**: resize диска 8→10ГБ
   перегенерил capacity из шаблона (CPU снова "100", DISK=10240). ФИКС ДОЛЖЕН
   ЖИТЬ В ШАБЛОНЕ backstage, иначе живёт до первого modify.
2. Z гетерогенный по ДЦ: ic-миньоны 112 vs dc/pc 128 → ошибка capacity ±12.5%
   на ic при хардкоде 128. Точный фикс per-host требует правки кода (репортёр).
3. Сжатие делает КЛИЕНТ: gzip/lz4 на продюсере не грузит брокера. Грузить брокера:
   топик compression.type=zstd (или gzip) + продюсер lz4 → брокер разжимает+жмёт;
   + retention.ms=60000/segment.ms=60000 → постоянный ролл+удаление сегментов;
   + консюмеры для TLS-отдачи (не понадобилось).
4. Один продюсер-поток упирается в ~2.2 MB/s сети (shaping между контейнерами) —
   для большей нагрузки несколько продюсеров с разных хостов.
5. Стресс-топик с RF=1 на 2-vCPU брокере с диском 8ГБ забивает диск за минуты
   (брокер падает exit=1). Следить за df, диск расширять заранее.
6. mcc sshexec убивает дочерние процессы при закрытии сессии — нагрузку гонять
   через `systemd-run --unit=... --collect` (+ скрипт с base64 через echo).

### 2026-08-22 (финал): msk-проверка конфиг-фикса → провал, полный откат обоих кластеров

Проверили схему `"CPU": "3.125"` (4 vCPU, Z=128) на 1.broker.rc msk без искусственной
нагрузки (собственная ~1.7 ядра):

| Источник | Значение |
|---|---|
| Хост /proc                  | 1.68 ядра = 42.1% от 4 vCPU |
| JVM process_cpu_seconds     | 1.69 ядра (идеально сходится) |
| JVM getProcessCpuLoad       | **0.0 во всех сэмплах 2+ часа** |
| CC CpuPct                   | 0.000% (capacity 3.125 применился, NumCore=0.03125) |

PCL на этом брокере деградировал с 0.5% (утро) до 0.0 — JDK-функция нестабильна:
иногда занижает в 50-100×, иногда отдаёт константный ноль. Конфиг-фикс работает
только при живом (пусть и заниженном) PCL.

**Оба кластера откачены в исходное состояние:**
- msk: capacity CPU="100"/DISK=307200 (PMS байт-в-байт, круиз рестартован) ✅
- cruise5: см. ниже — ОСТАВЛЕН фикс CPU="1.5625"/DISK=10240 (тестовый полигон) ✅

## ИТОГ MDBDEV-2029 (что доказано экспериментально)

1. Root cause: getProcessCpuLoad() в JDK 17/21 в Porto-контейнере делит на
   /proc/stat миньона (Z=128/112, гетерогенно) или ломается до нуля.
   getProcessCpuTime() работает всегда идеально.
2. Конфиг-фикс (CPU = vCPU×100/Z строкой в capacity.json) даёт сходимость 1.5%
   на стационарной нагрузке КОГДА PCL живой — но ненадёжен (PCL может быть 0.0)
   и живёт до первого modify-флоу.
3. Правильный фикс — патч репортёра cruise-control-metrics-reporter: утилизация
   из дельт getProcessCpuTime()/(interval×APC). Механизм сборки/публикации своего
   jar есть (nexus, CRUISECONTROL_VERSION=2.5.141 в 20-packages.sh).
4. num.cores map-формат — не фикс (capacity остаётся 100 при сломанной метрике).
5. МР docker-images !127 (JDK 21) — не чинит (проверено), можно закрыть.

Текущее состояние артефактов:
- МР backstage !234 — открыт, шаблон Capacity надо переделать (см. рекомендации).
- МР docker-images !54 (роли sysconfig) — актуален.
- МР docker-images !127 (JDK 21) — эксперимент, не чинит PCL.
- cruise5: JDK 21 + APC=2 в sysconfig + capacity CPU="1.5625" — рабочий полигон.
- msk: полностью исходное состояние.

1. **Шаблон `kafka-cruise-control-capacity`**: `"CPU": "$CPU"` → `"CPU": {"num.cores": "$CPU"}`
   (строковый формат рендерил бы неверную capacity 2%).
2. Шаблон `kafka-sysconfig` в МР уже добавляет `-XX:ActiveProcessorCount=${ACTIVE_PROCESSOR_COUNT}` — ок.
3. controller-sysconfig в МР без ActiveProcessorCount — controller тоже может хотеть (не критично,
   метрики CC шлют только брокеры).
4. МР docker-images !54 — разделение sysconfig по ролям (BROKER/CONTROLLER через KAFKA_ROLE), ок.

## Грабли

- `mcc sshexec` executor'ы параллельно на 7+ хостов: часть TLS handshake timeout (Attempt 1/2) —
  ретраи mcc сам делает, команды проходят; вывод фильтровать от ANSI.
- На хостах нет python3 — арифметику cgroup-дельт делать локально.
- PMS `values.do`: параметры `application=mdb&property=<name>` (без hostName), ответ — map
  hostName→value, доставать `jq -j '.["<queue>.clouds"]'` (байт-в-байт для последующего cmp).
  Прямой curl с `applicationName=`/`hostName=` в query даёт HTTP 400.
