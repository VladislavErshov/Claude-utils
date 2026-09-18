# Облачные метрики Porto (CPU/vcores) через mcc

Источник облачных цифр, которые рисуются в UI/goc: **сам Porto-мастер**. Доступ без Grafana/VictoriaMetrics
(доступа к VM с рабочей машины нет — URL не угадывать). Снято в рамках MDBSUP-3644/MDBDEV-3145 (2026-09-15).

## Utilization очереди (demand/allocated/utilization)

```bash
mcc -n infra status <queue-name>
```

Вывод:

```
name: crash-ratelimit-kafka.ratelimit.db.production.mdb.prod
  demand: vCPU=54 MEM=106G LAN in=4825Mbit out=3060Mbit NVME=150G HDD=14.06T
  allocated: vCPU=54 MEM=...
  utilization: vCPU=16.61 MEM=36.02G LAN ... (31%cpu 34%ram 26%in 16%out 5%nvme 31%hdd)
```

- `demand`/`allocated` — гарантия очереди в vcores (сумма `cloud_vcores` инстансов).
- `utilization` — потребление по учёту Porto за короткое окно (минуты): **колеблется между
  вызовами** (примеры одной очереди: 21.36 → 16.61 vCPU за ~2 мин). Не долгосрочное среднее!
- `(NN%cpu)` = `utilization / allocated` очереди. Проверено: 16.88/48 = 35%cpu; 7.51/28 = 27%cpu.
- Соответствие с `cpuacct.usage` внутри контейнера **по среднему сходится**
  (dc-шард ~1.2–1.6 vCPU/инстанс по Porto ↔ 1.31 ядра замером; sc ~1.07 ↔ 0.34 на самом тихом
  брокере — шард шире одного брокера). Расхождения — эффект окна, Porto включает бёрсты.

⚠️ **Шарды MDB-кластера живут в разных очередях** (пример crash-ratelimit-kafka):
- dc-шард: `crash-ratelimit-kafka.ratelimit.db.production.mdb.prod` (брокеры dc + controller + cruise)
- sc-шард (добавленный через add_hosts): `broker.crash-ratelimit-kafka.ratelimit.db.production.mdb.prod`
- Поиск: `mcc -n infra queues "%<alias>%"` находит не все шарды — надёжнее `mcc status` по
  предполагаемым именам; список инстансов очереди видно в `mcc status <queue>`.

## Spec инстанса (без live-метрик)

```bash
mcc -n infra tool_status --type instance -f json <fqdn>
```

Даёт `cloud_vcores`, `cloud_mem`, `cloud_image`, minion и пр. Live-утилизации в tool_status НЕТ — только `mcc status` очереди.

## Метрики UI/Grafana (семейство one_cloud_*, источник — Porto)

- **CPU Usage в Grafana** = `one_cloud_cpu_percent_value{cloud_service=~".*.<cluster>"}` —
  мгновенный CPU контейнера в % от гарантированных vcores (та же цифра, что `NN%cpu` в
  `mcc status` очереди). Окно Porto включает бёрсты → значение выше мгновенного cpuacct
  (пример: 11% cpuacct ↔ 24% Porto на пустом брокере v-analytics).
- Семейство: `one_cloud_memory_limit_value`, `one_cloud_memory_rss_swap_shmem_value`,
  `one_cloud_lan/wan_bits_in/out_value`. PromQL-примеры — `grafana-plot-creator/dashboard/*.json`, панель id=109.
- **«CPU throttled» / алерт** = Porto runtime-стат `vcores_overq` — потребление сверх
  гарантии (overqueue). Алерты `mdb-hardware-cpu-throttled-{1,5,15}min`:
  `runtimeStatsLimitChecker, args: type=vcores_overq,period=...,maxPercent=CPU_THROTTLED_PERCENT`
  (шаблон `backstage/plugins/mdb-backend/src/task/manifest/templates/alert-service-settings`).
  Чекер облачный closed-source, на хосте отсутствует (проверено 2026-09-15).
- Т.е. «throttled» растёт от **бёрстового спроса сверх vcores** (GC 38–43 тредов, скрейпы,
  спавны share-lag JVM) даже при низком среднем — на idle-кластерах метрика максимальна.

## Что это даёт для разбора троттлинга (vcores_overq)

- Колонка UI «CPU %» по брокеру = Porto utilization брокера / его `cloud_vcores`.
- Портовская «троттлинг»-статистика (vcores_overq / колонка «throttled» в goc) — учёт
  **мгновенного спроса сверх гарантии**: спарки 40+ тредов JVM (GC 38–43 тредов при
  ParallelGCThreads по CPU хоста, network processors, scrape) дают спрос ≫ 4 vcores
  при низком среднем → метрика раздувается на idle-брокерах. Точная формула семантики
  Porto-мастера не подтверждена (открытый вопрос этого разбора).
- Верификация фиксa `-XX:ActiveProcessorCount=<vcores>`: (1) счетчик GC-тредов падает до N;
  (2) `mcc status <queue>` utilization до/после; (3) UI throttled по брокеру против соседей (A/B).

## Грабли

- `mcc status <queue>` может сматчить соседнюю очередь по префиксу — сверяйте `demand` c
  ожидаемым числом инстансов × cloud_vcores.
- `%cpu` считается от allocated **очереди**, а не одного инстанса.
- Porto-utilization — не замена cpuacct-замерам: окна разные, Porto включает бёрстовый спрос.
