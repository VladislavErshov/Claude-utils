# MDBDEV-3145 — CPU throttling на Kafka 4.3 vs 3.8 (dsp-notices-msk vs dsp-notices-spb)

Дата: 2026-08-21, продолжение 2026-08-24. Статус: **живые замеры пика сняты; вывод — см. ниже**.

## Симптом

В UI mdb-data у кластера `dsp-notices-msk-adtech-kafka` (Kafka **4.3.0**, 4 vcores / 8 GB,
брокеры: dc/pc/rc × 1.broker + 2.broker, cruise в rc) колонка CPU throttled до **82%** на
1.broker.* и 5–20% на 2.broker.*. У `dsp-notices-spb-adtech-kafka` (Kafka **3.8.0**,
2 vcores / 8 GB, ic/nc/zc) троттлинга нет (~0.1%). Задача — объяснить разницу.

Что такое троттлинг в UI (кратко, не копать): алерт `mdb-hardware-cpu-throttled-*` из
`runtimeStatsLimitChecker` c `type=vcores_overq` — Porto-троттлинг контейнера на уровне
хостера. Шаблон: `backstage/plugins/mdb-backend/src/task/manifest/templates/alert-service-settings`.
Изнутри контейнера лимит не виден (`cpu.cfs_quota_us=-1` в cgroup v1).

## Методика замеров

- CPU контейнера целиком: дельта `/sys/fs/cgroup/cpu,cpuacct/cpuacct.usage` (внутри libpod
  под Porto; виден ВЕСЬ контейнер, включая детей-JVM, в отличие от `process_cpu_seconds_total`
  который видит только брокера).
- CPU по тредам брокера: `awk '{print $14+$15}' /proc/$PID/task/$T/stat` (utime+stime в ticks)
  + `/proc/$PID/task/$T/comm`, сортировка, деление на uptime (`process_start_time_seconds`).
- ⚠️ `nproc`/`loadavg`/`uptime` через mcc — это данные миньона, не хоста (см. check_metrics.md).

## Ключевые цифры (средние ядра за uptime ~22ч)

«Тихие» брокеры для честного сравнения версий:

| Компонент | msk 2.broker.dc (**4.3**) | spb 1.broker.ic (**3.8**) |
|---|---|---|
| Контейнер целиком (cpuacct) | **0.60** | **0.21** |
| `data-plane-kafka-network` треды | 0.20 (4 шт.) | **0.011** (в 17 раз меньше) |
| `prometheus-http` (JMX exporter 8080) | 0.196 | 0.164 |
| `ReplicaFetcher` треды | 0.067 | 0.003 |

Нагруженный 4.3 (msk 1.broker.dc): контейнер **1.99 ядра**; data-plane 0.94, prometheus-http 0.2,
ReplicaFetcher 0.3. Трафик ~9.4 МБ/с in + out (кумулятив BytesIn/BytesOut ÷ uptime).
GC здоровый на обоих (G1 Young 106с/22ч у 4.3, 34с у 3.8, Old GC = 0) — утечки нет.
Клиентских/дефолтных квот нет, throttle-квантили Produce/Fetch = 0 (кумулятивный
throttle-time ненулевой на ОБОИХ — не отличие). Sysconfig идентичен: heap 2G, G1,
TOS agent `mdb-kafka-tos-agent.jar=mode=dynamic,cpuClass=batch` на обоих.

## Главный version-специфичный виновник: share-group-lag-exporter (KIP-932)

- На 4.3 экспортер каждые **60с спавнит 2 полноценные JVM** (`kafka-share-groups.sh --offsets`
  + `--state`, каждая `-Xmx256m`, ~3s wall, ~2–4 CPU-сек). Поймали ребёнка
  `ps --ppid $(pgrep -f share_group_lag_exporter.py)` → `java -Xmx256m`.
  Дефолты: `SHARE_GROUP_LAG_SCRAPE_INTERVAL=60`, `DEFAULT_COLLECT_TIMEOUT=90`
  (`/opt/share-group-lag-exporter/share_group_lag_exporter.py`).
- Каждая сессия = SASL_SSL handshake → работа data-plane тредов брокера (коннект-черн).
- **Share groups на кластере 0 штук** — `share_group_lag{}` пустая, collect вхолостую.
- На 3.8 скрипта `/opt/kafka/bin/kafka-share-groups.sh` **нет в дистрибутиве** (KIP-932 — фича 4.x)
  → `last_collect_error{error="script not found..."} 1`, стоимость ~0. Сервис при этом `active` на обоих.
- Оценка стоимости на 4.3: ~0.1 ядра/брокер на JVM-спавнах + коннект-черн.

## Второй фактор: JMX exporter

`prometheus-http` (jmx_prometheus_javaagent-0.19.0) — №1 потребитель CPU на ОБОИХ кластерах
даже без нагрузки (0.16–0.2 ядра). На 4.3 тяжелее: /metrics **636KB vs 568KB** (добавились
KRaft/share-coordinator MBean'ы — плата за 2.4.2 в changelog 3.8-образа, где MBean'ы пустили
через blacklist). Топ-тред на тихих брокерах обоих кластеров.

## Третий фактор (конфиг, не версия): потоки на msk

`dsp-notices-msk-adtech-kafka`: `num.network.threads=2, num.io.threads=2` (минимум!) при
4 vcores и ~350 FetchFollower-запросов/с (счётчик `requestspersec` FetchFollower ≈ 3.1E7 за 22ч).
spb: `num.network.threads=3, num.io.threads=8`. ApiVersions (новые соединения): msk ~18.9k/22ч
(0.24/с/брокер... точнее смотреть по брокеру), spb ~37.5k/28ч — черн соединений сопоставим,
но на msk каждый коннект дороже (TLS handshake на 2 network-тредах).

## Почему всё же «4.3 жрёт больше» — сводка

1. **share-group-lag-exporter**: ~0.1 ядра + коннект-черн только на 4.3 (на 3.8 скрипта нет).
2. **Тяжелее /metrics** (636KB vs 568KB) → дороже каждый scrape Prometheus.
3. **Больше фоновых MBean'ов/Raft-активности** — data-plane треды на пустом брокере 0.20 vs 0.011
   (частично это нагрузка msk-кластера: replication-трафик ~9 МБ/с средн.; чистого «idle-шума» 4.3
   отдельно ещё НЕ вычленили — открыто).
4. Конфиг потоков 2/2 на msk усиливает эффект (очередь к 2 тредам → дольше занят CPU).

## Рекомендации (не применялись)

1. `SHARE_GROUP_LAG_SCRAPE_INTERVAL` 60 → 300–600с, либо выключать экспортер при пустом
   `--list` N раз подряд (по сути баг: гоняет JVM без share groups).
2. Поднять `num.network.threads`/`num.io.threads` с 2 на msk (modify-флоу).
3. Опционально: обновить jmx_prometheus_javaagent (0.19.0 медленный).

## Продолжение 2026-08-24 (пн, ~16:25 МСК, пиковая нагрузка) — живые замеры

Методика: дельта `cpuacct.usage` и per-thread `/proc/$PID/task/*/stat` за ~50с интервал;
rates трафика/запросов по JMX-счётчикам за ~30с; история = кумулятивные счётчики ÷ uptime.
(Брокеры msk перезапускались сб ~12:39, uptime 51ч; spb не перезапускался, uptime 100ч.)

### Живой срез пика

| Метрика | msk 1.broker.dc (4.3) | msk 2.broker.dc (4.3) | spb 1.broker.ic (3.8) |
|---|---|---|---|
| Контейнер CPU, ядер | **1.48** (из 4) | 1.25 | 0.28 (из 2) |
| data-plane треды (сумма) | 1.03 | 0.29 | **0.004** |
| prometheus-http (JMX exporter) | 0.23 | 0.25 | **0.20 = 70% CPU хоста** |
| ReplicaFetcher | 0.25 | 0.32 (догоняет) | — |
| Client in/out, MB/s | 11.3 / 8.3 | 3.4 / 3.4 | **0 / 0** |
| Replication in/out, MB/s | 13.2 / 22.7 | 18.9 / 6.8 | 0 |
| Fetch / FetchFollower / Produce /с | 2078 / 1854 / 1791 | 884 / 572 / 326 | **0 / 0 / 0** |
| GC G1 Young с старта | 304с / 51ч (Old=0) | — | 114с / 100ч (Old=0) |

2.broker.pc: 10.5 MB/s in (тоже нагружен), 2.broker.rc: 4.2 MB/s. ApiVersions ≈ 1/с —
коннект-черн незначим (фактор «черн соединений» из первой части снят).

### История (средние с момента старта)

- msk 1.broker.dc: **1.54 ядра @ 10.3/10.2 MB/s** — текущий пик (1.48 @ 11.3) ≈ среднему →
  **спайки троттлинга в UI эпизодические, не фон**.
- msk 2.broker.dc: 0.51 @ 0.9 MB/s; pc: 3.0; rc: 1.7 MB/s.
- spb 1.broker.ic: 0.19 ядра @ **0.023 MB/s за 100ч** — кластер idle все 4+ суток.
- Пятничные 22ч-средние (1.99 @ 9.4) были выше текущих — окно включало более тяжёлый период.

### Главные выводы продолжения

1. **Прямое сравнение 3.8 vs 4.3 «при равной нагрузке» на этой паре невозможно**: spb 3.8
   idle (0 req/s в пик пн, 0.02 MB/s среднее за 100ч). Вся нагрузка dsp-notices — на msk 4.3.
   Отсутствие троттлинга на spb — следствие нулевого трафика, не версии.
2. **Скеw лидерства на msk**: 1.broker.dc несёт ~65% produce кластера (10.3 из 15.9 MB/s) —
   поэтому троттлинг локализовался на 1.broker.*. Вопрос к Cruise Control / размещению партиций.
3. **prometheus-http (jmx_prometheus_javaagent 0.19.0): 0.2–0.25 ядра на КАЖДОМ брокере
   обеих версий** — на idle 3.8 это 70% CPU. Универсальный мониторинг-налог; апгрейд
   javaagent — общая оптимизация, не только для 4.3.
4. GC здоровый на обеих версиях — утечек нет (включая TOS agent).
5. share-group-lag-exporter на 4.3 продолжает collect каждые 60с (last_success обновляется),
   на 3.8 last_success=0 — поведение из первой части подтверждено.

### Открытые вопросы (обновлённые)

- Чистый idle-шум 4.3 не измерен: на msk НЕТ брокера без трафика (у всех репликация).
  Вариант: dev-кластер 4.3 без нагрузки или временно выведенный брокер.
- Внутренняя структура CPU data-plane (SSL vs сериализация vs Raft) — jfr/async-profiler
  на 1.broker.dc в момент спайка (вопрос Дужинского в Jira). Сейчас (1.48/4 ядер)
  профилировать малоинтересно — нужен момент троттлинга.
- vcores_overq в VictoriaMetrics — доступ из рабочей среды не найден; при необходимости
  смотреть через дашборд mdb/hardware alerts.

## Хосты (для продолжения разбора)

msk 4.3 (4 vcores): 1.cruise.rc, controllers dc/pc/rc, brokers: 1.(dc/pc/rc) + 2.(dc/pc/rc).
spb 3.8 (2 vcores): 1.cruise.ic, controllers ic/nc/zc, brokers 1.(ic/nc/zc).
Троттлинг UI на msk: 1.broker.* = 66–82% cpu / 27–70% throttled; 2.broker.* = 29–47% / 5–20%.
