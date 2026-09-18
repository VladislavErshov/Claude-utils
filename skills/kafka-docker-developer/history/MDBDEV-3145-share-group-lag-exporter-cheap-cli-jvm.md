# MDBDEV-3145 — share-group-lag-exporter: дешёвый старт дочерних CLI JVM (C1+SerialGC+CDS)

Дата: 2026-09-15/16. Статус: MR !145 (docker-images), ветка `MDBDEV-3145-share-group-lag-cli-jvm-opts`, на ревью у @d.duzhinskiy + @svc-gena.
Диагностика (как нашли) — `kafka-metrics-investigator/history/2026-09-15-MDBSUP-3644-crash-ratelimit-cpu-throttled-idle.md`.

## Проблема

Экспортер (python, ubuntu20-kafka-base) каждые ~60с спавнит 2 JVM
(`kafka-share-groups.sh` → `kafka-run-class.sh` → ShareGroupCommand) даже при нуле
share groups. Дети наследуют дефолт kafka-run-class.sh (`-server -XX:+UseG1GC`):
G1 разворачивает 38–43 GC-треда (под 56–64 CPU хоста, JVM не знает про квоту 4 vcores)
+ полный tiered JIT. Спавн стоил **4.8 CPU-s**, бёрсты до +0.5 ядра → Porto vcores_overq
→ CPU throttling в UI на пустых 4.3-кластерах. На 3.8 скрипта kafka-share-groups.sh нет —
экспортер бесплатен, троттлинга нет (test-downgrade7, major-62, dsp-notices-spb).

## Фикс (2 файла)

1. `ubuntu20-kafka-base/.../share_group_lag_exporter.py`: `_build_env()` передаёт детям
   `KAFKA_JVM_PERFORMANCE_OPTS = CLI_JVM_OPTS` (TieredStopAtLevel=1, UseSerialGC,
   CICompilerCount=2, ActiveProcessorCount=2, PerfDisableSharedMem) и добавляет
   `-XX:SharedArchiveFile` **только при наличии** архива (`os.path.isfile`).
2. `ubuntu20-kafka-4.3.0/rootfs/docker/build.d/35-share-groups-cds.sh`: генерация
   `/opt/kafka/share-group-lag-cli.jsa` тренировочным прогоном ShareGroupCommand
   на мёртвом эндпоинте (127.0.0.1:1, таймаут 2с) — офлайн грузит весь
   AdminClient/TLS-стек, ArchiveClassesAtExit пишет полный архив.

## Замеры

| Конфигурация детей | CPU на спавн | wall |
|---|---|---|
| дефолт | 4.76 CPU-s | 2.57с |
| C1+SerialGC | 2.79 | 2.38с |
| **C1+SerialGC+CDS** | **1.87 (−60%)** | **1.56с** |

Прод-хот-деплой (v-analytics 19.ic/19.zc): C2 в спавн-окнах 20–44% → 0%,
окна 0.64–0.82 → 0.46–0.55 ядра. 4.3-фикс vs 3.8: 0.52 vs 0.17 ядра
(остаток — JMX-налог + share-lag хвост ~0.11 + KRaft).

## Грабли

- **CI-сборка (TeamCity Mdb_Kafka_Kafka430)**: `/opt/kafka/config/client.properties`
  не существует при сборке (рендерится confp на хосте) — `cp` ронял build.
  Фоллбек: пустой train.properties + таймауты 2с.
- **mcc scp молча падает** (заливка скрипта на хост); рабочий путь — base64 чанками
  по 2400 символов через sshexec (5К уже даёт 431), md5-сверка.
- **pkill -f убивает собственный shell** (командная строка содержит паттерн) —
  экранировать класс `[r]` или проверять по `pgrep -fa` без self-match.
- CDS-архив валиден только под те же флаги/JDK — SerialGC обязателен и в тренинге,
  и в рантайме; тренинг строго внутри образа (apache/kafka#15771).
- CICompilerCount=1 валиден с TieredStopAtLevel=1 на Java 17.0.15 (проверено),
  но для портабельности поставили 2 (замечание ревью).
- Версионирование: `versions.md` (Current + секция 1.0.4) для 4.3.0;
  датированная запись в base `changelog.md` (экспортер живёт в base-образе!).
- ВАЖНО: пересборка **версионного** 4.3.0-образа не пересобирает **base** —
  python-патч экспортера живёт в base. Собирать оба или hot-reload скрипта.

## Откаты

- Хосты: `cp *.bak-3644` (или git-revert script) + `systemctl restart share-group-lag-exporter`.
- MR: revert коммита; CDS-архив можно удалять независимо — дети стартуют без CDS с warning.
