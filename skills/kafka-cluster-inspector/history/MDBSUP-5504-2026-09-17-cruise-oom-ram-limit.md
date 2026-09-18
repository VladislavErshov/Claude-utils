# MDBSUP-5504 (2026-09-17) — cruise-control OOM: лимит контейнера 2G при heap 4G

Кластер: `ssp-requests-adtech-kafka` (`02da7d4b-598b-4ef1-bbd6-b6d37d9f2d2b`, ns=infra, Kafka 3.8, cruise 2.5.147).
Cruise-хост: `1.cruise.ssp-requests-adtech-kafka.rc.one-infra.ru` (единственный, ДЦ rc).

## Первопричина

Манифест cloud-сервиса `cruise.ssp-requests-adtech-kafka` (submitted e.selivanov 2026-03-31):
`alloc: vCPU=2, mem=2G` → cgroup v1 `memory.limit_in_bytes=2147483648` (2 GiB),
а JVM круиза с дефолтом шаблона confp получает `-Xms4096m -Xmx4096m`. Heap 4G в
контейнере 2G → гарантированный container-OOM/crashloop (`OutOfMemoryError` в
`cruise-control.err.log` из AnomalyDetector/jetty/TopicMinIsrCacheCleaner).

⚠️ `free -h` на хосте показывает RAM minion'а (503G) — реальный лимит контейнера
только в cgroup (v1: `/sys/fs/cgroup/memory/memory.limit_in_bytes`, v2: `memory.max`;
пути зависят от контейнера — проверять оба).
⚠️ `mcc status` сервиса: `scheduling.demand/allocated MEM=2G` — самый быстрый способ
увидеть лимит без ssh.

## Сопутствующее: конфиги круиза в PMS

- `kafka.cruisecontrol.sysconfig` — `<NOT_SET>` → рендерился дефолт шаблона (heap 4096m).
- `kafka.cruisecontrol.properties` — задан, но устаревший короткий шаблон (jinja `{% if %}`,
  закомментированные дефолты) со **стейл-bootstraps**: только kc/pc/rc — dc/uc добавлены
  add_hosts 2026-09-16, в списке их не было.
- Эталон полного конфига: `test-cruise5-mdbdev-kafka.clouds` → `kafka.cruisecontrol.properties`
  (полный LinkedIn-дефолт; его bootstrap-лист = ВСЕ брокеры кластера — паттерн подтверждён
  по host_state).

## Что сделано

1. **PMS `kafka.cruisecontrol.sysconfig`** (ключ `ssp-requests-adtech-kafka.clouds`, ns=infra):
   записан отрендеренный `/etc/sysconfig/cruise-control` с хоста байт-в-байт (heap 4096m).
   Update.do → verify byte-identical.
2. **PMS `kafka.cruisecontrol.properties`**: взят raw из values.do с
   `test-cruise5-mdbdev-kafka.clouds` (16 759 байт), подменена только строка
   `bootstrap.servers=` на все 15 брокеров ssp-requests (1..3 × dc,kc,pc,rc,uc:9092).
   Update.do → verify byte-identical.
3. **Облако**: `mcc manifest` → правка `alloc.mem: 2G→6G` (+comment MDBSUP-5504) →
   `mcc -n infra -c rc submit <file> -t service`. Сабмит от vl.ershov 13:00:38,
   без уравнения-подтверждения.
4. **Применение**: облако обновило контейнер **in-place** (java-процесс не перезапустился,
   конфиги не перерендерились!) — лимит поднялся до 6442450944 на живом контейнере.
   Затем вручную: `confp --oneshot` (рендер 15 bootstraps) + `systemctl restart cruise-control`.

## Верификация

- `mcc status`: `allocated: vCPU=2 MEM=6G`, state RUNNING.
- cgroup limit 6442450944; usage ~1.4G и стабильно.
- `/opt/cruise-control/config/cruisecontrol.properties`: 15 bootstraps.
- java с `-Xmx4096m`, стартовал 13:05:09, `/state` → 200, `isProposalReady:true`,
  в err.log после рестарта только gson-варнинги, OOM нет.

## Продуктовые баги → MDBDEV-549 (комментарий разработчикам)

1. **Provisioning**: по п.8 MDBDEV-549 круиз должен получать RAM 4, фактически манифест
   был `mem=2G` — несовместимо с дефолтным heap 4G → любой новый кластер с круизом
   получит тот же crashloop. Предложение: синхронизировать дефолт RAM с heap-дефолтом.
2. **bootstrap.servers не обновляется при upscale**: в `kafka.cruisecontrol.properties`
   отсутствовали брокеры добавленных ДЦ (dc/uc, add_hosts 2026-09-16) — конфиг круиза
   сталеет после add_hosts. Предложение: обновлять bootstraps при add_hosts.

## Комментарий в тикет

Правка по text-writer: причина (2G контейнер vs 4G heap) + что подняли/зафиксировали +
состояние «работает штатно». Внутренняя кухня (mcc submit, PMS-ключи, verification) —
только здесь. mdb-solver-stub: [MDBSUP-5504-2026-09-17.md](../../../jira-mdbsup-solver/history/MDBSUP-5504-2026-09-17.md).

## Грабли

- `mcc submit` манифеста с изменённым alloc обновляет контейнер **без рестарта процесса**
  — PMS-конфиги применяются только после ручного `confp --oneshot && systemctl restart`.
- Патч heap-строки в конфиге: `grep '^bootstrap.servers=' | tr ',' '\n' | wc -l` — быстрый
  счётчик брокеров в рендере.
- exit code 1 от `grep -c` без совпадений sshexec отдаёт как `Error: non zero exit code`
  — не ошибка хоста.
