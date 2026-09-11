# Цели CC notReady из-за смерти миньона: брокер без CPU-метрик + cruise на мёртвом миньоне

**Дата**: 2026-09-11
**Кластер**: `pub-processing-dzencontent-kafka` (dzen, ns=dzen, 4 брокера pc/ec/dc/rc по 1 шт., Kafka 3.8.0)
**Хосты**: `1.broker.<cluster>.dc.idzn.ru` (brokerId 20001, виновник), `1.cruise.<cluster>.rc.idzn.ru` (жертва)
**Оба стояли на миньоне `srvr24018`, который умер.**

## Симптом

В mcc `NO_TASK_IN_PROGRESS; Proposals are not ready`; в `/state`:
`NumValidWindows: 0/5`, `NumValidPartitions: 55/131 (42.7%)` < 95%;
goals `DiskCapacity/CpuCapacity/NetworkInbound/NetworkOutbound/DiskUsageDistribution` — `notReady`
(`minMonitoredPartitionsPercentage=0.95`), RackAware/TopicReplica/LeaderReplica — ready.

## Цепочка причины

1. Миньон `srvr24018` умер (реестр: `mcc minions srvr24018` → EntityNotFound).
2. Контейнер dc-брокера на нём сломался: в `/sys/fs/cgroup/cpu,cpuacct/` НЕТ
   `cpuacct.usage*` (только cpu.cfs_*) → JDK `getProcessCpuLoad()` недоступен →
   `CruiseControlMetricsReporter: Failed reporting CPU util: java.io.IOException: JVM recent CPU usage is not available`
   каждую минуту с момента смерти миньона. Брокер при этом сам жив, остальные метрики
   (disk/bytes) репортит — молчание репортера в логе ≠ здоровье, тут он орал WARN.
3. RF=3 из 4 брокеров: партиция валидна только если сэмплы есть от ВСЕХ реплик →
   отсутствие CPU-метрик одного брокера валидно помечает ~3/4 партиций → покрытие 42.7%.
4. Cruise-инстанс на том же миньоне: запись RUNNING (стейл), автостарт
   «cannot start by either reason», sshexec → `Minion srvr24018 is not running`.

## Диагностика (грабли)

- **`/sys/devices/system/cpu/online` = `0` — НЕ признак поломки**, так и в здоровых
  контейнерах (проверено на rc-брокере). Признак — отсутствие `cpuacct.*` в cgroup.
- namespace для idzn.ru — `-n dzen` (в infra хост не ищется); retry sshexec 3-5 раз.
- CC webserver port **8080** (`webserver.http.port` в
  `/opt/cruise-control/config/cruisecontrol.properties`), не 9090.
- kafka CLI: `kafka-topics.sh --command-config`, но `kafka-console-consumer.sh` —
  **`--consumer.config`** (иначе usage-хелп). bootstrap — FQDN, не localhost (SAN).
- Вывод describe: партиции идут строками с ведущим TAB — `grep '^Topic:'` ловит только
  шапку; нужны `grep -P '^\tTopic:'` или `--unavailable-partitions`.
- Кто молчит/орёт: `grep 'Failed reporting CPU util' /mnt/logs/dbms/kafka-broker.out.log`
  по всем брокерам; «Starting Cruise Control metrics reporter» есть только в логе с
  момента старта брокера (ротация gz прячет прошлые старты — искать в
  `kafka-broker.out.*.log.gz`).

## Фикс

1. Брокер: `mcc --local -n dzen -c dc migrate --relocate --auto_solve
   "pub-processing-dzencontent-kafka.dzencontent.db.production.mdb.prod/broker/1"`
   → переехал на `srvd8197`, контейнер пересоздан, `cpuacct.*` появились, после рестарта
   репортера CPU-фейлов нет. Целиком совпадает с кейсом onemekafkaauth38 (2026-08-27).
2. Cruise: storage под именем `cruise` в реестре НЕТ (stateless, tool_status EntityNotFound),
   `mcc migrate` неприменим; stop → start держит пин на мёртвый миньон
   (`rejected required storage's minion: not running`, state NEW). Разблокировалось
   переносом/дропом диска инстанса (владелец, через облако) → поднялся на `srvr22621`.
3. CC: после возврата метрик покрытие 131/131 (100%), `isProposalReady: true`,
   все 8 целей ready. Полный прогрев 5×5мин, но ready наступает раньше.

## Урок

**CC цели внезапно notReady на живом кластере — проверь, не разделили ли сломанные
брокер и cruise один миньон.** Смерть миньона бьёт обоим сразу, и два симптома
(нет CPU-метрик + cruise не стартует) имеют один корень. Цепочка «метрики течёт,
но coverage 42%» без CRASH-loop брокера = тихий сломанный контейнер, не конфиг.
