# MDBDEV-3412 — wan-миграция хостов Kafka (cruise + hc), 2026-09-11

## Задача
Статусы/роли/утилизация cruise и hc хостов не отображались на вкладке hosts wan-кластеров.

## Первопричины
1. Cruise-инстансам wan-кластеров в облаке не выдана wan-сеть (манифест `network: lan`) → нет `host_wan`/`cloud_hostname_wan`.
2. host_checker выбирает имя через `get_host_name(is_wan_cluster)`: wan → env `cloud_hostname_wan`, иначе HOSTNAME; пустой env → пустое имя в репорте → mdb-health Redis без записи → UI UNKNOWN.
3. Utilization шлёт one-cloud-ops (`UtilizationCalculator`) по `taskInfo.host_wan`, пустой → хост скипается (потому у cruise не было vCPU/RAM/Lan; диск — из rscheck, был всегда).
4. PMS-override'ы `kafka.isWanCluster=false` (per-DC `dzen-access-logs-zinfra-kafka.hc`, сервисные `cruise.*`/`cruise-control.*.clouds`) перекрывали кластерный true.
5. Протухшие ini на хостах: PMS true, а confp не перерендеривал (на ряде хостов confp не достаёт conf-серверы).

## Что сделано
- Cloud (mcc --cloud <dc> -n dzen): манифесты 12 cruise-сервисов → `network: wlan + wan`. Грабли: wlan/lan взаимоисключающи; health-API `health.mdb.one-infra.ru` (fd00:b4c2::) достижим только через wlan; в ports `lan` — generic, остаётся. У ucp очередь `.mdb.batch`.
- PMS (update.do/delete.do, ns=dzen): 2 override → true, 1 (dzen-access-logs-zinfra-kafka.hc) удалён.
- Хосты: sed `is_wan_cluster false→true` в `/etc/host_checker/host_checker_config.ini` (бэкапы .bak) на 16 хостах; host-check (oneshot каждые 5с) подхватывает без рестарта.
- БД prod backstage_plugin_mdb.host_state: переименование lan→wan FQDN (events hc 13, 12 cruise, kafka-news 7.broker.pc, dzen-access-logs 2 hc). Итог: 0 хостов без .wan. во всех 21 wan кластерах.
- dzen-access-logs: изначально ждали wan-алиасы VM в админке облака; после перезапуска VM с новыми сетями confp отрендерил сам, sed не понадобился.

## Код (не закоммичено в рамках тикета, частично закоммичено)
- mdb-processing: `kafka-cruise-control-manifest.template` `${NETWORK}`; `CruiseManifest.isWan`; `KafkaCruiseManifestActivityImpl` (wlan+wan | lan); `toManifest` прокидывает isWan; тесты + docs/kafka/create-cruise-control.md. NullAway-сапрессы в тесте.
- mdb-data: `KafkaHostsInternalServiceImpl` — isWan из `kafkaClusterVersionService.getKafkaClusterParams().getClusterParams().getIsWan()` для saveCreatedKafkaCruise/saveBrokerHostStates (было one_cloud_meta/захардкожено).

## Грабли
- mcc: инстансы ищутся по hierarchy (lan-имена), wan-FQDN не резолвится; сабмит требует ответ на арифметику (парсить stdin); при частых вызовах — таймауты, сабмит может зависнуть молча.
- Внешний `tail -1` на вывод mcc ловит «Connection closed» — фильтровать по `^is_wan_cluster`.
- confp вручную (`confp`, `--oneshot`) на хостах часто падает «None of conf servers is available».
- mdb-health статусы хостов — только в Redis (в PG health их нет); dzen-access-logs 1.broker исторически шлёт в mdb.kaizen.idzn.ru.
