# events-front-kafka — проверка PMS контроллера uc (контроллер не стартует)

**Дата**: 2026-08-27
**Кластер**: `events-front-kafka` (dzen, `*.idzn.ru`), PMS-ключи `events-front-kafka.clouds` + `controller.events-front-kafka.clouds` (ns=dzen, app=mdb)
**Симптом**: `1.controller.events-front-kafka.uc.idzn.ru` — kafka-controller inactive, логов/journal нет, `/mnt/data/{log,metadata}` отсутствуют, `/opt/kafka/config/` — сток образа 2024. Контейнер пересоздан в день инцидента (log-директория 15:31).

## PMS snapshot

| Переменная | Значение | Вердикт |
|---|---|---|
| `kafka.layout` | `dc,pc,rc,hc,uc` | — |
| `kafka.controller.quorum` | `11001@pc, 10001@dc, 13001@hc, 14001@uc` (все :9093) | ✓ согласован с layout |
| `kafka.controller.properties` | шаблон node.id=`10000+dc_id*1000+instance_id`; SASL_PLAINTEXT; log.dirs=/mnt/data/log; metadata.log.dir=/mnt/data/metadata | ✓ |
| `kafka.users` (jaas) | `{% import utils.j2 %}` присутствует | ✓ |
| `kafka.sysconfig` (controller-ключ) | heap 4g, jaas.conf, JMX 8080, jolokia 7777 | ✓ |
| `kafka.ssl.enabled` | false | — |
| `zen.kafka.vaultRoot` | `zkv/mdb/front/kafka/events-front-kafka.front.db.production.mdb.prod` | ✓ dzen-путь |

node.id-матрица: dc→10001, pc→11001, rc→12001 (нет контроллера — ок), hc→13001, uc→14001 — все voter'ы совпадают. Паттерн I48592 (layout≠quorum) отсутствует.

## Итог (продолжение — root cause найден и починен)

**Root cause**: у контейнера uc при пересоздании подтянулся новый образ, в котором
`/etc/host_checker/host_checker_config.ini.j2` (ресурс №7 в kafka.yml, сразу после sysconfig)
рендерит `is_wan_cluster = {{ pms('kafka.isWanCluster') }}` **без дефолта**. У кластера в dzen
значение лежало только на per-DC ключах (`events-front-kafka.dc=true`, `.pc=true`, `.hc=false`
— override из ранбука «Переезд кластеров дзена из rc в hc», events — эталонный кластер), а
ключа для uc не было → confp-init exit 1 → OnFailure-юнит гасит контейнер через 3с → инфра
ждёт порт 9093 900с → FAILURE → бесконечный retry-loop.

**Фикс**: записал `kafka.isWanCluster=true` на кластерный ключ `events-front-kafka.clouds`
(dzen, app=mdb) — ровно так же пишет mdb-data (GenerateKafkaPmsSettingsTaskProcessor:123 →
`String(metaParams.isWan)`, queueNameForClouds; в one_cloud_meta кластера isWan=true).
Per-DC override hc=false продолжает выигрывать. Следующая попытка старта: confp-init OK,
`Kafka Server started` nodeId=14001, is_wan_cluster=true в host_checker.

**Хвост**: 14001 вошёл в кворум как Observer (lag 0), voter'ы остались 11001/10001/13001 —
на pc (лидер) и dc отрендерен старый `controller.quorum.voters` без 14001 (файлы от 10:29/10:33).
Лечится перезапуском операции (правка кворума на остальных контроллерах) — делает пользователь.

## Верификация PMS-значений по values.do (весь dzen, один запрос)

`values.do?property=kafka.isWanCluster` возвращает мапу ВСЕХ ключей — видно per-DC override'ы
и кластерные ключи одним запросом, без перебора хостов.

## Заметки

- CC `bootstrap.servers` в `kafka.cruisecontrol.properties` перечисляет только брокеров dc+hc
  (по 12 шт) — pc/uc это controller-only ДЦ.
- `mcc status -n dzen <FQDN>` → EntityNotFoundException; для state использовать
  `mcc instances -n dzen -c <dc> <FQDN> -f yaml` (state/outcome/outcome_text/progress).
- `mcc logs <host> <stream>` — единственный способ увидеть логи crash-loopящегося контейнера
  (ssh работает только в моменты живого контейнера). Потоки: @console, confp.log, systemd.log,
  bash.log, rsyslogd.log.
- macOS: `timeout` нет — фонить mcc logs, sleep, pkill, читать файл.
- pms-read.sh конвертирует FQDN в кластерный ключ и НЕ видит per-DC ключи — для per-DC
  значений дёргать values.do напрямую и смотреть всю мапу.

## 2026-08-28 — CC crash-loop: per-DC override kafka.broker.properties без cruise-блока

**Симптом**: `1.cruise.events-front-kafka.pc.idzn.ru` availability UNAVAILABLE ("He's dead,
Jim"), cruise-control.service crash-loop (exit 1 каждые ~70с):
`IllegalStateException: Cruise Control cannot find the metrics reporter topic
[__CruiseControlMetrics] in the Kafka cluster.`

**Root cause**: при включении CC mdb-processing записал cruise-блок (metric.reporters +
cruise.control.metrics.*) в `kafka.broker.properties` кластерного ключа
`events-front-kafka.clouds`. Но per-DC override'ы `events-front-kafka.dc`/`.hc` (наследие
ранбука rc→hc) ПЕРЕКРЫВАЮТ кластерный ключ — в них блока не было → confp рендерит без
reporter'а → топик не создаётся → CC умирает на CruiseControlMetricsReporterSampler.configure.
confp при этом пишет "does not need updating" (значение per-DC не менялось).

**Фикс** (2 записи POST /api/conf/update.do, ns dzen, app mdb): в per-DC значения
`kafka.broker.properties` добавлены (1) первой строкой `{% import "/etc/misc/utils.j2" as utils -%}`
(нужен для vault() в jaas-строке — в per-DC версиях import отсутствовал), (2) в конец cruise-блок
дословно из `.clouds`. Затем rolling-рестарт 24 брокеров (dc+hc по 12): confp --oneshot →
проверка metric.reporters → systemctl restart kafka-broker → после первого же брокера топик
создался (auto.create=true, 9 парт, RF=3), CC стартовал самостоятельно.

**Грабли**:
- `mcc sshexec` вывод маскирует ТОЛЬКО визуально — `password='kY3Kokn6HI4W4ueKsmdRfuddRfuddrNgyGq'`
  в рендере это реальный vault-пароль (лежит и в jaas.conf), не маркер маски.
- Верификация рестартов по grep "Successfully registered broker" в kafka-broker.out.log
  ломается logrotate'ом (ротация по расписанию обнуляет файл) — «0 регистраций» = ложная
  тревога; проверять systemctl ActiveEnterTimestamp + journalctl.
- macOS: нет setsid — фоновые скрипты через `nohup ... & disown` с быстрым возвратом
  tool-call (sleep в том же вызове убивает процесс-группу по таймауту).
- Параллельный rolling: рестарт-команды каждые ~10с из фоновых subshell (mcc-коннект ~40с
  доминирует, но рестарты ложатся с нужным шагом); верификация отдельным проходом в конце.
