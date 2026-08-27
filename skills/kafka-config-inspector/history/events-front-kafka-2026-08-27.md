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

## Итог

**Проблем в PMS не найдено.** Блокиратор не в PMS-значениях: на хосте признаки, что после
пересоздания контейнера confp/pre-start ни разу не отрабатывал (нет отрендеренных файлов,
нет journal, нет data-директорий). Копать в сторону host-create/рендер-стадии в mdb-data,
а не в PMS.

## Заметки

- CC `bootstrap.servers` в `kafka.cruisecontrol.properties` перечисляет только брокеров dc+hc
  (по 12 шт) — pc/uc это controller-only ДЦ.
- `mcc status -n dzen 1.controller...` → EntityNotFoundException (хост не находится в dzen-инвентаре mcc) — при разведе учитывать.
