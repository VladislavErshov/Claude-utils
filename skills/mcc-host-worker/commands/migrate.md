# `mcc migrate` — перенос storage/shard на другой миньон

Ломаный диск инстанса (`df: /mnt/data: Input/output error` после рестарта инстанса),
битый контейнер/cgroup — `mcc migrate --relocate` переносит тома на другой миньон.

## Главная грабля: имя таргета

`mcc migrate` принимает **storage/shard/minion**, но НЕ FQDN инстанса (по FQDN —
`EntityNotFoundException`). Для mdb-data кластеров имя storage строится из **полного
имени очереди** (pod/one-cloud иерархия) + `/controller`:

```
<queue>.<project>.db.production.mdb.prod/controller
```

Примеры (реальные кейсы):
```
adb-users-datatransfer-kafka.datatransfer.db.production.mdb.prod/controller
do-12738-datatransfer-kafka.datatransfer.db.production.mdb.prod/controller
kafka-1-mdbsandbox-kafka.mdbsandbox.db.production.mdb.prod
```

Как узнать полное имя очереди: `mcc --local -n <ns> status controller.<queue>` —
в строке `name:` в скобках лежит полный путь очереди:
```
name: controller.adb-users-datatransfer-kafka (controller.adb-users-datatransfer-kafka.datatransfer.db.production.mdb.prod)
```

## Команда (неинтерактивно)

```bash
mcc --local -n infra migrate --relocate --auto_solve "<queue>.<project>.db.production.mdb.prod/controller"
```

- `--relocate` — форсировать перенос томов на ДРУГОЙ миньон.
- `--auto_solve` — mcc перед выполнением опасного действия просит решить арифметическое
  уравнение (`8-3`, `6*1`); в неинтерактивном режиме stdin кончается → `ERROR: EOF`.
  Флаг решает автоматически.
- Успех: `message: 'MIGRATE of storage(s) <name> total [1/1] shard(s) requested'` —
  миграция запрошена, выполняется асинхронно.
- Проверка валидности имени до запуска: mcc отвечает `<name>/1 is MOUNTED` и только потом
  просит уравнение.

## Грабли

- **По FQDN не ищет**: `mcc migrate ... 1.controller.<queue>.<dc>.one-infra.ru` →
  `No storage, shard, minion or device matching ... is found`.
- **`mcc instances <pattern>` — неполный индекс**: часть mdb-data инстансов не находится
  (EntityNotFound), хотя `sshexec`/`restart`/`start` по тому же FQDN работают — это
  разные API. Наличие хоста проверяй sshexec'ом, а не instances.
- **`mcc minions <pattern>` / `minion_storage <minion>`** для миньонов mdb-data инстансов
  могут отдавать EntityNotFound — реестр mcc видит не все миньоны.
- **`mcc status controller.<queue>`** показывает только инстансы mcc-сервиса (replicas),
  а не все хосты из host_state mdb-data — составы mcc ↔ mdb-data расходились
  (кейс kafka-1-mdbsandbox: 2 реплики в mcc, 3 контроллера в mdb-data).
- Медленный master: `dial tcp ...:443: i/o timeout, Attempt 1` перед ретраем — норма,
  команда доработает сама.
- OOM-цикл контейнера миграцию не лечит (кейс kafka-1-mdbsandbox: `Container 'main' is
  dead: Out of Memory` при MEM=2G в манифесте) — тогда править манифест (mcc edit/submit),
  владельцу.

## Полезное рядом

- `mcc instances <pattern>` — hierarchy/service/queue/outcome по инстансам (когда индекс
  их видит): там виден `outcome_text: Container 'main' is dead: Out of Memory. Stopped`.
- `mcc restart <fqdn>` / `mcc stop` / `mcc start` — работают по FQDN (mdb one-cloud API),
  рестарт инстанса чинит не всё: I/O error диска переживает рестарт инстанса и хоста —
  тогда только migrate/пересоздание.
- Проверка после миграции: `sshexec <fqdn>` по FQDN снова отвечает + `df -h /mnt/data` без
  I/O error + `systemctl is-active <service>`.

## Связанный кейс: старт остановленных mdb-инстансов (класс voters_dead)

Остановленный в облаке контроллер (mdb-health `voters_dead`, `systemctl` на хосте:
«Task Instance is not scheduling on a minion») поднимается так:
```bash
mcc --local -n infra start <fqdn>          # запрос старта инстанса
# ждать загрузку 2-3 мин, затем внутри:
systemctl start kafka-controller && systemctl restart rscheck@kafka
# проверить роль: curl localhost:7777/jolokia/read/kafka.server:type=raft-metrics
```
- Часть стартует сразу и входит в кворум фоловером; часть уходит в автостарт-очередь
  с ошибкой «cannot start by either reason. Once resolved, it will start automatically» —
  ретраить mcc start через минуты; если не стартует долго — планировщик/ёмкость ДЦ,
  дальше UI/one-cloud-ops.
- Кворум из 3 при одном живом voter'е не собирается: поднять ВТОРОГО члена (старт
  инстанса/add_hosts), выборы пройдут сами, лидером станет тот, у кого свежее лог.
- Если у кандидата разошёлся metadata log (вечный candidate при живом лидере) — вайп
  `/mnt/data/log` + `/mnt/data/metadata` + старт (только НЕ на лидере).
