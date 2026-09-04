# ecom-dynamic-adtech-kafka: dc-контроллер в crash-loop — dc отсутствовал в kafka.layout + дубль 13001 в quorum

Дата: 2026-08-25. Кластер: `ecom-dynamic-adtech-kafka` (5 контроллеров: hc/kc/pc/uc/dc; лидер после фикса hc).

## Симптом

`1.controller.ecom-dynamic-adtech-kafka.dc.one-infra.ru` — статус unknown, mcc instances:
`state: FINISHED, outcome: ATTEMPTS_LIMIT, Container 'main' is dead: Exit code = 1`.
Остальные 4 контроллера AVAILABLE.

## Корень (вариация I48592)

- `kafka.layout` = `hc,kc,pc,uc` — **dc отсутствовал** → confp не мог отрендерить node.id для dc.
- `kafka.controller.quorum` содержал `13001@dc` **и** `13001@uc` — дубль id.
- node.id по формуле `10000 + index*1000 + 1`: hc=10001, kc=11001, pc=12001, uc=13001, dc должен быть 14001 (позиция 4).

## Фикс

1. PMS update.do (ключ `ecom-dynamic-adtech-kafka.clouds`, ns infra):
   - `kafka.layout` → `hc,kc,pc,uc,dc`
   - `kafka.controller.quorum` → dc как `14001@1.controller...dc...:9093`, uc остался 13001.
2. `mcc --local -n infra start 1.controller...dc...` — инстанс из FINISHED поднялся, confp отрендерил node.id=14001, метаданные свежие (вайп не понадобился).
3. Rolling restart контроллеров (confp --oneshot + systemctl restart kafka-controller):
   followers первыми (kc → pc → hc), лидер uc последним. Между шагами — `kafka-metadata-quorum describe --replication` через брокера.

## Результат

Кворум 5/5: hc=10001 Leader, dc=14001/kc/pc/uc — Followers, lag 0. dc availability после старта RESERVED — чекеры переводят в AVAILABLE с задержкой. В логе dc: `Kafka Server started`, переходы Candidate→Follower без ошибок.

## Грабли

- `kafka-metadata-quorum.sh` с bootstrap на **controller:9093** падает `UnsupportedVersionException: The node does not support METADATA` — нужно bootstrap через **broker:9092**.
- До рестарта лидера dc(14001) висит **Observer** в describe --replication — норма, статические voters у лидера ещё старые.
- `ss`/`netstat`: на контроллере нет `ss`, есть `sudo netstat -tln`.
- `mcc sshexec` на FINISHED-инстансе: `Task Instance ... is not scheduling on a minion, please start it first` → `mcc start <host>` поднимает из FINISHED.
