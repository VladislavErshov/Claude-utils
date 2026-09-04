# MDBSUP-5098 — prod-vk-support-kafka: ручной downscale hc-контроллера (PLAINTEXT vs SASL-check, повтор 5056)

**Дата:** 2026-09-03
**Кластер:** `prod-vk-support-kafka` (vk-support, ns infra; `adc0b417-759a-44f6-9325-1522aa0d5ca3`)
**Тикет:** MDBSUP-5098. Операция delete_hosts `c4655794-e578-4854-8eed-50d911034810`
(удаление hc-контроллера, Temporal `downscaleKafkaControllerInCluster`), Kafka **3.8.0**.
Хосты на момент работ: брокеры ec/kc/pc (21001/22001/23001), контроллеры
hc=10001/kc=11001/pc=12001/ec=13001, cruise pc. hc-брокер удалён успешной операцией
`8c3fad86` в 12:11 того же дня.

## Корень — детерминированный, тот же, что в MDBSUP-5056

Брокерский листенер **PLAINTEXT** (`listeners=BROKER://:9092`,
`listener.security.protocol.map=...BROKER:PLAINTEXT...`), а mdb-processing делает
connection-check только `[SASL_SSL, SASL_PLAINTEXT]` → `kafka_host_getLeaderId` падает
всегда. Это **третий** кейс на семействе vk-support: create_database 25.08 (canceled),
MDBSUP-5056 (stage-vk-support-kafka), этот тикет. Ошибка байт-в-байт совпадает с 5056.
Продуктовый вопрос открыт: PLAINTEXT-кластеры несовместимы с connection-check processing.

## Отличие от 5056 в момент разбора

В PMS `kafka.controller.quorum` уже был 3-voter (removeControllerFromQuorum успел),
`kafka.layout` не трогали (`hc,kc,pc,ec` — стабильность node.id, паттерн 5056/4970).
**Но на всех 6 хостах конфиги оставались 4-voter** — рендер не дошёл (в 5056 рендер уже
прошёл). Т.е. «removeControllerFromQuorum успешно» в тикете ≠ конфиги на хостах обновлены.

## Ручное доведение (03.09, последовательность)

1. `confp --oneshot` на контроллерах и брокерах kc/pc/ec → voters 3-voter
   (13001,11001,12001), `10001@` нигде не остался, node.id не сместились.
2. Рестарт контроллеров **kc → ec** (лидер pc=12001 последним) — после каждого
   проверка `kafka-metadata-quorum.sh --bootstrap-server localhost:9092 describe --status`
   на брокере: leader 12001 держался, voters полные.
3. **`systemctl stop kafka-controller` на hc перед рестартом лидера** — модификация
   канона 5056 ради устранения их грабли: hc не голосует → паразитные выборы невозможны.
   На время рестарта лидера кворум 2/3 от 3-voter набора — достаточно.
4. Рестарт pc → выборы, лидером стал **kc (11001)**, voters `[11001,12001,13001]` —
   hc вне кворума, без единого сбоя.
5. Withdraw hc: `mcc --local -n infra -c hc stop controller.prod-vk-support-kafka`
   → FINISHED → `withdraw controller.prod-vk-support-kafka` (сервис исчез из one-cloud)
   → `withdraw --type storage "prod-vk-support-kafka.vk-support.db.production.mdb.prod.hc/controller"`
   (pexpect-уравнение) → hc-иерархия PURGEABLE, kc/pc/ec остались MOUNTED (сверено по всем ДЦ).
6. `systemctl restart rscheck@kafka` на 6 живых хостах.
7. SQL (прод): DELETE hc-контроллера из `host_state` (7 хостов осталось);
   операция → `done/in_processing=false/error=NULL`;
   **`db_cluster_version` (id 237903, последняя строка): `controllerDcs` `["hc","kc","pc","ec"]`
   → `["kc","pc","ec"]`** — в проде записаны ДЦ контроллер-хостов, без правки UI/API считают
   контроллер существующим (в 5056 этот шаг пропустили). Шаблон добавлен в скилл db-worker.

## Итог

Кластер: 3 брокера + 3 контроллера (kc/pc/ec) + cruise(pc); лидер 11001, voters
[11001,12001,13001], observers [21001,22001,23001], LogEndOffset одинаковый, лаг 0,
unavailable-партиций нет; операция done. `kafka.layout` осознанно оставлен `hc,kc,pc,ec`.

## Грабли

- `kafka-metadata-quorum.sh` в 3.8: `--bootstrap-server` идёт **до** подкоманды
  (`... quorum.sh --bootstrap-server ... describe --status`), иначе `unrecognized arguments`.
- Через `mcc sshexec` java-утилиты падают `OCI runtime error` — работать через
  `mcc --local ssh` + expect-обёртку.
- INFO-дамп AdminClientConfig в stdout у quorum-утилит — фильтровать `grep -v INFO`.
- Tcl-грабля: `[0-9]` в send-строке = command substitution → `invalid command name "0-9"`.
