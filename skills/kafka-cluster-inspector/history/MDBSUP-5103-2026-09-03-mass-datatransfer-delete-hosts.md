# MDBSUP-5103 — 4 кластера datatransfer: delete_hosts hc-брокера, get_kafka_downscale_broker_result in progress (03.09.2026)

Контекст: массовая миграция ДЦ datatransfer. Тип 1 тикета — 4 Kafka-кластера, у каждого
delete_hosts последнего hc-брокера (`--cloud=hc --replicas=0`) падает в failed на
`get_kafka_downscale_broker_result`: «Operator task <fullQueue> in progress [object Object]»
(`GetResultOperatorTaskProcessor`). Temporal по всем operationId ПУСТ (operator-флоу).
Общее у всех: attempts_left=0, in_processing=false, host_state без hc-хоста (модифицируется
ещё на шаге modify_kafka_hosts), URP=0, партиции перелиты или переливаются.
База — [one_cloud_ops.md](../commands/one_cloud_ops.md) и разборы 4895/4899/5092.

## Четыре подтипа одного зависания (по одному на кластер)

| Кластер | Оп | Состояние оператора | Фактическое состояние кластера | Лечение |
|---|---|---|---|---|
| channel-info `c765ffd7` | `6ec6fde4` | Задача живая, «Waiting for broker reassignment check» | Перелив hc→uc идёт (7 партиций ~70 ГБ, ~490 ГБ, ~8.6 МБ/с, ETA ~16ч), URP=0, 20001 в ISR | op_stop → ждать drain → unregister → withdraw → SQL |
| dzen-comments2 `d7322fa3` | `a5001eb4` | Задача живая, ведёт перелив сама | То же, задача сама доведёт unregister+withdraw | Ничего не делать, потом проверить + SQL |
| pub-shard-test `568d8f8c` | `5237b6f4` | Задача живая, precondition false: `isBrokerDrained false` + `allBrokersAreAvailable false` — оператор **остановил hc-брокер** (ServicesSupport.stop 31.08) и завис на проверке drain по мёртвому брокеру | Drain завершён (URP=0, 20001 «no longer registered»), инстанс STOPPED, withdraw НЕ сделан | op_stop → withdraw service+storage → SQL |
| target-3-dwh `42fd0100` | `dadac82f` | Задачи в операторе НЕТ (оператор довёл всё: перелив, unregister, withdraw) | Облако чисто, 20001 не зарегистрирован, URP=0 | Только SQL done |

⚠️ Из 4895: «isBrokerDrained false» при остановленном брокере — артефакт проверки по
недоступному брокеру, НЕ признак недолитых партиций. Проверять факт по живому брокеру:
URP + api-versions + log-dirs 20001.

## Методика (повторяемый флоу)

1. БД: `operations` (status/attempts/in_processing) + `tasks` (op_state, invoked — что
   последнее делал оператор) + `operation_model->'host'->>'dc'`.
2. Temporal по operationId — пуст ⇒ operator-флоу, ретраев нет (правки безопасны).
3. `mcc -c hc ops "queue://<fullQueue>" -f json` — есть ли `tasks.downscale-broker`, alert.
4. Живой брокер (kc): `kafka-broker-api-versions.sh --bootstrap-server $(hostname -f):9092
   --command-config /opt/kafka/config/client.properties` (см. грабли SASL_SSL ниже) →
   состав брокеров; `kafka-log-dirs.sh --describe --broker-list 20001` → что осталось на hc;
   `--describe --under-replicated-partitions` → URP.
5. Если перелив идёт — НЕ вмешиваться: reassignment уже выставлен оператором
   (в describe видно `Adding Replicas: 23001, Removing Replicas: 20001`), доводится
   Kafka сам, независимо от op_stop. Скорость: Jolokia на целевом брокере
   `ReplicationBytesInPerSec` (порт 7777).
6. Drain завершён: op_stop (если задача жива) → `kafka-cluster.sh unregister --id 20001`
   (может уже быть снят) → в облаке: сервис hc в FINISHED/STOPPED ⇒
   `withdraw --type service` → дождаться EntityNotFoundException на сервис →
   `withdraw --type storage "<fullQueue>/broker"` (уравнение через pexpect, канон
   mcc-host-worker/commands/lifecycle.md) → PURGEABLE.
7. SQL: `UPDATE operations SET status='done', in_processing=false, finished_ts=now(),
   error_message=NULL WHERE id='...' AND status='failed'` (docker cp + psql -f,
   BEGIN/COMMIT).

## Новые грабли этого кейса

- **SASL_SSL + hostname verification**: листенер INTERNAL SASL_SSL, advertised — FQDN.
  `--bootstrap-server localhost:9092` даёт «Bootstrap broker disconnected» (hostname
  check на «localhost» против сертификата FQDN); PLAINTEXT без конфига — TimeoutException
  fetchMetadata. Рабочий вариант: `--bootstrap-server $(hostname -f):9092 --command-config
  /opt/kafka/config/client.properties`. (В 5092 листенер был PLAINTEXT — наоборот.)
  Настоящий конфиг брокера — `/opt/kafka/config/broker.properties`, а не server.properties
  (там дефолтный шаблон).
- **kafka-log-dirs JSON в одну строку**: `grep -c` считает строки (=1); партиции считать
  `grep -oE '"partition":"[^"]+"' | wc -l`. awk-вариант по `$4` occasionally ловит ключ
  `broker-list` — фильтровать по grep, не awk.
- **Порядок withdraw важен**: `withdraw --type storage` без `withdraw --type service`
  даёт `Cannot withdraw storage used by services ... replicas: 1` даже при
  EntityNotFoundException на инстансе по FQDN — инстанс STOPPED/исчез, но сервис ещё
  числится. Сервис проверять по имени роли `broker.<cluster>...`, не по FQDN инстанса.
- **Оператор может довести всё сам**: target-3-dwh — задача исчезла из `operators.kafka.tasks`
  после полного завершения (перелив+unregister+withdraw), но операция осталась failed,
  т.к. get_result уже не перезаряжался (in_processing=false). Закрытие — только SQL.
- **Скрипты на хост через base64-чанки** (200 симв. на send) — Tcl-эскейпы `$` в inline
  expect непобедимы; рабочий генератор: локальный script.sh → base64 → .exp с
  `printf %s <chunk> >> /tmp/b64.txt` (см. /tmp/opencode/push_and_run.sh в сессии).

## Открытые хвосты

- channel-info и dzen-comments2: дозакрыть после перелива (unregister/withdraw проверять
  по месту — у dzen оператор делает сам).
- Скорость перелива ~8.6 МБ/с без throttle-конфигов (ни на топике, ни на брокерах) —
  упор в сеть/диск hc→uc; причина не копалась.
- Тикет: Тип 2 (add_hosts pc, 2 кластера) и Тип 3 (modify 85c02238) — не разобраны.
