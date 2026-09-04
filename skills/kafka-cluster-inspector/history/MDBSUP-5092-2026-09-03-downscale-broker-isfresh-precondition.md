# MDBSUP-5092 — Kafka prod-vk-support-kafka: downscale-broker, precondition false из-за isFresh=false (03.09.2026)

Кластер `prod-vk-support-kafka` `adc0b417-759a-44f6-9325-1522aa0d5ca3` (vk-support, infra,
fullQueue `prod-vk-support-kafka.vk-support.db.production.mdb.prod`). **4 ДЦ** (ec/hc/kc/pc),
брокеры: hc=**20001**, kc=21001, pc=22001, ec=23001, по контроллеру в каждом ДЦ, лидер — pc,
cruise в pc. Операция `8c3fad86-d7f5-4a33-b188-db8af134636a` delete_hosts (удаление
последнего брокера hc) висла на `get_kafka_downscale_broker_result`. Temporal по operationId
пуст (operator-флоу, как [MDBSUP-4899](MDBSUP-4899-2026-08-27.md)).

## Отличия от 4899/4895

- Precondition false **не** «No one primary», а `operator().isFresh() false`:
  watch не мог обновить user/topic/consumer-group state (47 фейлов подряд),
  `kafka.sync: isClusterInfoRefreshed() false` — с **суток до** операции (02.09 17:10 UTC).
  Причина фейлов refresh не установлена (в логи оператора не попали; корень не копали).
- mdb-data **перезаряжал попытки** на уровне операции (attempts таски 8→14), операция
  сама никогда не ушла бы в failed — «подождать авто-завершения» не вариант.
- Партиции на брокере уже были 0 (drain завершён, оператор делал reassign до стака? —
  state показывал только withdraw-флаги false; reassign не потребовался).
- Операция **сошлась сама после op_stop**: get_result увидел TASK_ABSENT → done →
  выполнелись update_db_connection_url и finish_task → операция done (17:55 UTC).
  **Ручной SQL не понадобился** — отличие от 4895/4899.

## Лечение

1. `mcc op_stop "queue://<fullQueue>" kafka.downscale-broker` (-c hc) — задача исчезла из
   `operators.kafka.tasks`.
2. Reassign — не нужен (проверка: цикл `kafka-topics.sh --describe --topic <t>` по всем
   топикам, grep 20001 = 0; ⚠️ `--describe` БЕЗ `--topic` даёт только сводку).
3. Unregister 20001: `Unregister.java` через java source-file mode
   (`java -cp "/opt/kafka/libs/*" /tmp/Unregister.java localhost:9092 20001`) →
   `UNREGISTERED=20001`; в кластере 3 брокера. API: `admin.unregisterBroker(id)
   .all().get(...)` — у `UnregisterBrokerResult` нет `.get()` (проверять сигнатуру через
   `javap -cp kafka-clients.jar UnregisterBrokerResult`).
4. Withdraw: `mcc -c hc stop 1.broker.<...>.hc` (именно инстанс, брокеры живут в 4 ДЦ!) →
   FINISHED → `withdraw --type service` → дождаться исчезновения инстанса →
   `withdraw --type storage "<queue-без-роли>/broker"` (уравнение через pexpect, описано
   в mcc-host-worker/commands/lifecycle.md).
5. Верификация: 3 брокера, URP=0, лидер pc, задача оператора отсутствует, операция в БД
   `done` сошлась сама.

## Грабли этого кейса

- **У этого кластера BROKER-листенер PLAINTEXT** (`listener.security.protocol.map=...,
  BROKER:PLAINTEXT`): `client.properties` (SASL_PLAINTEXT/PLAIN) даёт «Unexpected
  handshake request with client mechanism PLAIN, enabled mechanisms are []». AdminClient
  для unregister — без command-config, чистый PLAINTEXT. Не повторять мой ложный след
  «сломанного SASL на hc» — конфиг одинаковый на всех 4 брокерах.
- `mcc scp` с macOS молча не залил файл → base64-чанки через expect-ssh (канон 4895).
- pty mcc ssh **искажает длинные send-строки** (вставляет пробелы в токены:
  `localhost:9 092`) — команды только короткими строками через heredoc-скрипт на хосте.
- Tcl expect ломается на подстановке b64-чанков в inline `-c` скрипт (кавычки) — чанки
  передавать через `env()` в файле .exp.
- Чтение логов оператора: `mcc log-streams 3.cdb.cloud-ops-infra.uc.<dc>... --container
  main|vector` (pod мульти-контейнерный; имя контейнера угадывается — `cdb` из манифеста
  deploy НЕ совпадает). `mcc logs` по cloud-ops ноде зависает — не ждать.

## Итог

Кластер: 3 брокера (ec/kc/pc) + 4 контроллера + cruise(pc), DUP_RACK: по реплике на ДЦ,
лидер pc. Операция done сама (после op_stop + withdraw). Тикет закрыт.

Открытые хвосты: причина фейлов refresh состояния оператора (сутки до операции); утренний
массовый «Expected partition ... missing. Creating» на брокерах ~08:56 UTC и рестарт
hc-брокера 10:30 UTC (до старта операции) — не разбирались, к зависанию напрямую не
привели (URP=0). Следить при следующих операциях кластера.
