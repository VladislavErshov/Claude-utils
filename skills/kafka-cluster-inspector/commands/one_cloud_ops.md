# Оператор one-cloud-ops: операции вне Temporal

Некоторые операции с Kafka-кластерами выполняет **оператор one-cloud-ops**, а не
mdb-processing/Temporal. По таким operationId в Temporal будет ПУСТО — это не значит,
что операция потерялась. Закрытие самой операции в прод-БД — скилл `jira-mdbsup-solver`
/ `db-worker`.

Типовой пример — `get_kafka_downscale_broker_result` (задача `kafka.downscale-broker`
внутри оператора). Признак из `error_message` операции в прод-БД:
«Operator task <fullQueue> in progress [object Object]» — это `GetResultOperatorTaskProcessor`
(mdb-backend): DONE он ставит только когда задача исчезает из `operators.kafka.tasks`
в статусе оператора.

Код оператора: `~/Documents/Git/one-cloud-ops/one-cloud-ops-server/src/main/java/one/cloud/ops/impl/kafka/`
(`tasks/DownscaleKafkaBrokerTask.java`, `DownscaleMdbReplicasTask.java`, `KafkaClusterInfo.java`).

## Что делает downscale-broker (--cloud=X --replicas=0, последний брокер роли в ДЦ)

`DownscaleKafkaBrokerTask` + `DownscaleMdbReplicasTask`:

1. `resolveBrokerId(host)` — AdminClient describeCluster по хосту.
2. `decommissionBrokerById` — alterPartitionReassignments round-robin (без дублей брокера;
   при 1 брокере на ДЦ = автоматически 1 реплика на ДЦ), с валидацией RF.
3. Цикл `isBrokerDrained` (партиций на брокере нет).
4. `unregisterBroker(brokerId)` — Admin API.
5. Withdraw service + withdraw storage в облаке (последний брокер = `isWithdrawing: true`).

## Диагностика оператора

```bash
mcc --local -n infra -c <dc> ops "queue://<fullQueue>" -f json | jq '.[0] | {alerts, tasks: .operators.kafka.tasks}'
```

- `alerts["kafka.watch[refreshAvailabilityState]"]` — состояние по **модели оператора**.
  «No one primary» при живом кластере = протухшая модель (ghost-хосты удалённых нод —
  MDBSUP-4895; застрявший alert без ghost-хостов — MDBSUP-4899). Сверять с реальностью
  через Jolokia:
  `curl -s http://localhost:7777/jolokia/read/kafka.server:type=raft-metrics/current-state`.
- `tasks.downscale-broker.state` — прогресс (`isWithdrawing`, `serviceWithdrawn`,
  `storageWithdrawn`).
- `invoked` у задачи — последний экшен оператора (например `KafkaAdminAction` на хосте).

Типовое зависание: `precondition() is false` / `isReadyForActions() false` («No one
primary») — задача стоит вечно, операция в БД failed с attempts_left=0.

## Ручное выполнение downscale-broker (если precondition висит вечно)

1. Убедиться, что нет RUNNING-ретрая операции (Temporal по operationId пуст, attempts_left=0).
2. **Сначала** остановить задачу оператора, чтобы не гонялась с ручными действиями:
   `mcc op_stop "queue://<fullQueue>" kafka.downscale-broker`.
   (В MDBSUP-4895 останавливали в конце — задача зациклилась на уже выведенном сервисе;
   в MDBSUP-4899 остановили первой — проще.)
3. BrokerId: `kafka-broker-api-versions.sh` (id в скобках, rack = ДЦ).
4. **Reassignment партиций — скилл `kafka-reassign-partitions`** (генерация reassign.json
   под схему размещения, `--execute/--verify`, throttle, base64-загрузка json на хост
   чанками, сохранение cross-DC redundancy 1 реплика/ДЦ). Применять его всегда, когда
   нужно **переместить партиции или изменить их replicas/RF** (вывод брокера, ручная
   балансировка, снижение RF) — вместо самописных Java-скриптов. Сначала проверить, не
   перелил ли оператор уже сам (describe топиков: партиций на брокере может быть 0).
   После reassign проверять чекером DUP_RACK=0 (RackCheck.java из MDBSUP-4895).
5. Unregister брокера после drain — стандартно
   [administration.md](administration.md) §Unregister (`kafka-cluster.sh unregister`).
   Альтернатива — single-file Java: `java -cp "/opt/kafka/libs/*" /tmp/Unregister.java
   <bootstrap:9092> <brokerId>` с `/opt/kafka/config/client.properties` (юзер `super`;
   скрипты-образцы в [history/MDBSUP-4895](../history/MDBSUP-4895-2026-08-26.md)).
6. Withdraw в облаке (последний брокер роли в ДЦ): `mcc stop <service>` → до FINISHED →
   `mcc withdraw --type service <service>` → подождать исчезновения инстанса (иначе
   `Cannot withdraw storage used by services`) → `mcc withdraw --type storage
   "<queue>/<role>"` (уравнение — pexpect, см. mcc-host-worker/commands/lifecycle.md).
7. Верификация: downscale-задача TASK_ABSENT в `mcc ops`; Jolokia raft-metrics — лидер на месте.
8. Операцию в прод-БД закрыть: UPDATE → done (шаблоны — скилл `db-worker` /
   `jira-mdbsup-solver`).

⚠️ Перед stop/withdraw проверить в прод-БД, что нет RUNNING-операций по кластеру
(иначе заблокируются «Already has unapplied operation»).

## Грабли (из MDBSUP-4895/4899)

Общие mcc-грабли (молчаливый scp, `414 URI Too Long`, Tcl/expect-эскейпы, уравнение
withdraw через pexpect) — канон в [`mcc-host-worker`](../../mcc-host-worker/SKILL.md)
(`commands/pitfalls.md`, `commands/scp.md` — там же генератор base64-чанковой заливки,
`commands/lifecycle.md` — withdraw). Специфика этого флоу:

- `java Foo.java` (source-file mode) с kafka-классами: classpath обязателен —
  `java -cp "/opt/kafka/libs/*" Foo.java`.
- `kafka-topics.sh --describe` БЕЗ `--topic` даёт только сводку; партиции — по каждому
  топику отдельно (`--describe --topic <t>` и `</dev/null` внутри while read).

## Разборы

- [history/MDBSUP-4895](../history/MDBSUP-4895-2026-08-26.md) — два кластера mail,
  ghost-хосты в модели оператора, полный ручной флоу + скрипты Decommission/Unregister/RackCheck.
- [history/MDBSUP-4899](../history/MDBSUP-4899-2026-08-27.md) — оператор ничего не успел
  сделать; op_stop первым; reassign не потребовался (уже перелито); unregister + withdraw + SQL.
- [history/MDBSUP-5051](../history/MDBSUP-5051-2026-09-03-upscale-broker-replicas-op-restart-false-success.md) —
  upscale-аналог `kafka.upscale-broker-replicas`: op_stop + отмена add_hosts (canceled) +
  удаление строки-призрака из host_state; ловушка «ложный успех get_result» после
  рестарта оператора (все tasks done ≠ операция done).
- [history/MDBSUP-5092](../history/MDBSUP-5092-2026-09-03-downscale-broker-isfresh-precondition.md) —
  precondition false из-за `isFresh() false` (фейлы refresh user/topic/group state,
  не «No one primary»); попытки операции перезаряжались на уровне операции; **операция
  сошлась сама после op_stop + withdraw** (get_result увидел TASK_ABSENT → done →
  update_db_connection_url + finish_task) — SQL не понадобился. Брокеры в 4 ДЦ — stop
  только инстанса, не сервиса; BROKER-листенер PLAINTEXT (client.properties с SASL
  ломает AdminClient).
