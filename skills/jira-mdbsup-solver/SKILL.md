---
name: jira-mdbsup-solver
description: Разбор тикетов MDBSUP про зависшие/упавшие операции с кластерами MDB (Kafka, Redis и др.). Диагностика по цепочке Jira → прод-БД (db-worker) → прод-Temporal (temporal-worker) → mcc-хосты (mcc-host-worker; Redis — redis-cluster/redis-sentinel-inspector), проверка реального состояния, предложение плана правок и выполнение ТОЛЬКО после явного одобрения пользователя. Используй при запросах вида «разбери MDBSUP-XXXX», «зависшая операция», «найди кластер и операции в темпорал».
allowed-tools: [bash]
---

# jira-mdbsup-solver: разбор тикетов MDBSUP про зависшие операции

Работай строго в три фазы: **Диагностика → План → Исполнение (только после "ок" пользователя)**.

## Фаза 1. Диагностика (только чтение)

1. **Jira** — `generic_jira_get_issue`: из описания достать cluster_id, operationId, тип БД, суть проблемы
   (типовой тикет: «операция failed → блокирует следующую операцию "Already has unapplied operation"»).
2. **Прод-БД** `backstage_plugin_mdb` — вся работа через скилл **`db-worker`**
   (port-forward 53480, psql из docker-контейнера, диагностические SELECT'ы по operations/host_state,
   шаблоны правок, грабли схемы и DML). Креды — в SKILL.md db-worker.
3. **Прод-Temporal** — вся работа через скилл **`temporal-worker`** (поиск workflow по
   operationId / ClusterId, история, failure-цепочки parent → child → activity, retry-политики).
   Кратко: workflowId операции = operationId; упавший child/activity искать рекурсией cause.
   ⚠️ Проверить, нет ли сейчас RUNNING-ретрая той же операции (workflow с тем же workflowId, статус RUNNING).
4. **mcc-хосты** — проверить, что хосты из операции реально существуют и живы:
   ```bash
   mcc -n infra sshexec -n infra <host> "hostname; systemctl is-active kafka-<role>"
   ```
   (namespace см. в таблице namespaces: domain one-infra → infra, idzn → dzen). `mcc status <service>` может
   врать EntityNotFoundException — верить sshexec по хостам.
   Типовая транзиентная причина: `confp --oneshot && systemctl restart ...` →
   `Invalid type of response received: class one.nio.http.Response` (ошибка one-cloud proxy; вручную проходит).

## Фаза 2. План

Сформулировать пользователю компактный план:
- что нашли (операция, workflow, причина падения, состояние хостов);
- какие будут правки (SQL: UPDATE operations → done/in_processing=false/finished_ts=now/error_message=NULL;
  INSERT в host_state недостающих хостов с onecloud_ui_link / grafana_dashboard_link по шаблону соседних
  хостов кластера, params `{"dc": "<dc>"}`);
- риски (RUNNING-ретрай, отсутствие хоста в облаке — тогда NOT NULL-инсерты делать нельзя);
- при RUNNING-ретрае предупредить: можно дождаться авто-завершения или править руками.

**Не выполнять ничего до явного одобрения пользователя.**

## Фаза 3. Исполнение

- DML в прод-БД — по правилам скилла `db-worker` (docker cp + `psql -f`, BEGIN/COMMIT,
  верификационный SELECT после).
- По завершении — обновить `history/` этого скилла (см. ниже) и при необходимости заметку в
  `db-worker/history/MDBSUP-*.md`.

## Работа с Redis-кластерами (mdb-data)

**Диагностика и починка Redis — только через скиллы-инспекторы** (там канон команд, ACL-подключение,
каталог известных проблем и сценарии фиксов):

- **`redis-cluster-inspector`** — шардированные Redis Cluster (слоты, `cluster nodes/myid/meet`,
  два мастера в шарде, `ERR Slot ... is already busy`, забытые/зачищенные ноды, resharding,
  forget ноды, failover, перебалансировка по ДЦ, миграция 7→8).
  Типовой кейс MDBSUP: после сфейлившегося change_primary/failover в шарде 2 мастера
  (expected 1 MASTER, found 2) → слоты/роль чинить по каталогу скилла, затем закрывать операцию в БД.
- **`redis-sentinel-inspector`** — Sentinel-кластеры (cfs-redis): known-peers, спам
  "Failed to resolve hostname", SENTINEL RESET, вечная переливка реплик, битый AOF.

Подключение к ноде: ACL-юзер из `/etc/redis/acl/users.acl` на хосте (юзер `master` с
`masterauth`-паролем из `/etc/redis/redis.conf`; у юзера `default` пароль другой и прав нет).
Хосты — через `mcc sshexec -n infra` (скилл `mcc-host-worker`).

## Оператор one-cloud-ops (флоу, которых нет в Temporal)

Некоторые операции (например `get_kafka_downscale_broker_result`) выполняет **оператор
one-cloud-ops** — в Temporal workflow по operationId будет ПУСТО. Признак из error_message:
«Operator task <fullQueue> in progress [object Object]» — это наш
`GetResultOperatorTaskProcessor` (mdb-backend): DONE он ставит только когда задача
исчезает из `operators.<name>.tasks` в статусе оператора.

Код оператора: `~/Documents/Git/one-cloud-ops/one-cloud-ops-server/src/main/java/one/cloud/ops/impl/<dbtype>/`
(Kafka: `tasks/DownscaleKafkaBrokerTask.java`, `DownscaleMdbReplicasTask.java`, `KafkaClusterInfo.java`).

### Диагностика оператора

```bash
mcc --local -n infra -c <dc> ops "queue://<fullQueue>" -f json | jq '.[0] | {alerts, tasks: .operators.kafka.tasks}'
```
- `alerts["kafka.watch[refreshAvailabilityState]"]` — реальное состояние по модели оператора
  («No one primary» при живом кластере = протухшая модель, ghost-хосты удалённых нод);
  сверять с Jolokia: `curl -s http://localhost:7777/jolokia/read/kafka.server:type=raft-metrics/current-state`.
- `tasks.downscale-broker.state` — прогресс (`isWithdrawing`, `serviceWithdrawn`, `storageWithdrawn`).
- `invoked` у задачи — последний экшен оператора (например `KafkaAdminAction` на хосте).

### Ручное выполнение шагов downscale-broker (если precondition висит вечно)

1. BrokerId: `kafka-broker-api-versions.sh` (id в скобках, rack = ДЦ).
2. **Reassignment партиций — использовать скилл `kafka-reassign-partitions`**
   (генерация reassign.json под схему размещения, kafka-reassign-partitions.sh --execute/--verify,
   throttle, base64-загрузка json на хост чанками, сохранение cross-DC redundancy 1 реплика/ДЦ).
   Применять его всегда, когда нужно **переместить партиции или изменить их replicas/RF**
   в Kafka-кластере (вывод брокера, ручная балансировка, снижение RF) — вместо самописных
   Java-скриптов. Unregister брокера после drain — single-file Java:
   `java -cp "/opt/kafka/libs/*" /tmp/X.java <bootstrap:9092> <brokerId>` с
   `/opt/kafka/config/client.properties` (юзер `super`; скрипты-образцы в history/MDBSUP-4895).
   После reassign проверять чекером DUP_RACK=0 (RackCheck.java, там же).
3. Withdraw в облаке (последний брокер роли в ДЦ): `mcc stop <service>` → до FINISHED →
   `mcc withdraw --type service <service>` → `mcc withdraw --type storage "<queue>/<role>"`
   (уравнение — pexpect, см. mcc-host-worker/commands/lifecycle.md).
4. Задачу в операторе остановить: `mcc op_stop "queue://<fullQueue>" kafka.downscale-broker`.
5. Операцию в БД закрыть: UPDATE → done (шаблон в «Шаблоны SQL»).

## Работа с облаком (one-cloud) через mcc

**Вся работа с mcc — через скилл `mcc-host-worker`** (канон паттернов и граблей живёт там):

- диагностика хоста/сервиса: [mcc-host-worker/commands/query.md](../mcc-host-worker/commands/query.md) — `instances`, `status`, логи без ssh; маркеры лежащего хоста (LOST_MINION).
- пересоздание хоста с новыми дисками: [mcc-host-worker/commands/lifecycle.md](../mcc-host-worker/commands/lifecycle.md) — полный флоу `stop` → `delete` volumes по UUID (уравнение-подтверждение, автосolv через pexpect) → `start` → **`purge all`** в storage для освобождения квот кластера. Использовать для битых конфигов на диске (пустой sysconfig) и LOST_MINION.
- полное удаление сервиса+storage (`withdraw`): history/MDBSUP-4827.

⚠️ Перед delete/withdraw проверить в прод-БД, что нет RUNNING-операций по кластеру
(иначе заблокируются «Already has unapplied operation»).

## История

Каждый разобранный тикет — файл `history/MDBSUP-<key>-<date>.md`: cluster_id, operationId, причина из
Temporal, проверки хостов, применённые SQL, итог. Перед новым разбором смотреть `history/` и
`/Users/vl.ershov/Documents/Git/backstage/.claude/skills/db-worker/history/MDBSUP-4752-add-hosts-stuck-ops.md`
(эталонный разбор двух кейсов: ecom-fsa uc-контроллер, ads-kafka ec-контроллер), а также
`history/MDBSUP-4832-2026-08-26.md` (PMS: пустой kafka.sysconfig у controller-ключей; запись в PMS
только в application `mdb`; Temporal reset/restart workflow; mcc lifecycle для datatransfer-очередей).

## Грабли

- У table `operations` нет `updated_ts` — есть `created_ts/started_ts/finished_ts`.
- `host_state` не имеет column `status` — состояние хоста только в облаке/mcc.
- Прод-БД: подключение, шаблоны правок и грабли схемы — в скилле `db-worker`.
- Прод-Temporal: все паттерны запросов и грабли API — в скилле `temporal-worker`.
