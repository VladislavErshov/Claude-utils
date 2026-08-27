---
name: jira-mdbsup-solver
description: Разбор тикетов MDBSUP про зависшие/упавшие операции с кластерами MDB (Kafka и др.). Диагностика по цепочке Jira → прод-БД backstage_plugin_mdb → прод-Temporal → mcc-хосты, проверка реального состояния, предложение плана правок и выполнение ТОЛЬКО после явного одобрения пользователя. Используй при запросах вида «разбери MDBSUP-XXXX», «зависшая операция», «найди кластер и операции в темпорал».
allowed-tools: [bash]
---

# jira-mdbsup-solver: разбор тикетов MDBSUP про зависшие операции

Работай строго в три фазы: **Диагностика → План → Исполнение (только после "ок" пользователя)**.

## Фаза 1. Диагностика (только чтение)

1. **Jira** — `generic_jira_get_issue`: из описания достать cluster_id, operationId, тип БД, суть проблемы
   (типовой тикет: «операция failed → блокирует следующую операцию "Already has unapplied operation"»).
2. **Прод-БД** `backstage_plugin_mdb` (port-forward 53480, user `backstage`, пароль в SKILL.md db-worker;
   psql только из docker-контейнера `pg_backstage_plugin_mdb` через `host.docker.internal`):
   ```sql
   SELECT id, status, type, attempts_left, in_processing, created_ts, finished_ts, error_message
   FROM operations WHERE cluster_id='<uuid>' ORDER BY created_ts DESC LIMIT 10;
   SELECT host, params FROM host_state WHERE cluster_id='<uuid>' ORDER BY host;
   ```
3. **Прод-Temporal** `https://mdb-processing-temporal.common.mdb.one-infra.ru` (UI локально не открывается — только API):
   ```bash
   curl -s --get ".../api/v1/namespaces/default/workflows" \
     --data-urlencode "query=WorkflowId = '<operationId>'" | jq ...
   # по кластеру (не все workflow пишут ClusterId):
   curl -s --get ".../api/v1/namespaces/default/workflows" \
     --data-urlencode "query=ClusterId = '<cluster_id>'" | jq ...
   ```
   Копать цепочку: родительский workflow → child (`_update-broker-config`, per-DC `<opId>_..._<dc>_<n>`) →
   упавшая activity. Достать failure: `recurse(.cause? // empty) | .message` из
   `WORKFLOW_EXECUTION_FAILED` / `CHILD_WORKFLOW_EXECUTION_FAILED` / `ACTIVITY_TASK_FAILED`.
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

- psql через `docker cp` файла + `psql -f` (stdin-heredoc молча не применяет DML!).
- Оборачивать в BEGIN/COMMIT, после — верификационный SELECT обеих таблиц.
- По завершении — обновить `history/` этого скилла (см. ниже) и при необходимости заметку в
  `db-worker/history/MDBSUP-*.md`.

## Шаблоны SQL

```sql
UPDATE operations SET status='done', in_processing=false, finished_ts=now(), error_message=NULL
WHERE id='<op_id>' AND status='failed';

INSERT INTO host_state (cluster_id, host, update_ts, onecloud_ui_link, grafana_dashboard_link, params, shard_id)
VALUES ('<cluster_id>', '<fqdn>', now(),
  'https://cloud.vk.team/cloud/<DC_UPPER>/ns/<ns>/service/<service_name>',
  'https://goc.vk.team/d/deahz1a8c50xsb/kafka-cluster?orgId=1&var-cluster=<cluster_name>&var-instance=<fqdn>&var-vm_datasource=P1D7AE08E5B4F8828',
  '{"dc": "<dc>"}'::jsonb, NULL);
```
(onecloud/grafana ссылки лучше копировать из строк соседних хостов кластера, заменяя dc/instance.)

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
- Temporal API: query обязательно через `--data-urlencode`, иначе `invalid query: malformed SQL`.
- Порт продовой БД в port-forward: 53480 (7432 на хосте; в истории встречаются разные варианты —
  проверять `lsof -iTCP:53480`).
