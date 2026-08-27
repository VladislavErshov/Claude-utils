---
name: jira-mdbsup-solver
description: Разбор тикетов MDBSUP про произвольные проблемы с кластерами MDB — зависшие/упавшие операции, недоступность, ошибки подключения/авторизации клиентов, конфиги/PMS, миграции, resize. Диагностика по цепочке Jira → прод-БД (db-worker) → прод-Temporal (temporal-worker) → mcc-хосты (mcc-host-worker); кластерная специфика — в скиллах-инспекторах (Kafka/Redis/Mongo/ClickHouse/PostgreSQL). План правок и исполнение ТОЛЬКО после явного одобрения пользователя. Используй при запросах вида «разбери MDBSUP-XXXX», «зависшая операция», «найди кластер и операции в темпорал».
allowed-tools: [bash]
---

# jira-mdbsup-solver: разбор тикетов MDBSUP

Скилл-**оркестратор** для произвольных проблем с кластерами MDB: зависшие/упавшие операции,
недоступность кластера, ошибки клиентов (подключение/авторизация), конфиги/PMS, миграции,
resize. Отвечает за цепочку Jira → прод-БД → Temporal/оператор → mcc; специфика самих
кластеров (подключение, каталог проблем, фиксы) живёт в скиллах-инспекторах — см. таблицу
«Маршрутизация по типам БД».

**Не выполнять ничего до явного одобрения пользователя** — сначала диагноз и план.

## Общий порядок работы

1. **Jira** — `generic_jira_get_issue`: из описания достать cluster_id, тип БД, суть
   проблемы; для кейсов про операции — operationId.
2. **Прод-БД** `backstage_plugin_mdb` — вся работа через скилл **`db-worker`**
   (port-forward 53480, psql из docker-контейнера, диагностические SELECT'ы, шаблоны
   правок, грабли схемы и DML). Креды — в SKILL.md db-worker.
3. **Прод-Temporal** — скилл **`temporal-worker`** (поиск workflow по operationId /
   ClusterId, история, failure-цепочки, retry-политики) — для кейсов, где была операция.
4. **mcc-хосты** — проверить, что хосты реально существуют и живы:
   ```bash
   mcc -n infra sshexec -n infra <host> "hostname; systemctl is-active <service>"
   ```
   (namespace см. в таблице namespaces: domain one-infra → infra, idzn → dzen). `mcc status <service>` может
   врать EntityNotFoundException — верить sshexec по хостам.
   Типовая транзиентная причина: `confp --oneshot && systemctl restart ...` →
   `Invalid type of response received: class one.nio.http.Response` (ошибка one-cloud proxy; вручную проходит).
5. **Специфика кластера** — дальнейшая диагностика и починка по скиллу-инспектору
   из таблицы ниже (тип БД берём из Jira / `db_cluster`).

## Маршрутизация по типам БД (специфика кластеров)

Диагностику и починку кластеров делать **только через скиллы-инспекторы** — там канон
подключения (ACL, пароли, пути), каталог известных проблем и сценарии фиксов. Этот скилл
отвечает только за цепочку Jira → БД → Temporal/оператор и закрытие операций.

| Тип БД                        | Скилл                                                                                                                                                                   | Типовые кейсы MDBSUP                                                                                                                                                                                             |
|-------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| Kafka                         | **`kafka-cluster-inspector`** (+ `kafka-host-inspector`, `kafka-log-investigator`, `kafka-metrics-investigator`, `kafka-reassign-partitions`, `kafka-config-inspector`) | downscale-broker застрял в операторе → [commands/one_cloud_ops.md](../kafka-cluster-inspector/commands/one_cloud_ops.md); modify/resize, sysconfig, KRaft-quorum, SCRAM/авторизация клиентов — `history/` скилла |
| Redis Cluster (шардированный) | **`redis-cluster-inspector`**                                                                                                                                           | после сфейлившегося change_primary/failover в шарде 2 мастера («expected 1 MASTER, found 2») → чинить по каталогу скилла, затем закрывать операцию в БД                                                          |
| Redis Sentinel (replica set)  | **`redis-sentinel-inspector`**                                                                                                                                          | known-peers / зомби-хосты, SENTINEL RESET, битый AOF, вечная переливка реплик                                                                                                                                    |
| MongoDB                       | **`mongo-cluster-inspector`**                                                                                                                                           | rs.status / отставшие ноды, смена мастера, BSONObjectTooLarge, диск 95%+ после удаления                                                                                                                          |
| ClickHouse                    | **`clickhouse-cluster-inspector`**                                                                                                                                      | broken parts, Keeper quorum / split-brain, TOO_MANY_SIMULTANEOUS_QUERIES                                                                                                                                         |
| PostgreSQL                    | **`postgres-cluster-inspector`**                                                                                                                                        | Stolon/etcd-состояние, stolonctl, pgbouncer                                                                                                                                                                      |

## Сценарий: зависшие/упавшие операции

Типовой тикет: «операция failed → блокирует следующую операцию "Already has unapplied
operation"». Диагностика — шаги 1–4 выше, плюс специфика операций:

- workflowId операции = operationId; упавший child/activity искать рекурсией cause
  (скилл `temporal-worker`).
- ⚠️ Проверить, нет ли сейчас RUNNING-ретрая той же операции (workflow с тем же
  workflowId, статус RUNNING) — правки при живом ретрае нельзя.
- Если Temporal по operationId пуст — возможно это оператор one-cloud-ops (см. ниже).

### План

Сформулировать пользователю компактный план:
- что нашли (операция, workflow, причина падения, состояние хостов);
- какие будут правки (SQL: UPDATE operations → done/in_processing=false/finished_ts=now/error_message=NULL;
  INSERT в host_state недостающих хостов с onecloud_ui_link / grafana_dashboard_link по шаблону соседних
  хостов кластера, params `{"dc": "<dc>"}`);
- риски (RUNNING-ретрай, отсутствие хоста в облаке — тогда NOT NULL-инсерты делать нельзя);
- при RUNNING-ретрае предупредить: можно дождаться авто-завершения или править руками.

### Исполнение

- DML в прод-БД — по правилам скилла `db-worker` (docker cp + `psql -f`, BEGIN/COMMIT,
  верификационный SELECT после).
- Кластерные фиксы — по канону скилла-инспектора соответствующего типа БД.

## Оператор one-cloud-ops (флоу, которых нет в Temporal)

Некоторые операции выполняет **оператор one-cloud-ops** — в Temporal workflow по
operationId будет ПУСТО. Признак из error_message: «Operator task <fullQueue> in
progress [object Object]» — это `GetResultOperatorTaskProcessor` (mdb-backend): DONE он
ставит только когда задача исчезает из `operators.<name>.tasks` в статусе оператора.

Снять состояние оператора (общий вид):

```bash
mcc --local -n infra -c <dc> ops "queue://<fullQueue>" -f json | jq '.[0] | {alerts, tasks: .operators.<name>.tasks}'
```

Что означают alerts/tasks, ручное выполнение шагов задачи и `mcc op_stop` — специфика
по типу БД; для Kafka — [kafka-cluster-inspector/commands/one_cloud_ops.md](../kafka-cluster-inspector/commands/one_cloud_ops.md).

## Работа с облаком (one-cloud) через mcc

**Вся работа с mcc — через скилл `mcc-host-worker`** (канон паттернов и граблей живёт там):

- диагностика хоста/сервиса: [mcc-host-worker/commands/query.md](../mcc-host-worker/commands/query.md) — `instances`, `status`, логи без ssh; маркеры лежащего хоста (LOST_MINION).
- пересоздание хоста с новыми дисками: [mcc-host-worker/commands/lifecycle.md](../mcc-host-worker/commands/lifecycle.md) — полный флоу `stop` → `delete` volumes по UUID (уравнение-подтверждение, автосolv через pexpect) → `start` → **`purge all`** в storage для освобождения квот кластера. Использовать для битых конфигов на диске (пустой sysconfig) и LOST_MINION (разбор — kafka-host-inspector/history/MDBSUP-4867).
- полное удаление сервиса+storage (`withdraw`) — kafka-cluster-inspector/history/MDBSUP-4827.

⚠️ Перед delete/withdraw проверить в прод-БД, что нет RUNNING-операций по кластеру
(иначе заблокируются «Already has unapplied operation»).

## История (связка с кластерными скиллами)

- **`history/` этого скилла — заглушки задач**: `MDBSUP-<key>-<date>.md` с cluster_id,
  operationId, сутью в 2–3 строках и **ссылкой на полный разбор** в `history/`
  скилла-инспектора соответствующего типа БД. Полные разборы кластерных кейсов живут
  ТОЛЬКО там — дублирование запрещено; повторяющиеся паттерны в новых кейсах заменять
  ссылками на старые разборы.
- Исключение — кейсы **без кластерной специфики** (Temporal UI API, vault, mdb-backend):
  остаются полными файлами здесь (например, MDBSUP-4887, MDBSUP-4894).
- Перед новым разбором смотреть `history/` здесь, эталонный разбор
  `/Users/vl.ershov/Documents/Git/backstage/.claude/skills/db-worker/history/MDBSUP-4752-add-hosts-stuck-ops.md`
  (два кейса: ecom-fsa uc-контроллер, ads-kafka ec-контроллер) и `history/` скилла-инспектора
  нужного типа БД.

## Грабли

- У table `operations` нет `updated_ts` — есть `created_ts/started_ts/finished_ts`.
- `host_state` не имеет column `status` — состояние хоста только в облаке/mcc.
- Прод-БД: подключение, шаблоны правок и грабли схемы — в скилле `db-worker`.
- Прод-Temporal: все паттерны запросов и грабли API — в скилле `temporal-worker`.
