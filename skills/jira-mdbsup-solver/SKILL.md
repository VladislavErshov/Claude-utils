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
2. **Прод-БД** `backstage_plugin_mdb` (port-forward 53480, user `backstage`, пароль в SKILL.md db-seed;
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
  `db-seed/history/MDBSUP-*.md`.

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

## Работа с облаком (one-cloud) через mcc — lifecycle хостов

Диагностика лежащего хоста и операции удаления/миграции делаются через mcc (подробнее — скилл `mcc-host-worker`).

### Диагностика лежащего хоста

```bash
# реальные инстансы сервиса + состояние (state/outcome/outcome_text) — без ssh
mcc --local -n infra instances "%.controller.<cluster>%" -f table
mcc --local -n infra status "<service>" -f table
```

Маркеры лежащего хоста: `state=FINISHED`, `outcome=LOST_MINION`, `outcome_text="Unreported by minions"`
или `"rejected required storage's minion: not running"` — миньон (VM) умер, диск/сервис остались.
Симптом в UI MDB: метрики хоста `unknown` (в т.ч. disk unknown).

Грабли: `mcc status <FQDN-хоста>` падает EntityNotFoundException — по FQDN работает только
`mcc instances "<полный FQDN>"` (паттерн `%`), status принимает имя *сервиса* (`controller.<cluster>`).

### Lifecycle: start / stop / withdraw / rescale

```bash
mcc --local -n infra start   "<host-prefix|service>"        # поднять остановленный
mcc --local -n infra stop    "<service>" [--now]            # --now = без graceful
mcc --local -n infra restart "<pattern>" [-m <min_running>] # осторожно, паттерн!
mcc --local -n infra withdraw "<service>"                   # вывести сервис (удалить из облака)
mcc --local -n infra withdraw "<namespace-path>/<role>"     # вывести storage (= удаление диска)
```

Порядок удаления хоста с диском (см. history/MDBSUP-4827):
1. `stop` сервиса (может требовать `-c <DC>` — без него EntityNotFoundException, облако не то).
2. `withdraw` **сервиса** — пока сервис держит replicas, storage-withdraw падает
   `ServiceValidationException: Cannot withdraw storage used by services`. `rescale 0` не работает.
3. `withdraw` **storage** — это и есть удаление диска. Подтверждение `N mod M` — ответ подавать
   через stdin: `echo "3" | mcc ... withdraw`.
4. Пересоздание хоста (миграция на нового миньона): rescale сервиса обратно / операция в mdb-data —
   поднять replicas, облако создаст новый хост с новым диском.
5. Success-критерий: EntityNotFoundException по сервису/storage (сущность исчезла).

### Миграция лежащего хоста (LOST_MINION)

Если хост FINISHED/LOST_MINION: диск привязан к мёртвому миньону, пересоздание = удалить диск
(withdraw storage) и дать облаку создать хост заново. Перед удалением проверить в прод-БД, что
нет RUNNING-операций по кластеру (иначе заблокируются «Already has unapplied operation»).

## История

Каждый разобранный тикет — файл `history/MDBSUP-<key>-<date>.md`: cluster_id, operationId, причина из
Temporal, проверки хостов, применённые SQL, итог. Перед новым разбором смотреть `history/` и
`/Users/vl.ershov/Documents/Git/backstage/.claude/skills/db-seed/history/MDBSUP-4752-add-hosts-stuck-ops.md`
(эталонный разбор двух кейсов: ecom-fsa uc-контроллер, ads-kafka ec-контроллер).

## Грабли

- У table `operations` нет `updated_ts` — есть `created_ts/started_ts/finished_ts`.
- `host_state` не имеет column `status` — состояние хоста только в облаке/mcc.
- Temporal API: query обязательно через `--data-urlencode`, иначе `invalid query: malformed SQL`.
- Порт продовой БД в port-forward: 53480 (7432 на хосте; в истории встречаются разные варианты —
  проверять `lsof -iTCP:53480`).
