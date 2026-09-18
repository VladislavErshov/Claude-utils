# MDBSUP-5521 — 2026-09-17 — update_user падает на невалидном ACL CLUSTER/* у robot-indexer-test

## Кейс

- **Кластер:** `search-vkpeople` (mdb10125, prod) `a596f57d-65fa-49f9-9ee4-3861839f733b`
- **Операция:** `update_user` `d3e84ee3-f198-4113-a672-c3beb2f99cd9` — failed, in_processing=true, attempts_left=0
- **Заявка:** «у пользователя robot-indexer-test ACL-правило CLUSTER/*, Kafka его не принимает, UI не отображает — заменить `*` на `kafka-cluster` или удалить правило»

## Корневая причина

В API-запросе `update_user` в 16:54:54 (успешная `update_user` `91108e1b` минутой ранее содержала только 6 валидных правил) пришло седьмое правило:

```json
{"host": "*", "aclOperation": "ALL", "resourceName": "*", "resourceType": "CLUSTER",
 "permissionType": "ALLOW", "resourcePatternType": "LITERAL"}
```

mdb-data записал его в `operations.operation_model` **без валидации** и передал в mdb-processing; processing отправил в Kafka AdminClient, который отверг весь батч: `Failed to createUser topic 'robot-indexer-test'. Invalid request: The only valid name for the CLUSTER resource is kafka-cluster`. Операция failed + in_processing=true → следующие операции по кластеру блокируются («Already has unapplied operation»).

Ключевой вывод: **правило не применилось нигде** — ни в Kafka ACL, ни в `permissions` БД. Требование из заявки («заменить на kafka-cluster») было основано на неверной посылке: чинить на кластере нечего, а UI правило не показывал именно потому, что его не существует.

## Диагностика (проверено)

1. Прод-БД: `operations` → failed/in_processing, error_message см. выше; `operation_model` — 7 правил, включая CLUSTER/*.
2. `permissions` БД (users.id=101333, permissions.id=89967) — только 6 валидных правил; БД и Kafka синхронны.
3. Kafka ACL (брокер ic, `unset KAFKA_OPTS JMX_PORT; kafka-acls.sh --bootstrap-server <fqdn>:9092 --command-config /opt/kafka/config/client.properties --list`): для robot-indexer-test те же 6 правил; из CLUSTER-ресурсов только `kafka-cluster` для kafka_exporter.
4. Temporal: `WorkflowId = d3e84ee3...` → 2 run, оба `WORKFLOW_EXECUTION_STATUS_FAILED`, RUNNING-ретрая нет.

## Фикс

```sql
BEGIN;
UPDATE operations SET status='done', in_processing=false, finished_ts=now(), error_message=NULL
WHERE id='d3e84ee3-f198-4113-a672-c3beb2f99cd9' AND status='failed';
COMMIT;
```

UPDATE 1, верификация: status=done, in_processing=f. На кластере правок не было. Заявителю: повторить правку без CLUSTER/* (или с resourceName=kafka-cluster, если кластерный ACL реально нужен).

## Хвост

- **Баг продукта (не закрыт):** mdb-data пропускает невалидный resourceName при resourceType=CLUSTER — [MDBDEV-3458](https://jira.vk.team/browse/MDBDEV-3458). Нужна валидация до создания операции с понятной ошибкой клиенту.
- Паттерн для похожих кейсов: если заявка просит «починить ACL» — сначала сверить три места (operation_model упавшей операции → permissions БД → фактические ACL через kafka-acls.sh); правило из упавшего payload могло вообще не примениться.
