# MDBSUP-5022 — Kafka bootstrap.servers из host_state в cluster links (2026-09-03)

Тикет: https://jira.vk.team/browse/MDBSUP-5022
MR: https://gitlab.corp.mail.ru/mdb/backstage/-/merge_requests/366 (branch ershov/MDBSUP-5022-kafka-bootstrap-servers, commit cb6873d0)

## Что проверяли

1. `KafkaUtils.selectBrokers` — снят кап «3 ДЦ» (теперь по брокеру из каждого ДЦ).
2. `KafkaUtils.generateConnectionSettings` — новый общий хелпер строки подключения.
3. `ClusterManager.getClusterLinks` — для Kafka актуализирует `databaseLinks.connectionUrl` на лету из `host_state` (read-time, БД не мутирует).
4. `UpdateDbConnectionUrlTaskProcessor` — теперь использует общий хелпер.

## Инфраструктура

- stubs compose (postgres 6432, redis 6379) + Backstage 7007 — были подняты, но бэкенд работал 42ч без фиксы → перезапущен `yarn mdb-start-backend` (лог /tmp/backstage.log).
- mdb-data/temporal не нужны — эндпоинт читает только 6432.

## Юнит-тесты

```bash
cd plugins/mdb-backend
yarn backstage-cli package test --rootDir=. --ci --watchAll=false test/util/KafkaUtils.test.ts
# Tests: 32 passed
```

⚠️ Без `--ci --watchAll=false` `backstage-cli package test` виснет (watch-режим) — запускать только так.
⚠️ Из корня репо jest не находит конфиг — только из `plugins/mdb-backend`.

## Сид (/tmp/mdbsup5022-seed.sql)

Кластер `11111111-5022-4022-8022-502250225022` (name=mdbsup5022-test, kafka, project 160, namespace infra=2):
- host_state: 6 брокеров в 5 ДЦ (dc×2, ec, kc, rc, uc) + 1 контроллер — FQDN `1.broker.mdbsup5022-test-160-kafka.<dc>.one-infra.ru`
- one_cloud_meta db-service: queue=mdbsup5022-test-160-kafka
- cluster_links.database_links.connectionUrl = ПРОТУХШИЙ: 3 брокера только из dc (репродукция бага из тикета)
- services_auth: ('local-tester', 160, 'w')

## Проверка

```bash
TOKEN=$(node -e "const jwt=require('/Users/vl.ershov/Documents/Git/backstage/node_modules/jsonwebtoken');console.log(jwt.sign({serviceName:'local-tester',projectId:160,accessType:'w'},'2210c0a2-fb9b-461f-9f21-a25acebb2559',{expiresIn:'1d'}));")
curl -s "http://localhost:7007/api/mdb/cluster/11111111-5022-4022-8022-502250225022/links" -H "Authorization: ${TOKEN}"
```

Результат: `bootstrap.servers=1.broker...dc:9092,1.broker...ec:9092,1.broker...kc:9092,1.broker...rc:9092,1.broker...uc:9092`
— все 5 ДЦ, по одному брокеру из ДЦ (2 dc-брокера дедуплицированы, контроллер исключён), SASL-секция на месте.
Значение в БД осталось протухшим — актуализация read-time (by design).
Edge «нет брокеров → connectionUrl остаётся старым» покрыт юнит-тестом generateConnectionSettings=undefined.

## Таска update_db_connection_url (вторая половина фиксы, 2026-09-03)

Процессор локален для БД Backstage (host_state → cluster_links), mdb-data/PMS не нужны.
Триггер синтетической операцией (воркер PT10S, условия: task scheduled + attempts>0 + op in_progress + депсы NULL):

```sql
INSERT INTO operations (id, cluster_id, created_by, status, type, db_type, operation_model, attempts_left, in_processing)
VALUES ('22222222-5022-4022-8022-502250225022', '<cluster_id>', 'local-tester', 'in_progress', 'add_hosts', 'kafka',
        '{"dbType":"kafka","clusterId":"<cluster_id>"}'::jsonb, 3, false);
INSERT INTO tasks (type, attempts_left, status, operation_id, is_critical, delay_after_sec)
VALUES ('update_db_connection_url', 3, 'scheduled', '<operation_id>', true, 0);
```

`operation_model` достаточно `{"dbType","clusterId"}` — процессор читает только их.
Результат: task=done за один цикл, `cluster_links.database_links.connectionUrl` **персистнут** в БД
со всеми 5 ДЦ. После этого GET /links отдаёт то же значение (read-time актуализация совпадает с записанным).

## Итог

Обе половины фиксы проверены на локальном стенде: read-time (links) + persist (таска). Юнит 32 passed.
