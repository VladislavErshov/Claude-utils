# 2026-08-25 — MDBSUP-4752 / MDBSUP-4856 — Разбор зависших операций add_hosts (upscale Kafka controller)

Прод-БД `backstage_plugin_mdb` (port-forward 53480, user `backstage`, креды в SKILL.md db-seed).
Прод-Temporal: `https://mdb-processing-temporal.common.mdb.one-infra.ru` (API: `/api/v1/namespaces/default/workflows`,
UI не доступен локально — только curl).

## Общий паттерн проблемы

Операция `add_hosts` (по факту workflow `upscaleKafkaController` в Temporal) падает на шаге
`config reload` → в таблице `operations` висит `status=failed`, `in_processing=true`, `attempts_left=0`
→ новые операции блокируются ошибкой «Already has unapplied operation for cluster».

## Диагностика

1. **Temporal UI/API**: `https://mdb-processing-temporal.common.mdb.one-infra.ru`
   ```bash
   curl -s --get ".../api/v1/namespaces/default/workflows" \
     --data-urlencode "query=WorkflowId = '<operationId>'"
   ```
   Смотреть цепочку: родитель `upscaleKafkaController` → child `updateConfigKafkaBroker`
   → per-DC workflows `<opId>_update-broker-config_<dc>_<n>` → activity `kafka_host_restartBrokerInstanceSsh`.

2. **Типовая причина**: `Failed exec call: <host>::confp --oneshot && systemctl restart kafka-broker.service`
   → `Invalid type of response received: class one.nii.http.Response` — транзиентная ошибка one-cloud proxy.
   Проверить руками: `mcc -n infra sshexec <host> "confp --oneshot"` — если проходит, хост жив.

3. **Проверка хостов через mcc**: `mcc -n infra sshexec <host> "hostname; systemctl is-active kafka-controller"`.

## Кейс 1: ecom-fsa (08f5ed31-fdc7-4505-a3fd-69d39724bdc2), операция 08a8e838

- Операция `08a8e838-b731-43e9-9174-1263128c549e`, add_hosts, failed c 2026-08-20, error «Unknown error while calling cloud».
- Проверено: `1.controller.ecom-fsa-adtech-kafka.uc.one-infra.ru` существует, kafka-controller active.
- Действия (прод):
  ```sql
  UPDATE operations SET status='done', in_processing=false, finished_ts=now(), error_message=NULL
  WHERE id='08a8e838-b731-43e9-9174-1263128c549e' AND status='failed';

  INSERT INTO host_state (cluster_id, host, update_ts, onecloud_ui_link, grafana_dashboard_link, params, shard_id)
  VALUES ('08f5ed31-fdc7-4505-a3fd-69d39724bdc2', '1.controller.ecom-fsa-adtech-kafka.uc.one-infra.ru', now(),
    'https://cloud.vk.team/cloud/UC/ns/infra/service/controller.ecom-fsa-adtech-kafka',
    'https://goc.vk.team/d/deahz1a8c50xsb/kafka-cluster?orgId=1&var-cluster=ecom-fsa-adtech-kafka&var-instance=1.controller.ecom-fsa-adtech-kafka.uc.one-infra.ru&var-vm_datasource=P1D7AE08E5B4F8828',
    '{"dc": "uc"}'::jsonb, NULL);
  ```
- Итог: операция done, контроллер uc в host_state (id 114489).

## Кейс 2: ads-kafka (6f4133c3-1cfa-4ed5-be46-a60532a29952), операция 0455a8f4

- MDBSUP-4856. Операция `0455a8f4-5a12-4bf9-a090-b884c189f5d1`, add_hosts, failed c 2026-08-24.
- Temporal: 2 рана upscaleKafkaController (24.08 и 25.08), оба failed. Причина — config reload failed
  на 5 брокерах (hc×2, kc×2, pc×2... точный список в error_message) через транзиентную ошибку cloud-proxy.
- confp на брокере вручную проходит.
- Проверено: все 4 контроллера (hc, kc, pc + новый ec) существуют, kafka-controller active на каждом.
- ⚠️ В 14:21 стартовал retry-ран той же операции (workflow `ce371f6a-...`, RUNNING) — по решению пользователя
  не ждали его завершения, таблицы поправили руками.
- Действия (прод): операция 0455a8f4 → done/in_processing=false/finished_ts=now/error_message=NULL;
  host_state + `1.controller.ads-kafka-rustore-kafka.ec.one-infra.ru` (`{"dc":"ec"}`, onecloud/grafana ссылки по шаблону).
- Итог: операция done, 4 контроллера в host_state. Блокировка update_image снята.

## Грабли

- `psql` через stdin-heredoc (`docker exec ... <<SQL`) молча не применяет DML — только `docker cp` + `psql -f`.
- Прод-БД: port-forward 53480, user `backstage` (креды в SKILL.md db-seed), psql только из docker-контейнера.
- mcc `status <service>` может отдать EntityNotFoundException, даже когда инстансы есть — проверять по хостам напрямую.
- Retry-ран той же upscale-операции может стартовать позже (у ads-kafka: `ce371f6a-...` в 14:21 после failed-рана 25.08 13:15).
  По решению пользователя его не ждали — таблицы поправлены руками, retry по завершении перезапишет статус операции (уже done).
- Поиск workflow по кластеру: `query=ClusterId = '<uuid>'` — но search-атрибут ClusterId пишется не всеми workflow
  (у ecom-fsa его не было); надёжнее искать `WorkflowId = '<operationId>'` или по префиксу `<operationId>_%`.
