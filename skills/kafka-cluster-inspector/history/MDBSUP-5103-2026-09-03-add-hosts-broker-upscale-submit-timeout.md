# MDBSUP-5103 (Тип 2) — add_hosts «Broker upscale failed in 1 DC(s): [pc]»: реальная работа выполнена, упал no-op шаг

Дата: 2026-09-03. Кластеры datatransfer (project 17, prod, ns infra):
- `target-5-dwh` (`b93e312e-dd8e-443c-9e12-4e60c05dd47c`), операция add_hosts `1edb6d84-fbc3-44b9-8c80-3176f9b3afa3`
- `trg-190836-dwh` (`936fcc40-5e2f-453c-8443-782611b80c6b`), операция add_hosts `e1569d23-b282-4317-8a84-0344467d90a8`

Обе созданы `service.debezium-operator` 03.09 16:45 MSK (массовая миграция ДЦ datatransfer):
+1 брокер в новом ДЦ **uc** (были брокеры hc/kc/pc, контроллеры hc/kc/pc).

## Цепочка падения

Родитель (workflowId = operationId) → children: `reconcileKafkaCluster` + `upscaleKafkaBrokerInDc` × 4 ДЦ.

- reconcile, hc, kc, **uc — COMPLETED**. uc-child сделал всю работу:
  `cloud_submitQueueIfNeeded` → `cloud_isServiceExists` (нет) → `cloud_copyFlattenAndSubmitServiceWithStorage`
  (создание хоста в облаке) → poll `cloud_getServiceInfo` — брокер дошёл до RUNNING.
- **pc — FAILED**: упала первая же activity `cloud_submitQueueIfNeeded(hc, pc, infra, queue)` —
  StartToClose timeout 30s × 3 (retryPolicy maximumAttempts=3, backoff 5s→60s) →
  `RETRY_STATE_MAXIMUM_ATTEMPTS_REACHED`. Для pc путь был **no-op** (сервис уже существует;
  hc/kc прошли тот же submit нормально) — транзиентный таймаут ops-API.
- Родитель из-за pc-child failed → `error_message = "Broker upscale failed in 1 DC(s): [pc]"`,
  `operations.status=failed, in_processing=t, attempts_left=0` → блок новых операций.

## Проверка фактического состояния (всё в целевом)

- `1.broker.<cluster>.uc.one-infra.ru` в облаке RUNNING, миньон жив, `systemctl is-active kafka-broker` = active.
- Регистрация в KRaft: лог брокера «Successfully registered broker 23001 with broker epoch», BrokerState=3 (Jolokia :7777).
- Очередь оператора (`mcc --local -n infra -c hc ops "queue://<cluster>.datatransfer.db.production.mdb.prod"`) —
  только watch/availability/sync, висящих upscale-задач нет (submit не оставил мусора).
- RUNNING-ретраев в Temporal нет.

⚠️ Грабля проверки: `kafka-broker-api-versions.sh` с hc-брокера падает OOM в network-thread
AdminClient (Request METADATA failed) даже с `KAFKA_HEAP_OPTS=-Xmx1g` — инструментальная
проблема, не кластера. Регистрацию брокера проверять по логу/Jolokia на самом хосте.

## Фикс (прод-БД, одна транзакция)

1. `UPDATE operations SET status='done', in_processing=false, finished_ts=now(), error_message=NULL` × 2.
2. `INSERT INTO host_state` uc-брокеров × 2 (`{"dc":"uc"}`, onecloud/grafana ссылки по шаблону pc-соседей,
   cloud: `https://cloud.vk.team/cloud/UC/ns/infra/service/broker.<name>`). Родитель вставил бы их
   после всех children — не доехало.
3. `controllerDcs` не трогали (контроллеры не добавлялись; brokerDcs в kafkaParams отсутствует).

Верификация: обе операции done/in_processing=f, uc-строки в host_state на месте.

## Новое для каталога

- **Паттерн «реальная работа done, упал no-op шаг»**: в add_hosts (upscale broker per-DC) каждый
  child начинается с `cloud_submitQueueIfNeeded`; таймаут submit'а на ДЦ, где делать нечего,
  роняет родителя, хотя целевой ДЦ (uc) отработал полностью. Диагностика: смотреть children
  per-DC (`<opId>_<dc>`), сравнить выполненные activity uc- vs упавшего child'а.
- Отличие от MDBSUP-5051 (ложный успех get_result после рестарта оператора): там была
  ghost-строка в host_state и задача оператора; здесь наоборот — работы все done, строка не
  доехала, фикс чисто bookkeeping (UPDATE + INSERT, без op_stop и DELETE).
