# 2026-08-31 — MDBDEV-3245: reconcileKafkaCluster блокировал рестарт upscaleKafkaControllerInCluster

- **Тип workflow**: `upscaleKafkaControllerInCluster` (parent) → `reconcileKafkaCluster` (child)
- **Кластер**: `7e5d0e54-8c67-4dd0-90bd-d965d516676c`
- **Операция/workflowId**: `7e5d0e54-8c67-4dd0-90bd-d965d516676c`
- **Симптом**: «reconcileKafkaCluster не дает вф перезапускаться» — каждый рестарт операции
  падает через ~0.5 с после старта.

## Цепочка падения (run 3, eventId)

- `e1` START, `e5-7` discovery-activity, `e11-13`mdb-data activity
- `e17` `START_CHILD_WORKFLOW_EXECUTION_INITIATED` → child `…_reconcile-cluster`
- `e18` `START_CHILD_WORKFLOW_EXECUTION_FAILED` — child уже существует
- `e22` `WORKFLOW_EXECUTION_FAILED`: `ChildWorkflowFailure` → cause `ApplicationFailure
  (type=io.temporal.client.WorkflowExecutionAlreadyStarted)`, `RETRY_STATE_NON_RETRYABLE_FAILURE`

## Таймлайн по run'ам (listing)

| Run | Старт | Финал | Что было |
|---|---|---|---|
| `01a057ca…` | 12:27:44 | FAILED 12:36:20 | reconcile child COMPLETED 12:27:45, родитель упал позже (деплой) |
| `01a0581e…` | 14:00:04 | FAILED 14:00:05 | Instant-фейл на старте child'а |
| `01a0581f…` | 14:01:13 | FAILED 14:01:14 | То же |

## Корневая причина

1. Child-workflowId детерминированный: `<parentId>_reconcile-cluster`
   (`WorkflowOptionsBuilder.optionsForChild`).
2. Reuse policy всех child'ов — `WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE_FAILED_ONLY`:
   повторный старт разрешён только после упавшего run'а. После **успешного** завершения child'а
   Temporal отклоняет новый старт тем же workflowId → `WorkflowExecutionAlreadyStarted`.
3. Хелпер `ReconcileKafkaClusterWorkflow.reconcileCluster` (статический, все 5 call-site'ов
   Kafka-флоу) вызывал child напрямую, без `ChildWorkflowUtils.runIgnoringAlreadyStarted` —
   в отличие от остальных child-вызовов кодовой базы. Non-retryable фейл → рестарт операции
   заблокирован навсегда.

## Фикс

`reconcileCluster` обёрнут в `runIgnoringAlreadyStarted`: COMPLETED child → скип (reconcile уже
сделан), RUNNING → скип (окно гонки секунды), FAILED → перезапуск, барьер сохраняется.
Тест: `UpscaleKafkaControllerInClusterWorkflowImplTest.restart_afterReconcileCompleted_doesNotBlockOnAlreadyStartedChild`
(run 1: reconcile ок + деплой падает; run 2 тем же workflowId: скип reconcile, флоу до save).

MR: mdb-processing!435.

## Открытые вопросы

- Исходное падение run 1 (12:27→12:36, деплой) — отдельная история, этим разбором не покрыта
  (история не-последних run'а через UI-API недоступна).
