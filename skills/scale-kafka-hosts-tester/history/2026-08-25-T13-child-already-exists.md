# T13 Ретрай operationId при живых/завершённых children → WORKFLOW_ALREADY_EXISTS (2026-08-25)

**Результат: PASS — механизм runIgnoringAlreadyStarted переиспользует существующих children**

Источник: прод-кластер `9c3cd391` — `START_CHILD_WORKFLOW_EXECUTION_FAILED_CAUSE_WORKFLOW_ALREADY_EXISTS`
на child `updateConfigKafkaBroker` (`<opId>_update-broker-config`). Прод-ран в итоге FAILED,
затем COMPLETED после ретрая.

## Прогон

1. `POST …/hosts/controllers?dc=kc` → op `593a223e` (kc 1→2).
2. Дождался `_update-broker-config` COMPLETED (parent ещё RUNNING, controller reload идёт)
   → terminate PARENT (children не тронуты).
3. Прямой перезапуск того же workflowId (`593a223e…`, input из первого рана,
   taskQueue `kafka-activities-queue`) — имитация ретрая операции с тем же operationId.
4. **Прод-паттерн воспроизведён**: 5 × EVENT_TYPE_START_CHILD_WORKFLOW_EXECUTION_FAILED
   CAUSE_WORKFLOW_ALREADY_EXISTS (`_dc/_hc/_kc/_ic` + `_update-broker-config`) —
   все children прошлого рана уже существовали.
5. **НО parent не упал**: `ChildWorkflowUtils.runIgnoringAlreadyStarted` подхватил handles
   существующих children, дождался их результатов, доехал controller reload → COMPLETED.
6. Save: 7 хостов (dc=2, hc=2, kc=2, ic=1), PMS 7 voters, дублей нет, второй деплой не выполнялся.

## Вывод

- Локальный код (бранч MDBDEV-3180) корректно обрабатывает WORKFLOW_ALREADY_EXISTS на ретрае
  того же operationId — в отличие от прода, где этот кейс давал FAILED-ран (возможно, старый
  код без already-started handshake, или child был RUNNING на момент старта — тогда Temporal
  кидает WorkflowAlreadyStarted как activity-failure, а не START_CHILD_FAILED; для
  RUNNING-child сценарий остался непроверенным, T9 частично его покрывает через
  ignoreAlreadyStarted у `Async.function`).
- Грабли воспроизведения: terminate parent НЕ каскаден на children (Temporal) — поэтому
  дети живут и конфликтуют с новым раном; именно это и нужно было для паттерна.

## Примечание

Операция в mdb-data осталась in_progress (direct-start не вызывает её callback-закрытие) —
закрыта UPDATE'ом. Это же — причина, почему прямой запуск в скилле запрещён для позитивных
сценариев: save вызывается из workflow, а operation lifecycle — нет.
