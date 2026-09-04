# T10 Child-падение в waitInstanceRunning → PARTIAL_UPSCALE_FAILURE + рестарт (2026-08-25)

**Результат: частично PASS + найден БАГ 5 (бесконечное ожидание в reload-цикле) и симптом T14**

Источник сценария: прод-кластер `85169290` (ecom-dynamic-adtech) — `Instance … is not RUNNING`,
ретраи waitInstance упёрлись. Локально смоделировано terminate'ом child в фазе waitInstanceRunning.

## Прогон

1. `POST …/hosts/controllers?dc=hc` → op `5f1fbdc7` (parent + `_hc` child).
2. Child дошёл до `waitInstanceRunning` (cloud_rescaleService DONE, started polling-таймер).
3. Terminate `_hc` по UI API (csrf-cookie).
4. **Parent FAILED ровно как задумано**: `Controller upscale failed in 1 DC(s): [hc]`,
   type=`PARTIAL_UPSCALE_FAILURE`, nonRetryable=true
   (UpscaleKafkaControllerInClusterWorkflowImpl.deployControllers:133).
5. Промежуточное состояние: PMS-кворум уже содержал `11002@2.controller.hc` (upsert до deploy —
   допустимо), host_state hc=1, операция in_progress (закрыта руками UPDATE).

## Ретрай

6. Повторный `POST ?dc=hc` → op `9d324fe4`. Все child COMPLETED, hc: `already has 2 replicas,
   nothing to submit` — идемпотентность submit подтверждена.
7. **Reload-фаза зависла навсегда**: инстанс `2.controller.hc` в облаке застрял в `DEPLOYING`
   ("Waiting sandbox to be ready", 3+ часа) после terminate первой попытки.
   `UpdateConfig…Controller` крутился в цикле `cloud_getInfoForInstances` + `Pausing PT15S` ~25 мин
   (пока не терминировали руками).

## БАГ 5: executeReloadCycle не имеет таймаута на non-RUNNING хосты

`KafkaHostReloadHelper.executeReloadCycle` (KafkaHostReloadHelper.java:55-77):
`iteratePolicy.takeNext` отдаёт только RUNNING-инстансы, DEPLOYING-хост никогда не выбирается,
условие выхода (`promises.isEmpty() && processedHosts==allHosts`) недостижимо → бесконечный
цикл 15s-таймеров. Workflow-таймаут 3ч (TTL) спасёт, но операция всё это время висит RUNNING.
Прод-паттерн `85169290` — тот же случай (инстанс не поднялся). Предложение: ограничить ожидание
(напр. общий deadline на хост) или падать RELOAD_NOT_STARTED после N итераций.

## Попутно: симптом T14 (облако опережает host_state)

- После упавшей операции downscale через mdb-data построил цель `hc: 2→0`
  (`Downscale supports remove only one controller at a time. Current: 2, target: 0`,
  INVALID_REPLICAS_COUNT): host_state не знал о 2-м контроллере (save не выполнился),
  а downscale берёт фактические реплики из облака → рассинхрон база/облако.

## Cleanup

- Прямой запуск `downscaleKafkaControllerInCluster` (workflowId `t10-cleanup-downscale-hc`,
  цель hc:1, input скопирован с успешного d012d1cc) → COMPLETED: битый DEPLOYING-инстанс
  удалён, PMS-кворум = 5 voters (dc=2, hc/kc/ic=1) = host_state = baseline.
- Грабли прямого старта через UI API: taskQueue workflow = `kafka-activities-queue`
  (НЕ kafka-workflow-queue — WF повиснет на WORKFLOW_TASK_SCHEDULED);
  workflowExecutionTimeout — строкой `"10800s"`, не объектом.

## Вердикты

- T10 (terminate child в waitInstance → non-retryable PARTIAL_UPSCALE_FAILURE; ретрай идемпотентен
  по submit) — PASS.
- БАГ 5 (бесконечный reload-цикл на non-RUNNING хосте) — воспроизведён, требует решения.
- T14-рассинхрон подтверждён как реальный кейс (downscale строит 2→0).
