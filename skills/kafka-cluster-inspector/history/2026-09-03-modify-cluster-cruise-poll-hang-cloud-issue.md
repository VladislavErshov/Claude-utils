# 2026-09-03: modify_cluster на dwh-kafka — updateConfigKafkaCruise висит в poll 4 часа из-за облачных проблем

## Кейсы

| Кластер | Операция | Создана |
|---|---|---|
| target-5-dwh (`b93e312e-dd8e-443c-9e12-4e60c05dd47c`) | `3bdbd733-693c-4cc7-a203-5734d1c21089` | 28.08 18:36, n.rasskazkin |
| trg-190836-dwh (`936fcc40-5e2f-453c-8443-782611b80c6b`) | `b5fb9483-5458-4a1a-bfb2-ba95406c7da4` | 31.08 20:20, n.rasskazkin |

Оба — новые `datatransfer-kafka` dwh (infra, prod): 3 брокера + 3 контроллера + 1 cruise (kc).
Симптом: modify_cluster failed, новые операции блокируются.

## Цепочка падения (единая)

`modifyKafkaCluster` → child `modifyKafkaCruise` → grandchild
`updateConfigKafkaCruise` (workflowId `<opId>_modify-cruise_update-cruise-config`) →
upsert конфига/capacity ОК → **бесконечный poll `cloud_getInfoForInstances([cruise-хост])`
каждые 15с** (dozens попыток, все COMPLETED) → ровно через **4ч**
WorkflowExecutionTimeout → FAILED. На родителях RetryPolicy нет
(`RETRY_STATE_RETRY_POLICY_NOT_SET`) → операция failed сразу после таймаута child'а.

Вторичный режим (target-5, последний run 03.09 13:03): activity
`cloud_getExistingServiceDcs` — 75с в очереди воркера, затем не уложилась в
StartToClose 30s, 3/3 попыток → RETRY_STATE_MAXIMUM_ATTEMPTS_REACHED → быстрый фейл.
(ретраи 3/5s/60s, scheduleToClose 14400s — из `activityTaskScheduledEventAttributes`).

## Что проверено и ИСКЛЮЧЕНО (не причина)

- `cruise-control.service` на обоих cruise-хостах active (с 31.08), err.log пуст.
  ⚠️ Юнит называется `cruise-control.service`, НЕ `kafka-cruise-control.service`.
- Оператор one-cloud-ops (`mcc ops`): «Cluster is AVAILABLE», «fresh, ready for actions».
- host-check.service inactive — норма (timer-based oneshot).
- **Контрольная группа**: за 31.08–03.09 десятки `updateConfigKafkaCruise` у других
  кластеров COMPLETED за 10–15с. Висели только эти два → проблема кластер-специфичная.

## Корневая причина и резолюция

Проблема была **на стороне облака** (one-cloud). Рестарт облачных компонентов →
poll сходится, операции прошли (подтверждено пользователем; контрольный SELECT по
operations не делался — port-forward упал).

## Выводы / чек-лист на будущее

1. **Poll в `updateConfigKafkaCruise` зависит от здоровья one-cloud**: зависший poll
   (десятки успешных `cloud_getInfoForInstances`, но условие не наступает) при живом
   cruise-сервисе и свежем операторе = первым делом подозревать облако.
2. Быстрая диагностика: listing `WorkflowType = 'updateConfigKafkaCruise'` — если у всех
   кластеров COMPLETED за ~10с, а у одного TIMED_OUT/TERMINATED ровно через 4ч — ждать
   внешнюю причину, не кластерную.
3. 4ч — это WorkflowExecutionTimeout самого child'а; activity-ретраи (3×30s) не спасают
   при системном лежании облака (см. также 2026-08-27 upscale reload, deploy-j2 drift).
4. Если операция уже failed/attempts_left=0 и облако починили — просто перезапустить
   операцию: reconcile-children идемпотентны (runIgnoringAlreadyStarted, MDBDEV-3245),
   ModifyKafkaCruise при ретрае стартует заново и догоняет конфиг.
