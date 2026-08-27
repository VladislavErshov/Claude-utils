# T11 Terminate в reload контроллеров + рестарт (2026-08-25)

**Результат: PASS**

Источник: прод-кластер `08a8e838` (ecom-fsa-adtech) — cloud exec падает на reload контроллера
(`confp --oneshot && systemctl restart kafka-controller.service`).

## Прогон

1. `POST …/hosts/controllers?dc=hc` → op `534ad312`. Deploy-фазы + broker reload прошли.
2. В момент `UpdateConfigKafkaController` (посреди последовательности хостов, после рестарта
   `1.controller.hc`) → terminate child `…_update-controller-config`.
3. Parent FAILED немедленно: childWorkflowExecutionFailure (terminated), retryState
   RETRY_STATE_NON_RETRYABLE_FAILURE — failure проброшен из
   `AbstractKafkaControllerScaleWorkflow.reloadControllers` (:154, через runIgnoringAlreadyStarted).
   Аналог прод-паттерна «упал в reload → операция failed».
4. Промежуточное состояние: PMS-кворум уже 6 voters (upsert был до deploy), host_state ещё 5
   (save не выполнялся) — рассинхрон только в БД.

## Ретрай

5. Закрыл операцию в БД (UPDATE), повторный `POST ?dc=hc` → op `66da4538`.
6. Все фазы идемпотентно доехали: child skip (`already has 2 replicas`), broker+controller
   reload, save = 6 хостов, parent COMPLETED.

## Верификация

- host_state: 6 контроллеров (dc=2, hc=2, kc=1, ic=1), дублей нет.
- PMS `kafka.controller.quorum` = 6 voters (11002@2.controller.hc ровно один раз) = host_state.
- mdb-data log: `Save upscaled Kafka controller hosts: 6`.

## Вывод

Рестарт после падения в controller-reload сходится к эталону T1; PMS не дублируется,
reload-фаза идемпотентна (повторный restart хостов безопасен). Единственный нюанс — parent
необорачивает terminated/упавший reload-child в типизированный failure (type=null), операция
в mdb-data просто failed; для дежурства сообщение неинформативно (minor).
