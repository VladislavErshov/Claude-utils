# 2026-08-27: upscaleKafkaControllerInCluster — массовые фейлы reload-фазы

## Запрос

«Посмотри историю операций по апскейлу кафка контроллеров, есть ли проблемы с кодом на
нашей стороне». Тип: `upscaleKafkaControllerInCluster`.

## Статистика (26–27.08.2026)

18 запусков: 8 FAILED / 9 COMPLETED / 1 «RUNNING» (фактически FAILED, статус в listing
протух). Неудачи:

| Кластер | Операция (workflowId) | Фейлы | Итог |
|---|---|---|---|
| eba4c8ec (vkcluster-kafka-dpkafka-kafka, 160 брокеров, 4 ДЦ ec/hc/kc/pc) | 903a4dcc | 26.08 14:04, 16:34, 17:02; 27.08 07:54 | 4 фейла подряд, не сходится |
| ab0f4486 (logs-prod-rustore-kafka) | 6ba9881b | 26.08 14:56, 15:25 | 2 фейла, не сходится |
| 9fc47c1b (тестовый, test-modify3) | 18428dfe, 5998339c | 26.08 10:56, 13:58 | тесты симуляции падений |
| 6a5d52e1 | c85e3bc6 | 26.08 11:51 | сошлось ретраем 12:21 |

## Цепочка падения (единая для eba4c8ec и ab0f4486)

1. Родитель `upscaleKafkaControllerInCluster`: per-DC дети `<opId>_pc/_kc/_hc/_ec`
   завершились ещё в первом run'е (COMPLETED). На ретраях родителя их старт падает с
   `START_CHILD_WORKFLOW_EXECUTION_FAILED cause=WORKFLOW_ALREADY_EXISTS` — скипается
   через `runIgnoringAlreadyStarted` (это рабочий идемпотентный паттерн, НЕ баг).
2. Падает фаза reload: child `<opId>_update-broker-config` (тип `updateConfigKafkaBroker`).
3. Внутри: 160 per-host детей `reloadKafkaBrokerInstance` (workflowId
   `<opId>_update-broker-config_<dc>_<n>`). Прогон 27.08: **140 FAILED, 0 COMPLETED,
   20 START_FAILED (ALREADY_EXISTS)**.
4. Per-host ребёнок: activity `kafka_host_restartBrokerInstanceSsh` →
   `cloudService.sshExec(instance, "confp --oneshot && systemctl restart kafka-broker.service")`
   → корневая ошибка **`Invalid type of response received: class one.nio.http.Response !`**
   — известная транзиентная ошибка one-cloud proxy (см. jira-mdbsup-solver, «типовая
   транзиентная причина»). Обёртка: `CloudException("Unknown error while calling cloud")`
   (proxylib `CloudExceptionTranslator`).
5. `KafkaHostReloadHelper.assertReloadComplete` → non-retryable `RELOAD_FAILED`
   «Config reload failed for hosts: [все ~160 брокеров]» → родитель FAILED.

## Ключевая аномалия: activity падает БЕЗ ретраев

У `_update-broker-config_ec_5` (27.08): история 11 событий, ровно один
`ACTIVITY_TASK_SCHEDULED/STARTED/FAILED`, при этом
`retryState=RETRY_STATE_MAXIMUM_ATTEMPTS_REACHED` ⇒ эффективный maximumAttempts = 1.

А в репо `application.yaml` → `temporal.activity-options.kafka-activity-options.retry`:
initial 1s, backoff 2.0, max-interval 60s, **max-attempts: 8** (коммит 47cac8e5
MDBDEV-1531 «increase timeout and retries»). Расхождение: либо прод-конфиг переопределяет,
либо override в коде (проверяется — см. «Открытые вопросы»).

Прогон 27.08 07:54–08:16: упали **все 140 из 140** — в тот момент proxy лежал системно
(не единичные транзиенты); 8 ретраев с бэкоффом ~2 мин, вероятно, вытащили бы.

## Проблемы кода mdb-processing (кандидаты)

1. **Нет эффективных ретраев на `restartBrokerInstanceSsh`** (см. аномалию выше) —
   транзиентный фейл proxy сразу роняет per-host child.
2. **All-or-nothing**: один упавший хост → non-retryable `RELOAD_FAILED` всего
   `_update-broker-config` → падает вся операция; ретрай операции = полный повтор reload
   всех брокеров заново.
3. **Оптимистичный скип** `runIgnoringAlreadyStarted` в
   `AbstractKafkaControllerScaleWorkflow.reloadBrokers` (строка ~107): если предыдущий
   `_update-broker-config` ещё RUNNING, родитель считает reload «завершённым» и идёт в
   `saveUpscaledControllers` — операция может стать done, а reload потом упадёт.
4. Per-host child `ReloadInstanceKafkaBrokerWorkflow` сам без RetryPolicy
   (`RETRY_STATE_RETRY_POLICY_NOT_SET` у его фейла) — второй слой без ретраев.

## Код-референсы

- `UpscaleKafkaControllerInClusterWorkflowImpl` — родитель (фазы upsertPms → deploy → reload → save)
- `AbstractKafkaControllerScaleWorkflow.reloadBrokers/reloadControllers` — child-флоу reload
- `UpdateConfigKafkaBrokerWorkflowImpl` → `KafkaHostReloadHelper.executeReloadCycle/assertReloadComplete`
- `ReloadInstanceKafkaBrokerWorkflowImpl` — per-host: restartBrokerInstanceSsh + 2 waiter'а
- `KafkaHostActivityImpl.restartBrokerInstanceSsh` — sshExec `confp --oneshot && systemctl restart`
- `ActivityOptionsFactory` + `application.yaml` (`kafka-activity-options`) — retry-политики
- `ChildWorkflowUtils` — идемпотентные хелперы

## Грабли API, подтверждённые в этом разборе

- `runId` в `/workflows/{wid}/history?runId=` игнорируется → читаем только последний run;
  статусы старых run'ов — из listing-запроса по `WorkflowId = '<opId>'`.
- `/runs/{rid}/history` → 404. describe тоже игнорирует runId.
- `echo "$RESP" | jq` в zsh ломает JSON — только `curl -o файл`.
- Статус RUNNING в listing может протухнуть (run 07:54 показывался RUNNING, реально FAILED 08:16).

## Открытые вопросы

- [ ] Где теряются ретраи `kafka_host_restartBrokerInstanceSsh` в проде: внешний конфиг
      воркера, override бина, аннотации? (проверка по коду — в процессе)
- [ ] Та ли причина у фейлов 26.08 16:34/17:02 (старые run'ы недоступны через API) —
      можно косвенно по operations.error_message в прод-БД.
- [ ] Почему 20 per-host детей были ALREADY_EXISTS в прогоне 27.08 — остались живыми
      с прошлого run'а (16:34/17:02) или политика reuse.
