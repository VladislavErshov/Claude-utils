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

## Ключевая аномалия: в проде действует СТАРАЯ retry-политика (deploy-override)

Фактическая политика из `activityTaskScheduledEventAttributes.retryPolicy` (проверено на
3 per-host детях 27.08): `initial 10s / backoff 2.0 / max-interval 120s / maximumAttempts 3`.

А в `src/main/resources/application.yaml` (`kafka-activity-options`) с 01.07 (47cac8e5,
MDBDEV-1531): `initial 1s / max-interval 60s / max-attempts 8`.

**Причина:** прод-конфиг рендерится из `deploy/mdb-processing/templates/etc/application.yaml.j2`
(внешний application.yaml перекрывает yaml из jar), и там секция `kafka-activity-options`
осталась со старыми значениями `10 / 2.0 / 120 / 3` (строки ~45-52). При MDBDEV-1531
deploy-шаблон не синхронизировали; при MDBDEV-3180 туда добавили новую очередь
`kafka-metadata-version-activity-options` (10/2.0/120/6 — совпадает с репо), а старый блок
не поправили — классический drift дублированного конфига (см. также ветку
`ershov/Fix-duplicate-from-prod-yaml` — чистка дублей в этом же j2).

Поправка к первичному разбору: попытки у activity БЫЛИ — 3 из 3 (финальный
`ACTIVITY_TASK_STARTED.attempt = 3`, затем `MAXIMUM_ATTEMPTS_REACHED`); UI-API сворачивает
промежуточные попытки в один Started. 3 попытки с бэкоффом 10s/20s (~2 мин) при системном
лежании proxy не спасли: 27.08 упали все 140 из 140 хостов по 3 попытки.

## Проблемы кода mdb-processing (кандидаты)

1. **Drift deploy-шаблона**: `deploy/.../application.yaml.j2` держит старые retry
   `kafka-activity-options` (3 попытки / 10s / 120s) — в проде действует она, а не 8 из
   репо-yaml. Фикс: синхронизировать блок или убрать дубль из j2.
2. **All-or-nothing**: один упавший хост → non-retryable `RELOAD_FAILED` всего
   `_update-broker-config` → падает вся операция; ретрай операции = полный повтор reload
   всех брокеров заново.
3. **Оптимистичный скип** `runIgnoringAlreadyStarted` в
   `AbstractKafkaControllerScaleWorkflow.reloadBrokers` (строка ~107): если предыдущий
   `_update-broker-config` ещё RUNNING, родитель считает reload «завершённым» и идёт в
   `saveUpscaledControllers` — операция может стать done, а reload потом упадёт.
4. Per-host child `ReloadInstanceKafkaBrokerWorkflow` сам без RetryPolicy
   (`RETRY_STATE_RETRY_POLICY_NOT_SET` у его фейла) — после исчерпания activity-ретраев
   (3) child падает сразу, слоя ретраев на уровне workflow нет.
5. По коду репо override retry для `restartBrokerInstanceSsh` НЕТ: интерфейс только
   `@ActivityMethod(name=...)`, без `@MethodRetry`; бин `kafkaActivityOptions` один
   (`ActivityOptionsFactory`); профилей нет. Override только deploy-j2 (п.1).

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

- [x] Где теряются ретраи `kafka_host_restartBrokerInstanceSsh` в проде → **найдено**:
      deploy-шаблон `application.yaml.j2` со старой политикой 3/10s/120s (закрыто 27.08).
- [ ] Та ли причина у фейлов 26.08 16:34/17:02 (старые run'ы недоступны через API) —
      можно косвенно по operations.error_message в прод-БД.
- [ ] Почему 20 per-host детей были ALREADY_EXISTS в прогоне 27.08 — остались живыми
      с прошлого run'а (16:34/17:02) или политика reuse.
- [ ] Фикс: синхронизировать `kafka-activity-options` в deploy j2 с репо-yaml
      (или убрать дубль); при необходимости поднять ретраи именно для reload-активностей
      (3 попытки ~2 мин мало при системном лежании proxy).
- [ ] Рассмотреть отказ от all-or-nothing в `KafkaHostReloadHelper.assertReloadComplete`
      (частичные успехи / повтор только упавших хостов при ретрае операции).

## Итог (ответ на «есть ли проблемы с кодом на нашей стороне»)

Да, есть — но триггер падений внешний (транзиентная/системная ошибка one-cloud proxy
`Invalid type of response received`), а не сходится операция из-за нас:

1. **Главное (P1): drift deploy-конфига** — `deploy/.../application.yaml.j2` держит
   старую retry-политику `kafka-activity-options` (3 попытки, 10s/120s) и перекрывает
   репо-yaml (8/1s/60s с 01.07). Из-за этого транзиентные фейлы proxy не выживаются.
   Фикс дешёвый: поправить/дедуплицировать блок в j2.
2. **P2: all-or-nothing reload** — один хост из 160 роняет всю операцию; ретрай = полный
   повтор reload. Вместе с P1 даёт 4 подряд фейла на eba4c8ec и 2 на ab0f4486.
3. **P3: оптимистичный скип `runIgnoringAlreadyStarted`** — риск закрыть операцию как
   done при ещё бегущем (и потенциально падающем) reload.

Ответ на исходный вопрос про override retry: в java-коде override нет, фактическая
политика задаётся deploy-шаблоном j2 (подтверждено `retryPolicy` из
`activityTaskScheduledEventAttributes` прод-истории).
