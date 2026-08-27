---
name: temporal-worker
description: Чтение состояний флоу (workflow) сервиса mdb-processing в прод-Temporal и агрегация ошибок. Используй, когда нужно найти операции/workflow по кластеру, типу операции или operationId, посмотреть цепочку parent → child → activity, достать причины падений (failure-цепочки, retryState, число реальных попыток), собрать статистику фейлов по типу workflow или кластеру. Триггеры — «посмотри в темпорале», «история операций», «почему упал workflow», «агрегируй ошибки флоу».
allowed-tools: [bash]
---

# temporal-worker: состояния флоу mdb-processing в прод-Temporal

Прод-Temporal: `https://mdb-processing-temporal.common.mdb.one-infra.ru` (UI локально не
открывается — только API, без авторизации). Namespace — `default`.

## 1. Поиск workflow'ов

```bash
BASE="https://mdb-processing-temporal.common.mdb.one-infra.ru/api/v1/namespaces/default"
# по типу workflow (query обязательно через --data-urlencode!)
curl -s --get "$BASE/workflows" --data-urlencode "query=WorkflowType = 'upscaleKafkaControllerInCluster'" | jq ...
# по операции (workflowId = operationId)
curl -s --get "$BASE/workflows" --data-urlencode "query=WorkflowId = '<operationId>'"
# по кластеру (не все workflow пишут ClusterId в search attributes!)
curl -s --get "$BASE/workflows" --data-urlencode "query=ClusterId = '<cluster_id>'"
```

Компактная таблица запусков (время / статус / runId / clusterId):

```bash
curl -s --get "$BASE/workflows" --data-urlencode "query=WorkflowType = '<type>'" \
| jq -r '.executions | sort_by(.startTime) | .[] | [.startTime, .status, .execution.workflowId, .execution.runId,
    ((.searchAttributes.indexedFields.ClusterId.data // "") | @base64d | fromjson? // "-")] | @tsv'
```

## 2. История workflow

```bash
curl -s --get "$BASE/workflows/<wid>/history" --data-urlencode "maximumPageSize=250" -o /tmp/h1.json
```

⚠️ **Грабли API (проверено 2026-08-27):**

- Путь `/workflows/{wid}/runs/{runId}/history` → `404 Not Found`. Правильный путь —
  `/workflows/{wid}/history?runId=<run>`.
- **Параметр `runId` ИГНОРИРУЕТСЯ** (в любом написании: runId/runID/run/executionRunId) —
  всегда отдаётся история **последнего** run'а данного workflowId. Старые run'ы того же
  workflowId через UI API недоступны; их статусы видны только в listing-запросе из п.1.
- Пагинация: `nextPageToken` — передавать целиком (не обрезать!), крутить цикл до пустого.
- zsh: НЕ делать `echo "$RESP" | jq` — echo портит JSON (escape-последовательности).
  Только `curl -o файл` → `jq файл`.
- `describe` (`GET /workflows/{wid}?runId=`) тоже игнорирует runId.

## 3. Извлечение ошибок

**Failure-цепочка** (все вложенные причины, сверху-вниз):

```bash
jq -r '.history.events[] | select(.workflowExecutionFailedEventAttributes != null)
  | .workflowExecutionFailedEventAttributes | recurse(.cause? // empty) | .message' /tmp/h1.json
```

Аналогично: `childWorkflowExecutionFailedEventAttributes` (упавший child),
`activityTaskFailedEventAttributes` (упавшая activity). У child/workflow-фейлов полезен
`stackTrace` — там виден наш Java-класс и строка.

**Реальные попытки activity:**

- `retryState` в `activityTaskFailedEventAttributes`:
  `RETRY_STATE_MAXIMUM_ATTEMPTS_REACHED` / `RETRY_STATE_NON_RETRYABLE_FAILURE_TYPE` / `RETRY_STATE_TIMEOUT`;
- число попыток = число событий `ACTIVITY_TASK_STARTED` между SCHEDULED и финальным FAILED
  (каждая попытка — отдельный Started; если Started один, ретраев не было);
- `failure.applicationFailureInfo.type` — Java-класс исключения,
  `.nonRetryable: true` — флаг non-retryable.

**START_CHILD_WORKFLOW_EXECUTION_FAILED cause=WORKFLOW_ALREADY_EXISTS** — это наш паттерн
ретрай-идемпотентности (`ChildWorkflowUtils.runIgnoringAlreadyStarted`): предыдущий run
уже создал child с этим workflowId. Сам по себе не ошибка; важно, что родитель после
скипа НЕ ждёт результата уже существующего child'а.

## 4. Агрегация ошибок

Сводка по run'ам типа workflow (статусы, времена, история — только последние run'ы):

```bash
for wid in $(curl -s --get "$BASE/workflows" --data-urlencode "query=WorkflowType = '<type>'" \
    | jq -r '.executions[].execution.workflowId' | sort -u); do
  # для каждого workflowId: все run'ы (статусы) + failure последнего run'а
done
```

Полезные срезы: доля FAILED по типу; у упавших — группировка фейл-сообщений по
`failure.cause...message` (нижний уровень цепочки = корневая причина); по кластерам
(ClusterId из search attributes, base64-decode).

## 5. Связка с кодом mdb-processing

Workflow-типы маппятся на классы `*WorkflowImpl` в
`mdb-processing/src/main/java/one/cloud/mdb/processing/<domain>/workflow/`.
Retry-политики activity: бины в `ActivityOptionsFactory` из
`src/main/resources/application.yaml` (`temporal.activity-options.<queue>.retry`).
После разбора сверять stackTrace из фейла с кодом.

## История разборов

Каждый разбор — файл `history/<date>-<тема>.md`: тип workflow, затронутые кластеры/операции,
цепочка падения с eventId, корневая причина, найденные проблемы кода, открытые вопросы.
Перед новым разбором смотреть `history/`.
