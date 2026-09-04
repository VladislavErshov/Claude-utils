# Конфиги и шаблоны запуска тестов (серия 2026-08-24/25, кластер test-modify3)

## Общие параметры

- cluster_id: `9fc47c1b-011d-4aaa-b411-de5345a0204e`, project 160, namespace infra
- mdb-data: `http://localhost:8081`, temporal UI API: `http://localhost:8233/api/v1/namespaces/default`
- PMS-чтение: `~/.claude/skills/pms-worker/bin/pms-read.sh 1.broker.test-modify3-mdbdev-kafka.dc.one-infra.ru <key>`
  (кворум читать по BROKER-ключу, не controller!)

## Штатный запуск операции (mdb-data)

```bash
# upscale (+1 контроллер в ДЦ)
curl -s -X POST 'http://localhost:8081/api/v2/mdb/kafka/clusters/9fc47c1b-011d-4aaa-b411-de5345a0204e/hosts/controllers?dc=hc'
# downscale (-1 контроллер из ДЦ)
curl -s -X DELETE 'http://localhost:8081/api/v2/mdb/kafka/clusters/9fc47c1b-011d-4aaa-b411-de5345a0204e/hosts/controllers?dc=hc'
```

## Служебные SQL (локальная БД pg_backstage_plugin_mdb)

```sql
-- разблокировать 409 «Already has active or failed operation»
UPDATE operations SET status='done'
WHERE cluster_id='9fc47c1b-011d-4aaa-b411-de5345a0204e' AND status IN ('in_progress','failed');

-- симуляция «save не выполнился» (облако опережает host_state) — T14
DELETE FROM host_state WHERE host='2.controller.test-modify3-mdbdev-kafka.kc.one-infra.ru';

-- контроллеры по ДЦ
SELECT host FROM host_state WHERE cluster_id='9fc47c1b-011d-4aaa-b411-de5345a0204e'
AND host LIKE '%controller%' ORDER BY host;
```

## Terminate workflow (UI API, DELETE и temporal-CLI не работают)

```bash
API=http://localhost:8233; CJ=/tmp/cookies.txt
curl -s -c $CJ "$API/api/v1/namespaces/default/workflows?pageSize=1" -o /dev/null
TOK=$(grep _csrf $CJ | awk '{print $NF}')
curl -s -b $CJ -X POST "$API/api/v1/namespaces/default/workflows/<WID>/terminate" \
  -H "X-Csrf-Token: $TOK" -H 'Content-Type: application/json' -d '{"reason":"test"}'
```

## Прямой запуск workflow (input в base64 json/plain, TTL строкой!)

- taskQueue ВСЕГДА `kafka-activities-queue` (и для workflow, и для activity)
- workflowExecutionTimeout: строка `"10800s"` (объект-Duration → protojson syntax error)
- Шаблоны: `direct-start-upscaleKafkaControllerInCluster.json` (T13),
  `direct-start-downscaleKafkaControllerInCluster.json` (T10 cleanup)
- Прямой запуск не закрывает operations (in_progress навсегда) — только для негативных/cleanup.
- Input для нового прогона: скопировать из history первого рана
  (`.history.events[0].workflowExecutionStartedEventAttributes.input.payloads[0].data`).

## Декодирование input/output

```bash
curl -s "http://localhost:8233/api/v1/namespaces/default/workflows/$WID/history?maximumPageSize=10" | \
  jq -r '.history.events[0].workflowExecutionStartedEventAttributes.input.payloads[0].data' | base64 -d | jq
```

## Состояние инстансов облака (из activity-результата cloud_getInfoForInstances)

```bash
jq -r '.history.events[] | select(.eventType=="EVENT_TYPE_ACTIVITY_TASK_COMPLETED") |
  .activityTaskCompletedEventAttributes.result.payloads[0].data' /tmp/hist.json | tail -1 | base64 -d | jq
```

## PIDs инфраструктуры (текущая сессия)

- mdb-data 8081: gradle bootRun в ~/Documents/Git/mdb-data, profile local
- mdb-processing 8080: gradle bootRun в ~/Documents/Git/mdb-processing, profile local (лог /tmp/mdb-processing.log)
- temporal 8233: docker-compose localrun/
