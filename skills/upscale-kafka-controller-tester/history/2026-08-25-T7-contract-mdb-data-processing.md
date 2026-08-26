# T7 Контракт mdb-data ↔ processing (2026-08-25)

**Результат: PASS — input контракта QueueInfo/controllersPerDc соответствует ожиданиям**

Проверка выполнена декодированием input двух workflow из T15-прогона
(op `6d84553c`, запуск через mdb-data API `POST …/hosts/controllers?dc=ic`,
mdb-data на branch MDBDEV-3180). Отдельный modify-запрос не потребовался —
все прогоны T1–T19 и так шли через mdb-data API.

## Parent `upscaleKafkaControllerInCluster` input

| Поле | Значение | Ожидание |
|---|---|---|
| `controllersPerDc` | `{dc:2, hc:2, kc:2, ic:2}` — абсолютные значения по всем ДЦ | OK |
| `queueInfo` | заполнен полностью: queueName, productId=7514, queueShortName, pmsHost, namespace=infra | OK |
| `brokerDcs` | `[dc, hc, kc]` — из хостов-брокеров (ic без брокеров → отсутствует) | OK |
| `hardwarePresetInputData` | alloc + volumesAlloc (NVME 10g) | OK |
| `workflowTtl` | 10800s = 3ч = DEFAULT_TTL (TTL не передавался) | OK |

## Child `upscaleKafkaControllerInDc` input (dc=ic)

- `dc`, `replicas: 2` (абсолютное), `queueInfo` проксирован без потерь.
- `sourceDc: ic`, `sourceControllerHost: 1.controller.test-modify3-mdbdev-kafka.ic.one-infra.ru` —
  источник для копирования конфигов указан корректно.
- `hardwarePresetInputData` идентичен parent.

## Декодирование

```bash
curl -s "http://localhost:8233/api/v1/namespaces/default/workflows/$WID/history?maximumPageSize=20" \
 | jq -r '.history.events[0].workflowExecutionStartedEventAttributes.input.payloads[0].data' \
 | base64 -d | jq .
```
