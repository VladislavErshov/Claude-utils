# Паттерны таблиц в Grafana

Как строить таблицы из одного или нескольких PromQL-запросов. Боевые примеры — из `dashboard/dashboard-test.json`.

## Базовые настройки target'а для таблицы

```json
{
  "datasource": {"type": "victoriametrics-datasource", "uid": "${vm_datasource}"},
  "editorMode": "code",
  "exemplar": false,
  "expr": "<PromQL>",
  "format": "table",
  "instant": true,
  "interval": "1m",
  "legendFormat": "{{group}}",
  "range": false,
  "refId": "A"
}
```

Ключевое:
- `format: "table"` — Prometheus вернёт таблицу где каждая метрика — колонка, labels — тоже колонки.
- `instant: true`, `range: false` — нужны текущие значения, не временной ряд.
- `editorMode: "code"` — писать PromQL вручную, не строить через визуальный билдер.
- `exemplar: false` — не нужны exemplar'ы (ссылки на трейсы), это для graphs.
- `legendFormat: "{{group}}"` (или другой label) — без него в легенде мусор.

## Паттерн 1: Один target, labels → колонки

Использовать когда: одна метрика, нужны колонки из labels + Value.

```json
"transformations": [
  {"id": "labelsToFields", "options": {"mode": "columns"}},
  {
    "id": "organize",
    "options": {
      "excludeByName": {"Time": true},
      "indexByName": {"group": 0, "state": 1, "Value": 2},
      "renameByName": {"group": "Group", "state": "State", "Value": "Members"}
    }
  }
]
```

`labelsToFields` превращает labels в колонки. `organize` — переименовывает, сортирует, исключает `Time`.

## Паттерн 2: Multi-target, одинаковые labels — merge

Использовать когда: несколько метрик с **одинаковым** набором labels (например, все агрегированы `by (instance)`). Пример — панель "Brokers info" (id=2).

```json
"targets": [
  {"refId": "A", "expr": "time() - jvm_info{...} * on (instance) group_left process_start_time_seconds{...}", "format": "table", "instant": true},
  {"refId": "B", "expr": "sum(kafka_controller_kafkacontroller_activecontrollercount{...}) by (instance)", "format": "table", "instant": true},
  {"refId": "C", "expr": "sum(kafka_server_replicamanager_partitioncount{...}) by (instance)", "format": "table", "instant": true}
],
"transformations": [
  {"id": "merge", "options": {}},
  {
    "id": "organize",
    "options": {
      "excludeByName": {"Time": true, "cloud_instance": true, "host": true, "job": true},
      "renameByName": {"Value #A": "Uptime", "Value #B": "Active Controllers", "Value #C": "Partitions"}
    }
  }
]
```

`merge` объединяет все frame'ы в один. Колонки Value автоматически получают суффикс ` #<refId>`: `Value #A`, `Value #B`, `Value #C`.

## Паттерн 3: Multi-target, разные labels — joinByField

Использовать когда: метрики с **разными** labels, нужно объединить по одному общему полю. Пример — "Share Group State" (id=146): target A имеет labels (group, state, coordinator), target B только (group), target C только (group). Объединяем по `group`.

```json
"targets": [
  {"refId": "A", "expr": "max by (group, state, coordinator) (kafka_share_group_state{...} * 0 + 1)", "format": "table", "instant": true},
  {"refId": "B", "expr": "max by (group) (kafka_share_group_members{...})", "format": "table", "instant": true},
  {"refId": "C", "expr": "sum by (group) (kafka_server_sharepartitionmetrics_in_flight_message_count{...})", "format": "table", "instant": true}
],
"transformations": [
  {"id": "joinByField", "options": {"byField": "group", "mode": "outer"}},
  {
    "id": "organize",
    "options": {
      "excludeByName": {"Time": true, "Value #A": true},
      "indexByName": {"group": 0, "state": 1, "coordinator": 2, "Value #B": 3, "Value #C": 4},
      "renameByName": {
        "group": "Group",
        "state": "State",
        "coordinator": "Coordinator",
        "Value #B": "Members",
        "Value #C": "In-flight messages"
      }
    }
  }
]
```

`joinByField` делает outer join по полю `group`. Колонки Value получают суффиксы ` #A`, ` #B`, ` #C`. В organize исключаем `Value #A` (это просто флаг=1 от target A, не нужен) и `Time`.

## Колонки Value после merge/joinByField

Имена колонок Value:
- После `merge`: `Value #A`, `Value #B`, `Value #C` (где A/B/C — `refId` target'ов).
- После `joinByField`: то же самое, `Value #A`, `Value #B`, ...

В `renameByName` указываем эти имена явно.

## Переименование и сортировка колонок

```json
"organize": {
  "excludeByName": {"Time": true},
  "indexByName": {"group": 0, "state": 1, "Value #B": 2},
  "renameByName": {"group": "Group", "Value #B": "Members"}
}
```

- `excludeByName: {"Time": true}` — скрыть колонку Time (всегда есть в table format).
- `indexByName` — порядок колонок (0, 1, 2...).
- `renameByName` — переименование.

## Цветовое выделение ячеек

Через `fieldConfig.overrides`:

```json
"overrides": [
  {
    "matcher": {"id": "byName", "options": "State"},
    "properties": [
      {
        "id": "mappings",
        "value": [{
          "options": {
            "Stable": {"color": "green"},
            "Dead": {"color": "red"},
            "Empty": {"color": "yellow"},
            "PreparingRebalance": {"color": "orange"}
          }
        }]
      },
      {"id": "custom.width", "value": 180}
    ]
  },
  {
    "matcher": {"id": "byName", "options": "Members"},
    "properties": [{"id": "custom.width", "value": 100}]
  }
]
```

## Тип панели

`"type": "table"` для таблиц.

Опции таблицы:
```json
"options": {
  "cellHeight": "md",
  "footer": {"show": false, "countRows": false, "enablePagination": true},
  "frameIndex": 0,
  "showHeader": true,
  "sortBy": []
}
```

## Ловушки

1. **Забыл `format: "table"`** — Grafana вернёт time series, таблица будет пустая или кривая.
2. **Забыл `instant: true`** — то же самое.
3. **Несколько target'ов с разными labels, используешь `merge` вместо `joinByField`** — получишь не склеенную таблицу, а кучу строк с пустыми ячейками (каждый frame отдельно).
4. **В `renameByName` пишешь `"Value"` вместо `"Value #B"`** — не сработает, после merge/joinByField у колонок суффикс.
5. **Хочешь одну строку на group, но `legendFormat` пустой** — Grafana может выдать несколько рядов; всегда ставь `legendFormat: "{{<label>}}"`.
