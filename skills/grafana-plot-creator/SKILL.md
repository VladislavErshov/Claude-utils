---
name: grafana-plot-creator
description: Создание и правка графиков/панелей в Grafana dashboard JSON для MDB Kafka-дашборда (и аналогичных VictoriaMetrics-дашбордах). Правила размещения панелей (gridPos, collapsed row vs top-level), паттерны multi-target таблиц (merge/organize/joinByField), PromQL с переменными $cluster/$instance/$consumer_group, форматирование descriptions на русском (\n\n для абзацев, без описания расчёта). В папке dashboard/ — две версии: stable и test. Используй когда нужно добавить новую панель, поправить PromQL/transformations/description, или разобраться почему панель отображается не в той секции.
allowed-tools: [bash, read_file, write_file, edit_file]
---

# Скилл создания Grafana-графиков

Скилл для правки Grafana dashboard JSON. Боевой дашборд — MDB Kafka, источник данных VictoriaMetrics.

## Базовые принципы

1. **Пользователь правит позиции в UI Grafana, не через JSON.** При добавлении новой панели ставить любой разумный `gridPos` (например `y=0, x=0, h=8, w=24`), не вычислять координаты аккуратно и не пересчитывать y соседей. Пользователь двигает мышкой в UI и сохраняет обратно в файл.
2. **Descriptions на русском.** Переносы строк — двойной `\n\n` (одиночный `\n` Grafana не рендерит, склеивает в одну строку). Без описания как метрика считается (формулы, `60 × rate(...[$__rate_interval])` и т.п. — не писать).
3. **Datasource** — `victoriametrics-datasource` с uid `${vm_datasource}`.
4. **Переменные дашборда**: `$cluster` (mdb_kafka_cluster), `$instance` (instance regex), `$consumer_group` (group regex). Использовать во всех PromQL.

## Структура скилла

- `SKILL.md` — этот файл.
- `commands/panel_placement.md` — правила `gridPos`, collapsed row vs top-level, типичные ошибки.
- `commands/table_panel_patterns.md` — паттерны multi-target таблиц: merge+organize, joinByField, labelsToFields.
- `commands/promql_patterns.md` — PromQL-рецепты для Kafka-метрик (per-group агрегации, group_left, rate в минуту).
- `commands/description_style.md` — стиль descriptions на русском.
- `dashboard/dashboard-stable.json` — **stable**-версия дашборда (бой, импортирована в Grafana prod).
- `dashboard/dashboard-test.json` — **test**-версия (черновик, новые итерации сюда).

Готовые патроны панелей брать прямо из `dashboard-test.json` — там лежат рабочие примеры stat / timeseries / table-merge / table-joinByField. Ищать через `id` или `title`.

## Workflow с двумя версиями dashboard

1. **Правки делать в `dashboard/dashboard-test.json`** — это черновик.
2. Пользователь импортирует `dashboard-test.json` в Grafana UI, проверяет, двигает панели, сохраняет обратно в файл (пользователь обновит файл сам — попросить его об этом).
3. Когда тестовая версия проверена и стабильна — копируем `dashboard-test.json` → `dashboard-stable.json`:
   ```bash
   cp dashboard/dashboard-test.json dashboard/dashboard-stable.json
   ```
4. Stable-версия не трогать без явного разрешения пользователя.

Если пользователь дал свежий экспорт из Grafana (например `/Users/vl.ershov/Downloads/grafana2.txt`) — это новая test-версия, скопировать в `dashboard/dashboard-test.json`:
```bash
cp /Users/vl.ershov/Downloads/grafana2.txt dashboard/dashboard-test.json
```

## Шаблоны панелей — брать из dashboard-test.json

Готовые рабочие паттерны лежат прямо в `dashboard/dashboard-test.json`. Ищать через `id` или `title`:

| Что искать | Пример панели |
|---|---|
| Простая stat-панель (одна метрика, большое число) | `id=5` "Brokers Online" |
| Time series с throughput в минуту (`60 * rate(...)`) | `id=147` "Record Acknowledgements Per Minute" |
| Таблица multi-target с одинаковыми labels (`merge + organize`) | `id=2` "Brokers info" |
| Таблица multi-target с разными labels (`joinByField + organize`) | `id=146` "Share Group State" |

Как использовать:
1. Найти нужную панель в `dashboard-test.json` через `id`.
2. Скопировать её JSON, поменять `title`, `description`, `expr` (PromQL) в target'ах.
3. Поправить `renameByName` / `indexByName` в organize под свои колонки.
4. Вставить в дашборд (top-level или внутрь row.panels — см. `commands/panel_placement.md`).
5. Поставить любой `gridPos` — пользователь подвинет в UI.

## Быстрый старт — добавить новую панель

1. Прочитать `commands/panel_placement.md` — куда класть панель (top-level vs row.panels).
2. Посмотреть подходящий шаблон в `examples/` (stat / timeseries / table-merge / table-joinByField).
3. Прочитать `commands/promql_patterns.md` — PromQL с правильными переменными и агрегацией.
4. Сгенерировать JSON панели: скопировать шаблон, поменять title/description/expr, поставить любой `gridPos`.
5. Вставить в `dashboard/dashboard-test.json` (top-level или внутрь row.panels).
6. Сказать пользователю: «позицию подвинешь в UI, потом сохрани экспорт обратно».

## Типичные ошибки (НЕ делать)

- Ставить панель в top-level когда нужно в `row.panels` (или наоборот) — см. `panel_placement.md`.
- Пересчитывать `gridPos.y` у соседних панелей чтобы «освободить место» — пользователь делает это в UI.
- Использовать одиночный `\n` в `description` — Grafana его склеит. Только `\n\n` для разделения абзацев, либо одна строка.
- Писать в description как метрика считается (`rate(...[$__rate_interval])`, `sum by`, `group_left`) — это детали реализации, не описание.
- Использовать `format=time_series` для таблиц — брать `format=table` + `instant=true`.
- Забывать `legendFormat: "{{group}}"` (или нужный label) — без него Grafana показывает сырые метрики.
- Править `dashboard-stable.json` без явного разрешения.

## Что НЕ покрывает скилл

- Создание dashboard с нуля (через API/Grafana provisioning) — только правка существующего JSON.
- Настройка переменных дашборда (`$cluster`, `$instance`) — они уже есть в боевых дашбордах.
- Alerting rules — это отдельная тема, тут не описано.
- Plugin-specific options (Geomap, NodeGraph и т.п.) — только stat/graph/table/heatmap.
