# Очереди one-cloud: диагностика и создание

Контекст: очередь кластера MDB — `<cluster>-<project>-<type>.<project>.db.<env>.mdb.<root>`
(пример `otvet-dev-otvet-kafka.otvet.db.dev.mdb.prod`); родительская очередь проекта —
`<project>.db.<env>.mdb.<root>`. При добавлении хостов в новый ДЦ mdb-processing создаёт
кластерную очередь через `submitQueueIfNeeded`, но **родительскую очередь проекта не
создаёт** — если её нет, мастер отвечает
`404: Parent queue <project>.db.<env>.mdb.prod not found`, операция падает
`Controller upscale failed in 1 DC(s): [<dc>]` (MDBSUP-5508).

## Диагностика

```bash
# есть ли очередь на мастере ДЦ и в каком она состоянии
mcc --local -n infra -c <dc> queues "%otvet%" -f json | jq -c '.[] | {name, state, meta}'

# аудит: кто и как создавал/менял очередь (и каким манифестом)
mcc --local -n infra -c <dc> audit "<queue>" | tail
```

Здоровое состояние родительской очереди — `state: RUNNING` + `meta.product: "<id проекта>"`.
Промежуточные узлы (`prod`, `mdb.prod`, `dev.mdb.prod`, `db.dev.mdb.prod`) в новых ДЦ уже
существуют — не хватает именно листа `<project>.db.<env>.mdb.<root>`.
Сверять с ДЦ, где очередь есть (`-c hc`): `mcc --local -n infra -c hc manifest "<queue>" -f json`.

## Создание: только манифест с state RUNNING, НЕ addqueue

```bash
# 1. манифест (product id проекта — из очередей того же проекта в других ДЦ или `mcc queue`)
cat > /tmp/queue.yaml <<'EOF'
{"type":"queue","namespace":"infra","name":"otvet.db.dev.mdb.prod","state":"RUNNING","meta":{"product":"4811"}}
EOF

# 2. создать
mcc --local -n infra -c <dc> submit -t queue /tmp/queue.yaml -f json

# 3. проверить
mcc --local -n infra -c <dc> queue otvet.db.dev.mdb.prod -f json
```

## Грабли

- **`addqueue` создаёт очередь в STOPPED** и product-меты не ставит. Поднять её потом
  нельзя: `mcc submit -t queue` с `state: RUNNING` по существующей очереди **state не
  меняет** (update игнорирует state), `mcc start` работает только для сервисов/инстансов
  (`Neither service or instance could be found`). Лечится только удалением и пересозданием.
- **Пересоздание**: `mcc --local -n infra -c <dc> --auto_solve withdraw --type queue
  "<queue>"` — требует уравнения-подтверждения (`--auto_solve` решает сам; допустимо
  только для пустой очереди: «Confirm auto withdraw 0 config(s)…»). Затем сразу submit
  манифеста с `state: RUNNING`.
- `state` в манифесте применяется **только при создании** — поэтому рабочий путь
  «withdraw → submit с RUNNING» (так создавались очереди других проектов, видно по audit:
  «Queue created with manifest …"state":"RUNNING"…»).
- Кластерную очередь руками не создавать — её при ретрае операции создаёт
  mdb-processing (`submitQueueIfNeeded`).
- Ретрай упавшей операции — кнопкой «Повторить» в UI mdb-data; отработает только
  недостающий ДЦ.
