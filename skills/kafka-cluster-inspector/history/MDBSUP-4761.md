# MDBSUP-4761 — NotEnoughValidWindows на свежеподнятом CC: самоустранилось повтором remove_broker

**Дата**: 2026-08-21
**Кластер**: `vkmarket-events-p-adblogger-kafka` (KRaft, брокеры в hc/kc/pc)
**Хост**: `1.cruise.vkmarket-events-p-adblogger-kafka.hc.one-infra.ru`

## Симптом

Из тикета: при удалении брокера через CC `remove_broker` падает с
`NotEnoughValidWindowsException: There are only 0 valid windows when aggregating in range [-1, ...]`.
Контекст: 20.08 в 10:42 выполнен `create_additional_service` (поднятие CC), в 13:29 — `add_hosts`.

## Диагностика (порядок проверки)

| Что | Значение | Вердикт |
|---|---|---|
| `systemctl is-active cruise-control` | active, NRestarts=0, старт 20.08 10:44 | сервис жив, рестартов не было |
| `bootstrap.servers` в `cruisecontrol.properties` | 3 реальных брокера (hc/kc/pc), SASL_SSL | конфиг отрендерен — НЕ кейс MDBSUP-4739 (localhost) |
| `cruise-control.err.log` | 1× `OutOfMemoryError` в HTTP-Dispatcher в 20.08 15:12 | разовый, побочный эффект упавшего запроса, процесс выжил |
| `GET /state` (порт 8080) | `NumValidWindows: 5/5 (100%)`, `NumValidPartitions: 129/129`, `isProposalReady: true` | CC прогрелся, здоров |
| `GET /user_tasks` | см. timeline ниже | операция уже прошла успешно при повторе |

## Timeline из user_tasks (ключ к диагнозу)

- 20.08 13:34 — `POST add_broker?brokerid=23001` → Completed (это add_hosts из тикета)
- 20.08 15:12 — `POST remove_broker?brokerid=21001` → **CompletedWithError** (NotEnoughValidWindows)
- 21.08 10:49 — `POST remove_broker?brokerid=21001` повторён → **Completed**,
  в /state `recentlyRemovedBrokers: [21001]`

## Корень

`remove_broker` запустили до прогрева CC: сервис поднят 20.08 10:44, запрос в 15:12 —
меньше необходимых 5 окон по 5 мин валидных метрик (на момент запроса окон было 0).
CC корректно копил метрики (репортеры на брокерах работали), просто время не пришло.

## Фикс

Не потребовался — операцию повторили на следующий день, прошла успешно. Тикет закрыт
разбором без вмешательства.

## Грабли / уроки

1. **При `NotEnoughValidWindows` на свежеподнятом CC первым делом смотреть
   `GET /user_tasks`** (отфильтровать шум поллинга: `grep -vE 'GET /kafkacruisecontrol/state'`)
   и `/state` — возможно, повтор уже прошёл и копать конфиги не нужно.
2. **Не путать с MDBSUP-4739**: там CC вечно долбил `localhost:9092` (конфиги pms под
   неправильным hostname). Здесь конфиг валидный, окна просто не успели накопиться.
   Различие видно по `bootstrap.servers` + датам файлов в `/opt/cruise-control/config/`.
3. **OOM в `cruise-control.err.log` не всегда означает падение CC** — разовый
   `OutOfMemoryError` в HTTP-Dispatcher (унcaught в одном треде) может быть побочным
   эффектом упавшего REST-запроса. Проверять `NRestarts` и `ExecMainStartTimestamp`
   (`systemctl show cruise-control -p NRestarts -p ExecMainStartTimestamp`), прежде чем
   поднимать heap.
4. Ротации `cruise-control.out.*.log.gz` в течение дня — это log4j-ротация, не рестарты
   сервиса. Реальные рестарты — только через systemd.
5. Metric anomalies в `/state` по брокеру (BROKER_LOG_FLUSH_TIME_MS_999TH и т.п.) со
   статусом IGNORED — шум, действий не требует.
