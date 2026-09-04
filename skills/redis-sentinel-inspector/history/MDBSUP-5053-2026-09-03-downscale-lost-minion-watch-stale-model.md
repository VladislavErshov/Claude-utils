# MDBSUP-5053 — allure-staging: LOST_MINION-мастер kc + зависший downscale + протухшая модель watch-таски

Дата: 2026-09-03. Кластер Redis Sentinel `allure-staging`
(`f65db824-a42d-4258-bf93-6061a9df9c39`, очередь `allure-staging-allure-redis.allure.db.dev.mdb.batch`).
Парный кейс prod `ce8107c8…` — идентичный delete_hosts, сошёлся сам после рестарта
оператора 02.09 (get_result вернул успех в 14:30, флоу доделал host_state/cleanup к 14:32).
Отсюда вывод: **зависание get_result redis-sentinel downscale лечится рестартом оператора,
если задача в облаке реально дошла до конца** — сначала проверить очередь оператора.

## Суть

delete_hosts (hc, вывоз из HC по CDT-817) упал на `get_result_redis_sentinel_downscale_operator`
23.07. К 03.09 в облаке hc уже не было (сервис withdrawn, storage отсутствует —
`EntityNotFoundException: Storage … not found`, флаг `storageWithdrawn: false` у задачи был
протухший). Остатки: призрак hc в `host_state`, failed-операция, висящая задача
`downscale-instances` (waiting for precondition) и статус UNAVAILABLE.

Отдельно: хост **kc** — `FINISHED/LOST_MINION` (минион srvk5140 перестал рапортовать),
но **VM жива и была мастером** (реплики ec/pc с `master-link ok`). Оператор по этому
поводу: `Sentinel wrong info in host: 1.db...kc...` → Cluster is UNAVAILABLE.
**Это НЕ баг MDBDEV-1418** — runtime-взгляды sentinel'ов были чистые (все пиры по
hostname, без ip-дублей). «Wrong info» = оператор не может сматчить живой sentinel
kc со своей облачной моделью (инстанс FINISHED). Диагностикаkc — только с живых
соседей (`redis-cli -h <kc> -p 26379` с ec), sshexec на LOST_MINION-хост невозможен
(`is not scheduling on a minion`), `sshexec` требует `-c <dc>` облака хоста.

## Что сделано (порядок важен)

1. **SQL (зеркало сошедшегося prod-флоу)**: `DELETE host_state` hc-призрака; таски
   678983–678986 (get_result failed + 3 scheduled) → `done` с result как у prod'а
   (`{"data": {}}`, у finish_task `{"data": true}`); operations → `done`.
2. **`sentinel failover allure-staging`** с живого sentinel — мастер kc → ec, kc стал
   репликой (`master_link_status: up`).
3. **`mcc migrate <uuid1>,<uuid2>` дисков kc — НЕВОЗМОЖЕН**:
   `ServiceValidationException: No devices found to make MIGRATING` — данные копировать
   с мёртвого минионa неоткуда (см. mcc-host-worker query.md, LOST_MINION).
4. **Дроп дисков + передеплой**: `mcc -c kc delete <uuids>` (уравнение через pexpect) →
   `mcc -c kc start "db.allure-staging-allure-redis"` → инстанс RUNNING на новом минионе
   (srvk3492), реплика online у мастера, lag=0.
5. **`op_stop downscale-instances`** — алерт «waiting for precondition» ушёл.
6. Watch продолжал `PARTIALLY_AVAILABLE / Unavailable hosts: kc` при полностью
   здоровом redis-слое — **протухшая модель watch-таски**. Лечение:
   `op_stop "queue://…" redis-sentinel.watch` + `op_start …` — модель пересобралась,
   `Cluster is AVAILABLE`. Рестарт watch-таски (op_stop/op_start) — рабочий приём
   сброса кэша доступности без рестарта всего оператора.
7. `purge <queue>/db all` в kc и hc — «none to purge».

## Грабли

- `redis-cli -p 26379` (sentinel-порт) не отвечает на `info replication` — там только
  sentinel-команды; пустой вывод ≠ мёртвый redis.
- После op_stop watch-таски её статус в `mcc ops` может ещё минуту держать старый
  алерт — проверять после op_start + refresh-интервала.
- `availability: RESERVED` с `availability_details: 'node is: slave, connected slaves: 0'`
  — норма для реплик (и ec-мастер тоже RESERVED) — на этот чек оператор не опирается.
- LOST_MINION: `mcc instances` по FQDN (state/outcome/minion), а не `mcc status`.
