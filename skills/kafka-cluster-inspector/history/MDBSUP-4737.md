# MDBSUP-4737 — «медленный reassignment из-за троттлинга CC» оказался num.replica.fetchers=1

**Дата**: 2026-08-21
**Кластер**: `auction-realtime-adtech-kafka` (KRaft, брокеры в hc/kc/pc/uc, 8 брокеров, CC в hc)
**Тикет**: remove broker kc-брокеров «застрял», ~70 Мбит/с на брокер; тяжёлый топик
`bannerd_potential_info` 3–4 Гбит/с на запись. Пользователь предполагал троттлинг CC
(replicationThrottleMb=2000 — потолок CC).

## Симптом

Операция remove broker не завершается, URP на брокерах hc/pc, at-min-ISR на uc.
Со стороны выглядит как «CC агрессивно троттлит reassignment».

## Диагностика — как отличить троттлинг CC от fetcher-бутылочного-горлышка

| Проверка | Команда | Результат здесь |
|---|---|---|
| Динамический throttle от CC | `kafka-configs.sh --describe --entity-type brokers --all \| grep throttle` | пусто — CC почистил после таска |
| Активный reassignment | `kafka-reassign-partitions.sh --list` (grep `^\s*Reassignment`) | пусто |
| CC executor | `GET /state` → ExecutorState | `NO_TASK_IN_PROGRESS` |
| Освобождены ли выводимые брокеры | `GET /kafka_cluster_state` (список brokers: leaders/replicas) | kc 21001/21002 = 0/0 — перемещение ЗАВЕРШЕНО |
| Fetcher-метрики на живом брокере | `:8080/metrics` → `replicafetchermanager_maxlag/minfetchrate`, `fetcherlagmetrics_consumerlag` | maxlag ~2.9M, весь лаг на `replicafetcherthread_0` от одного лидера, minfetchrate 1.4/s |

Ключ: **URP без активного reassignment = хронический replication lag**, а не медленный
reassignment. Лаг весь на одном fetcher-треде → `num.replica.fetchers=1` (дефолт).
Второй подтверждённый кейс после MDBSUP-4649.

## Фикс

В pms `kafka.broker.properties` → `num.replica.fetchers=4`. Правка делается через UI MDB
в настройках брокера — дальше все действия (применение конфига, рестарты брокеров)
выполняет оркестратор mdb автоматически, ручные рестарты не нужны.

## Грабли / уроки

1. **«Медленный reassignment» в тикете ≠ идущий reassignment** — сначала `--list` и
   ExecutorState CC: возможно, перемещение давно завершилось, а URP — хронический lag.
2. `mcc ... ops <uuid>` может отдать `EntityNotFoundException: Partition ... is not managed
   by both one-cloud-ops and ops-temporal` — кластер под другим механизмом, не блокер.
3. Вывод `kafka-reassign-partitions.sh --list` / `kafka-topics.sh` тонет в INFO-логах
   AdminClient — grep по `^\s*Reassignment` и `^\s*Topic:` (табуляция!).
4. `kafka_cluster_state` CC — быстрый способ увидеть, освобождены ли выводимые брокеры
   (0 leaders / 0 replicas), без перебора брокеров.
5. Массовые `reported_progress` warnings про subnets (36497 subnets) — шум инфраструктуры,
   к проблеме отношения не имеют.
