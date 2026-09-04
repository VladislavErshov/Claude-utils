# `mcc ops` / `op_start` / `op_stop` — работа с оператором one-cloud-ops

```bash
mcc --local -n <namespace> -c <dc> ops <cluster-id-or-name>          # статус партиции оператора
mcc --local -n <namespace> -c <dc> op_stop <partition> <operator.task>   # остановить задачу
mcc --local -n <namespace> -c <dc> op_start <partition> <operator.task> -- <args>  # запустить задачу
```

Без `-n <namespace>` падает `NamespaceMissingException`. Kafka-кластеры: partition =
`queue://<fullQueue>` (например `queue://workspace-mail-kafka.mail.db.production.mdb.prod`,
fullQueue из `one_cloud_meta.params`); задача = `kafka.downscale-broker` и т.п.

## Чтение статуса (`ops -f json`)

Структура: `alerts` (map текстов), `operators.kafka.tasks` (активные задачи; когда задача
исчезает из tasks — она завершена), `operators.kafka.taskinfos` (все известные задачи
оператора), `persisted` (размеры состояния). Ключевые поля:
- `alerts["kafka.watch[refreshAvailabilityState]"]` — availability по модели оператора.
  ⚠️ Модель может протухать: ghost-хосты удалённых нод дают «Cluster is UNAVAILABLE /
  No one primary» при реально живом кластере. Сверять с Jolokia на хосте:
  `curl -s http://localhost:7777/jolokia/read/kafka.server:type=raft-metrics/current-state`.
- `tasks.<name>.state` — прогресс задачи (например downscale-broker:
  `isWithdrawing/serviceWithdrawn/storageWithdrawn`).
- `tasks.<name>.invoked` — последний экшен (какой action, на каком хосте, когда).
- `tasks.<name>.args` — аргументы запуска (`--cloud=hc --replicas=0`).

Под капотом — cloud-ops API `GET /api/status/byname?name=<partition>` на контроллере
cloud-ops (`*.cdb.cloud-ops-infra.<dc>.one-infra.ru`, mTLS из ~/.mccloud). Наш бэкенд
(mdb-backend `GetResultOperatorTaskProcessor`) поллит этот же эндпоинт и ждёт исчезновения
задачи из tasks, чтобы закрыть операцию как DONE.

## op_stop — когда нужен

Если задача оператора зациклилась (например, сервис уже выведен руками, а задача всё
пытается его withdraw → alert «Failed to withdraw ... Could not execute request»), сама
она не завершится. `op_stop <partition> <operator.task>` снимает задачу; следующая
операция из mdb-data сможет стартовать.

## Маркеры ответа

- `EntityNotFoundException: Partition <cluster> is not managed by both one-cloud-ops and ops-temporal`
- `Not found ops by namespace <ns>`
- `Failed to create one-cloud client: Namespace cannot be resolved from <ns>`
- `dial tcp: lookup cdb.cloud-ops.clouds.vkcl.ru: i/o timeout` — с бекстейджа/локальной
  машины DNS может не резолвиться для namespace `vkontakte`. Запускать mcc с хоста, у
  которого есть доступ.

Полный разбор операторного флоу downscale-broker: jira-mdbsup-solver/history/MDBSUP-4895-2026-08-26.md.
