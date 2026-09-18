# MDBSUP-5510 — зомби-кластер MongoDB secop-admin: delete_cluster падает на queue.watch

Дата: 2026-09-17. Кластер: secop-admin (MongoDB, unisearch), UUID `12685785-e25d-4978-903f-ef1563f63b68`.
Операция: `7e28d8d4-1c24-4697-b96e-49acbb97dd74` (delete_cluster).

## Суть

Кластер-зомби: записи в прод-БД есть (db_cluster + host_state 4 хоста), в облаке нет ничего —
все 4 хоста `EntityNotFoundException` (mcc status), `mcc instances` пусто, operator-партиции нет
(`mcc ops` → "Partition ... is not managed by both one-cloud-ops and ops-temporal").
Причина: 16.09 15:04 create_cluster отменили через cancelClusterOperations до материализации
в облаке; записи в mdb-data остались.

delete_cluster падает на первом шаге start_watch_queue_operator:
`Error while start_watch_queue_operator: queue.watch not started for cluster queue://secop-admin-unisearch-mongo.unisearch.db.production.mdb.prod`
— не за чем следить. В Temporal по operationId пусто (флоу операторный). Ретрай бессмысленен.

## Фикс

Ручная чистка записей из прод-БД одной транзакцией (в облаке удалять нечего), порядок с учётом FK:
permissions (по database_id/user_id) → tasks (по operation_id) → operations → users → databases →
host_state → db_cluster_version → one_cloud_meta → cluster_to_template → db_cluster.

Объём: permissions 2, tasks 53, operations 3, users 1, databases 2, host_state 4,
db_cluster_version 1, one_cloud_meta 2, cluster_to_template 1, db_cluster 1.
Верификационный SELECT после COMMIT — 0 по всем таблицам.

## Грабли / паттерн

- Симптом «queue.watch not started для cluster queue://<fullQueue>» при delete = партиции
  кластера нет в операторе; проверить `mcc ops "queue://<fullQueue>"` и `mcc status` хостов
  из host_state, прежде чем копать глубже.
- Тикет создали через сутки после отмены create: статус операции failed, attempts_left не тратится.
- psql локально (`/opt/homebrew/bin/psql`) через туннель 53480 — рабочий запасной путь, когда
  docker-демон не запущен (контейнер pg_backstage_plugin_mdb недоступен).
- Кандидат в баг: delete_cluster не умеет удалять кластеры, которых нет в операторе/облаке
  (упавший create оставил записи) — требует ручной хирургии прод-БД.
