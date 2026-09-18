# MDBSUP-5216 — kafka-news-snapshots (news): зависшая sys_update_cluster — задача kafka.update исчезла, операция «запаркована»

Дата разбора: 2026-09-08. Кластер: `kafka-news-snapshots` (project news=10, prod, ns **infra**), UUID `9974b16f-f2d1-4b29-a237-a9753d1f3700`, fullQueue `kafka-news-snapshots-news-kafka.news.db.production.mdb.prod`. 4 ДЦ (ec/hc/kc/pc), 3 контроллера (ec/kc/pc), 12 брокеров, CC в hc. За дни до этого: modify_cluster, add_hosts, delete_hosts (04.09), add_hosts (07.09 10:13, все done).

Операция: `sys_update_cluster` `de275607-e287-4422-aaca-500fb2736b02` (создана 07.09 12:06, обновление docker-образа ubuntu20-kafka-3.8.0 тег 2.3.2 → 2.4.4). В тикете: шаг `get_result_service_sys_update_operator` завис с 07.09 13:07.

## Главное: sys_update_cluster — тоже операторский флоу (задача `kafka.update`), Temporal пуст — это НОРМА

- Temporal по operationId — пусто (`{}`). Это не «история удалена», как предполагали в тикете: `sys_update_cluster` исполняется **оператором one-cloud-ops** (задача `kafka.update --db-type=kafka --image-version=<целевой>`), а mdb-backend `GetResultOperatorTaskProcessor` поллит `GET /api/status/byname` и ставит DONE только когда задача исчезает из `operators.kafka.tasks`.
- По ClusterId в Temporal 82 executions — все чужие (upscaleKafkaBroker add_hosts 07.09, createKafkaTopics 08.09) COMPLETED. Workflow `de275607` там и не должен был появиться.

## Состояние оператора (08.09 ~18:00)

- `operators.kafka.tasks`: только watch/availability/sync — задачи `update` **НЕТ**, `action: "no action"`.
- **`taskinfos` и `persisted` тоже не содержат следов `update`** — оператор не хранит историю завершённых/снятых задач: если задачи нет в tasks, по состоянию очереди невозможно понять, была ли она вообще.
- Алерт: «Cluster is AVAILABLE Failed hosts: **no failed hosts since 2026-09-08 17:39:04**» — до 17:39 08.09 в облаке были failed hosts (вероятная причина стоянки задачи `kafka.update` в precondition-ожидании; похоже на паттерн MDBSUP-5219, где update ждал контроллеры в PREFAIL).
- Логи cloud-ops-контроллера через sshexec недоступны (multiple containers / TLS timeout, канон mcc-host-worker/commands/sshexec.md) — точный момент исчезновения задачи не восстановить.

## Хосты на старой версии — обновление не перекатилось вообще

- Выборка (брокеры pc/hc/ec/kc, контроллер pc): `/opt/kafka/libs/` → `kafka_2.13-3.8.0` = образ 2.3.2. Частичного ролла нет.
- Последняя строка `db_cluster_version` (draft, 07.09 10:15, l.marutsak) — тоже 2.3.2/3.8: целевая 2.4.4 не применилась ни на хосты, ни в БД. В отличие от 5219, версию в БД править НЕ нужно — она фактическая.

## Ловушка: задача исчезла, а get_result не увидит TASK_ABSENT (отличие от MDBSUP-5092)

В 5092 после op_stop `get_result` увидел TASK_ABSENT → операция закрылась **сама**. Здесь задача исчезла раньше, но операция замерла: `status=in_progress` + **`in_processing=false`** — процессор get_result больше не запланирован, закрывать надо руками.

Сигнатура «запаркованной» операции (сверять при разборе):
- 5216: `in_progress / in_processing=false / attempts_left=2 / error_message=''` — вечно;
- 5219 (та же механика): дошло до `failed / in_processing=false / attempts_left=2` c `Error while get_result_service_sys_update_operator: Operator task ... in progress undefined`.

## Что сделать

1. Закрыть операцию SQL (реальных изменений нет, `db_cluster_version` не трогать). Сделано 08.09 17:57 — статус **canceled** (владелец решил отменить, а не считать успешной):
   `UPDATE operations SET status='canceled', in_processing=false, finished_ts=now() WHERE id='de275607-e287-4422-aaca-500fb2736b02' AND status='in_progress';`
2. Обновление до 2.4.4 — перезапустить владельцем через UI как новую `sys_update_cluster`. **Повтор 08.09 17:58 (`caa4a039`) прошёл штатно:** операторская задача завершилась за ~1 мин, все 4 шага done (start → get_result → update_cluster_image_version → finish_task), `db_cluster_version` → dockerTag 2.4.4 проставил сам шаг `update_cluster_image_version`; `operations.status` перевернулся в done сам в 18:00:47 — вручную не понадобилось (первый срез в 18:00 поймал гонку с finish_task и показывал in_progress — сверять статус операции только после finished_ts).

⚠️ Проверять фактический тег образа на хосте по kafka-jar из `/opt/kafka/libs` НЕЛЬЗЯ — теги 2.3.2 и 2.4.4 одного образа ubuntu20-kafka-3.8.0 содержат одинаковый kafka_2.13-3.8.0.jar; смотреть docker-контейнер/image на хосте.
