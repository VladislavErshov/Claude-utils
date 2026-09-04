# MDBSUP-5051 — kafka-news-snapshots: upscale-broker-replicas, операция add_hosts «завершилась» после рестарта оператора, но осталась in_progress

Дата: 2026-09-03. Кластер `kafka-news-snapshots` (`9974b16f-f2d1-4b29-a237-a9753d1f3700`,
project news, prod, ns infra), операция add_hosts `da93cf45-ce19-4060-bab5-85d321759e7e`
(+1 брокер в hc, заявитель s.boryaev). Пользователь просил отменить операцию и
зафиксировать 4 брокера в hc.

## Хронология

- 03.06 13:16 — операция стартовала: `modify_kafka_hosts` создал строку `5.broker...hc`
  в host_state, `start_kafka_upscale_broker_operator` зарегистрировал задачу оператора
  `kafka.upscale-broker-replicas --cloud=hc --replicas=5`. Облако хост не создало —
  задача ретраила rescale и падала («Operator task ... in progress» в get_result).
  Операция failed, attempts исчерпаны (июнь).
- 02.09 ~17:06 — **оператор перезапустился**, задача на миг исчезла из
  `operators.kafka.tasks` → авто-ретрай операции прошёл «успешно»: `get_result_...`
  увидел задачу абсентной → done, затем `update_db_connection_url` + `finish_task`
  done (17:08:40). Но `operations.status` остался `in_progress`
  (in_processing=false, error_message=NULL, finished_ts=17:08:40) — финализация до
  done не доехала.
- 02.09 17:09:53 — оператор восстановил задачу `upscale-broker-replicas` из
  персистентного состояния и продолжил ретраить создание 5-го брокера:
  алерт «Failed to rescale ... Could not execute request». В облаке по-прежнему 4
  брокера в hc (все RUNNING), kc/pc — по 4.

## Что сделали

1. `mcc --local -n infra -c hc op_stop "queue://<fullQueue>" kafka.upscale-broker-replicas`
   — задача остановлена, после проверки через `mcc ops` не перерегистрировалась.
2. Прод-БД (одна транзакция):
   - `UPDATE operations SET status='canceled', in_processing=false, finished_ts=now(),
     error_message='...' WHERE id='da93cf45-...'` — canceled (не done: 5-го брокера нет);
   - `DELETE FROM host_state WHERE cluster_id='...' AND host='5.broker...hc...'` —
     убрана строка-призрак → модель кластера = 4 брокера в каждом ДЦ.
3. Верификация: `mcc ops` — из tasks остались только availability/sync/watch;
   SELECT — canceled + ghost_left=0.

## Новое для каталога

- **`upscale-broker-replicas`** — зеркальный кейсу downscale-broker таск оператора
  (см. §«Что делает downscale-broker» выше), op_stop работает так же. Отличие: при
  upscale mdb сам создаёт строку в host_state на старте (`modify_kafka_hosts`) — при
  отмене add_hosts эту строку-призрак надо удалять руками.
- **Ловушка «ложный успех get_result»**: рестарт оператора на время убирает задачу из
  `operators.kafka.tasks` → ретрай операции проходит (все tasks done, workflow
  completed), но `operations.status` может остаться `in_progress`. «Все задачи done»
  ≠ «операция done» — сверять статус операции и наличие задачи в `mcc ops`, а не
  только таблицу tasks.
- Cruise-хост в hc в это же время мигал в алерте watch «RUNNING UNAVAILABLE» —
  notificationState RESOLVED, `mcc instances` показывал RUNNING с пустым outcome:
  шум, не связан с кейсом.
