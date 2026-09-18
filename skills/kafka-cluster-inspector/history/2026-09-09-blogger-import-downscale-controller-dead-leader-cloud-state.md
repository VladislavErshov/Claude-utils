# blogger-import — delete_hosts (downscale kc-контроллера) упал на «не-RUNNING» HC-лидере

- Дата: 2026-09-09
- Кластер: Kafka `blogger-import` `1135e80d-8cff-4d7a-b873-c6ec4d5cc2c8`, ns **dzen** (mcc `-n dzen`),
  fullQueue `blogger-import-video-kafka.video.db.production.mdb.prod`
- Операция: `d0206f30-f44c-4398-94cf-2c4817674cc0` (delete_hosts = downscale kc-контроллера до 0),
  status=failed, attempts_left=0, in_processing=true (блокирует следующие операции кластера)
- Хостов в host_state: 3 брокера (hc/kc/pc, update_ts 2025-06-19) + 4 контроллера (ec/hc/kc/pc,
  update_ts 15:00:56 — от добавления add_hosts `7cfe3c32` в тот же день; до этого был modify 14:02)

## Причина

Целевой состав: pc=1, kc=0, hc=1, ec=1 контроллера (kc удаляем). Удаляемый — **kc**, но упал
воркфлоу на **hc** — актуальном лидере KRaft: облако считает hc-инстанс не RUNNING (в UI mdb-data
«unknown», при этом процесс жив — иначе не был бы лидером). Цепочка:

- `restartControllerInstanceSsh(pc)` ✓ → ping ✓; `restartControllerInstanceSsh(ec)` ✓ → ping ✓
- `restartAndRestoreControllerInstanceSsh(hc)` ✗ → `CloudException$NotRunning`
  («Instance 1.controller.blogger-import-video-kafka.hc.idzn.ru is not RUNNING»),
  `RETRY_STATE_MAXIMUM_ATTEMPTS_REACHED`; 3 run'а workflowId=operationId
  (13:09/13:32/15:06 UTC) — все FAILED.

Корень в коде: `CloudServiceImpl.sshExec` (mdb-commons-lib proxylib, :63) бросает `NotRunning`
по cloud-статусу **до** любого SSH — расхождение «cloud state ≠ факт» кладёт флоу.

## Разбор по коду (что было бы, если бы лежал удаляемый)

`DownscaleKafkaControllerInClusterWorkflowImpl` + `DownscaleKafkaControllerInDcWorkflowImpl`:

- SSH-шаги (`restart*ControllerInstanceSsh`, quorum/leader-чтение, ping) — только по
  **оставшимся** контроллерам и актуальному лидеру (`AbstractKafkaControllerScaleWorkflow:40`,
  `DownscaleKafkaControllerInClusterWorkflowImpl:288`).
- Удаляемый вычищается без SSH: `removeControllerFromQuorum` — PMS-API (`upsertPms`, :151,
  идемпотентно), withdraw ДЦ — cloud-API `stopService → withdrawService → withdrawStorage`
  (child-workflow, :82) — stop на не-RUNNING инстансе валиден.
- **Вывод: лежащий удаляемый контроллер операцию бы не сломал** — флоу дошёл бы до конца.
  Единственный теоретический фейл: если удаляемый — активный лидер, `migrateLeaderIfNeeded`
  (:226) рестартит его через SSH и упал бы так же; но мёртвый узел не может быть лидером KRaft.
- До `withdrawControllerService(kc)` воркфлоу не дошёл — kc остался в host_state, операция висит.

## Дальнейшие шаги (не выполнялись — нет одобрения)

1. Разобраться с cloud-состоянием hc-контроллера (UI unknown / not RUNNING при живом процессе) —
   `mcc instances` по `*blogger-import*` в dzen-instance-листинге контроллеров не показал,
   смотреть через `-c HC`/control plane.
2. Когда hc RUNNING → ретрай операции (новый run с тем же workflowId отработает идемпотентно:
   removeControllerFromQuorum no-op, restart pc/ec повторный, withdraw kc).
3. Альтернатива — закрыть руками: `UPDATE operations SET status='done', in_processing=false,
   finished_ts=now(), error_message=NULL WHERE id='d0206f30-...'` + дослать withdraw kc каноном
   MDBSUP-5263 (stop → withdraw service → withdraw storage → DELETE host_state + правка
   `controllerDcs` в db_cluster_version).
