# MDBSUP-5557 — qlab-production (mdb4922): create_cluster упал на квоте PC, кластер «застыл в создании»

Дата: 2026-09-18. Kafka, ns infra, облака hc/pc/rc.
Кластер `7ffc6888-e027-485d-80ac-6541351e9fcb`, fullQueue
`qlab-production-mdb4922-kafka.mdb4922.db.production.mdb.prod`.
Операция `2e613902-2953-4cd1-9e1d-e8d747c8ab14` (create_cluster).

## Суть

create_cluster упал на `submit_cruise_control_manifest`: квота ДЦ PC исчерпана
(`MEM=210G > 206G`, дефицит 4G) — манифест Cruise Control отклонён облаком.
Cruise-хост в облаке НЕ создавался (EntityNotFound на `cruise.<queue>` во всех ДЦ),
но строка в host_state и ключ `kafkaParams.cruiseControl` в версии были. Заявитель
просил перенести круиз PC → HC (перенос не поддерживается → удалить, пересоздать в HC).

## Диагностика — operator not fresh, а не только БД

1. БД: op failed/attempts_left=0/in_processing=false; host_state 7 хостов (6 живых + cruise-фантом).
2. `mcc instances '%qlab-production%'` — паттерн ОБЯЗАТЕЛЬНО с `%` (без него
   EntityNotFoundException даже по существующим хостам, полдня на это ушло).
3. ops: алерт `kafka.sync: precondition is false ... isClusterInfoRefreshed() false`
   с момента создания; у оператора kafka нет задачи `watch` (у здорового кластера —
   watch/availability/sync, у нас availability/sync).
4. Граф задач операции: после failed `submit_cruise_control_manifest` остались
   scheduled `get_submit_cruise_control_manifest`, `generate_kafka_operator_settings`,
   `finish_task`. Settings-таск (GenerateKafkaOperatorSettingsTaskProcessor) не выполнился.
5. Корень: в PMS (host `host-one-cloud-ops`, app `one-cloud-ops`) нет per-queue свойств
   `queue://<fullQueue>:root` (`start.watcher=kafka.watch`) и `queue://<fullQueue>:kafka`
   (7 строк настроек). Из-за отсутствия `vaultReadAdminPassPath` оператор читал
   superuser-пароль из Vault по дефолтному пути из MdbOpConf
   (`zkv/mdb/front/mysql/test-rmukh16-...`) → `VaultAction[vault:action] on vault is failed`
   → users/topics не рефрешились → `isFresh()=false` → kafka.sync заблокирован навсегда.
6. Wildcard-правило `queue://**-kafka.**.db.production.**.prod:root` содержит только
   `start.availability` + `start.sync` — `start.watcher=kafka.watch` приходит только
   из settings-процессора. У старых кластеров watch висит в persisted-состоянии
   `root.start` с моменat создания (per-queue root-переменной у communal тоже нет).

## Фикс (по инструкции «удалить хост и дотолкать операцию»)

1. SQL (прод, локальный psql через туннель — docker на маке был завис):
   `DELETE 1` host_state (cruise), `UPDATE 1` db_cluster_version
   (`cluster_params #- '{kafkaParams,cruiseControl}'`), `UPDATE 1` operations
   (failed → done, finished_ts=now(), error_message=NULL).
2. PMS update.do (ns=infra, host=host-one-cloud-ops, app=one-cloud-ops) — ровно то,
   что писал бы settings-процессор:
   - `queue://<fullQueue>:root` = `start.watcher=kafka.watch`
   - `queue://<fullQueue>:kafka` = `pmsClusterHost=qlab-production-mdb4922-kafka.clouds\nvaultReadAdminPassPath=zkv/mdb/mdb4922/kafka/<fullQueue>/super\ns3Endpoint=https://s3.idzn.ru\npmsApplicationName=mdb\nwanCluster=false\ns3PrefixBackups=\ns3VaultPath=`
   (значения сверены байт-в-байт с values.do; isWan=false из one_cloud_meta).
3. `mcc --local -n infra -c pc op_start "queue://<fullQueue>" kafka.watch` — watch в
   рантайме сразу, не дожидаясь перечитывания PMS мастером. root.start не останавливает
   «неконфигурные» задачи, пока в том же операторе бегут другие (StartTask).

## Проверка

~5 минут: `kafka.watch[refreshAvailabilityState] = Cluster is AVAILABLE, no failed hosts`,
`kafka.watch[refreshStatus] = Operator is fresh. Is ready for actions.`; задачи
watch/availability/sync запущены; алерты kafka.sync/vault исчезли. BrokerState=3
(RunningAsBroker) на брокере pc; 6 хостов в БД = 6 в облаке;raft-metrics MBean на 4.x
брокере отсутствует (InstanceNotFoundException — не диагностический признак).

## Грабли

- `mcc instances`/`status` без `%`-паттерна не находят существующие хосты (ложный EntityNotFound).
- Туннель `mcc tp-port-forward` (tsh) умирает молча: слушающий сокет есть, соединение
  падает. Лечится kill старого PID tsh + перезапуск; docker exec на маке может висеть
  независимо — локальный psql (~/.brew) работает и без контейнера.
- `generate_kafka_operator_settings` не выполняется, если упал любой предыдущий таск
  графа create_cluster — кластер при этом может быть полностью живым (хосты созданы),
  но оператор никогда не станет fresh: без PMS-свойств Vault-путь дефолтный.
- При ручной дописке `queue://<fullQueue>:root` — НЕ затирать wildcard-дачные
  start.availability/start.sync: per-queue значение пишется отдельной переменной,
  мержит их сам мастер конфигурации.

## Хвост

Пересоздание Cruise Control в HC — заявитель через UI (кнопка активна после чистки
`kafkaParams.cruiseControl`). one_cloud_meta (cruise-control-service) не трогали —
create-флоу переиспользует.
