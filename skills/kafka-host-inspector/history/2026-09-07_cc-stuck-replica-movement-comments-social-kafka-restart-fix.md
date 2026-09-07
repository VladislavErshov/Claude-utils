# CC завис в INTER_BROKER_REPLICA_MOVEMENT_TASK_IN_PROGRESS на 3 дня — перезапуск CC помог

**Кластер:** `comments-social-kafka` (dzen, ДЦ dc/hc/kc/pc/rc)
**Хост CC:** `1.cruise.comments-social-kafka.rc.idzn.ru`
**Дата:** 2026-09-07 (зависшее движение запущено 2026-09-04 19:18 MSK)

## Симптом

Executor Cruise Control висит в `INTER_BROKER_REPLICA_MOVEMENT_TASK_IN_PROGRESS` ~3 дня:
1 партиция `replies-updates-2` (переезд реплики `24001 → 22001`, `[24001,20001,21001]` →
`[22001,20001,21001]`), `totalDataToMove: 131` байт, `numFinishedPartitionMovements: 0`.
Anomaly detection всё это время скипается («Skipping anomaly detection because the executor
is in INTER_BROKER_REPLICA_MOVEMENT_TASK_IN_PROGRESS state»).

Контекст: 04.09 днём в кластер добавляли брокеры 23001/24001 (`POST /add_broker?brokerid=23001,24001&dryrun=false`),
затем 19:18 MSK `POST /rebalance?dryrun=false` с 127.0.0.1 (mdb-data) запустил 1 inter-broker
movement — и он не завершился.

## Диагностика

1. `/state?verbose=true` → ExecutorState: `IN_PROGRESS`, executionId 112, pending пуст, dead
   movement пуст. Сервис active, REST жив (порт **8080**, health-проверки с 127.0.0.1 каждые
   несколько секунд — 200 OK).
2. В логе CC периодические `java.net.UnknownHostException: 1.broker.comments-social-kafka.{dc,pc,kc,hc}.idzn.ru:
   Temporary failure in name resolution` от KafkaAdminClient (вплоть до момента инспекции),
   при этом `getent hosts` с того же хоста резолвит нормально → флапающий DNS внутри
   контейнера CC. Правдоподобная причина зависания: AdminClient CC не мог стабильно
   достучаться до брокеров для завершения reassignment.
3. Со стороны Kafka `kafka-topics.sh --describe --topic replies-updates`: у партиций 0-7
   `Leader: 24001, Isr: 24001` (реплика 22001 так и не вошла), новое назначение не применилось.
4. Сам хост `1.cruise...` в mcc `availability: PREFAIL` — не блокер для диагностики, но
   симптом вялости.

## Вывод

Reassignment на 131 байт не может ехать 3 дня — executor CC завис (вероятно из-за флапающего
DNS в контейнере: UnknownHostException по брокерам). **Перезапуск `cruise-control.service`
вылечил** — движение завершилось, кластер вышел из stuck-состояния (подтверждено пользователем).

## Действия

- `systemctl restart cruise-control` на `1.cruise.comments-social-kafka.rc.idzn.ru` — **помогло**.

## Грабли/заметки

- REST CC в MDB — порт **8080** (`webserver.http.port` в cruisecontrol.properties), не 9090.
- `grep` по логу CC на «finished/completed» вытаскивает гигантские JSON-блобы `/state` из
  operationLogger — фильтровать `grep -v operationLogger`.
- Команда `mcc instances '*comments-social*'` в `-n dzen` видит только cruise-хост; брокеры
  кластера через `mcc status <fqdn>` в этом неймспейсе не ищутся (EntityNotFoundException),
  но `mcc sshexec` на брокер работает.
- Признак для таких кейсов: бесконечное «Skipping anomaly detection because the executor is
  in ... state» в логе = executor застрял, обычное лечение — рестарт CC.
