# MDBSUP-5335 — events-front-kafka: CC завис на remove_broker, «фантомы» оказались живыми брокерами

**Дата:** 2026-09-11 → 2026-09-12
**Кластер:** events-front-kafka (front, прод, dzen namespace, FQDN *.idzn.ru / *.wan.idzn.ru)
**Круиз:** 1.cruise.events-front-kafka.pc.idzn.ru (REST на порту 8080!)

## Симптомы

1. «Cruise завис» — ExecutorState: `INTER_BROKER_REPLICA_MOVEMENT_TASK_IN_PROGRESS`, прогресс 0.01% за 3+ часа.
2. Kafka-side: `kafka-reassign-partitions.sh --list` → `No partition reassignments found` при 57 in-progress движениях в CC. Executor крутится вхолостую, reassignments в Kafka не доходят.
3. `POST /stop_proposal_execution` возвращает `{"message":"Proposal execution stopped."}`, но задача остаётся InExecution — **abort не работает**. Единственный способ снять — `systemctl restart cruise-control`.

## Финал (2026-09-13): найден upstream-баг, два слоя проблемы

**Слой 1 — известный баг CC, исправили конфигом:**
- [PR #2339](https://github.com/cruise-control-for-kafka/cruise-control/pull/2339) + [Issue #2385](https://github.com/cruise-control-for-kafka/cruise-control/issues/2385) (репо `cruise-control-for-kafka/cruise-control`, LinkedIn передал проект комьюнити): `ReplicationThrottleHelper.waitForConfigs` виснет, если в кластере есть **cluster-wide default replication throttle** (`--entity-default`): удалённый per-broker конфиг резолвится в default → верификация не сходится. `CruiseControlMetricsUtils.retry` удваивает sleep без капа (5s×2ⁿ, 13-я попытка ~11ч) → executor-поток паркуется навсегда. Симптомы: «execution hung после первой партии», новые задачи отклоняются, abort не помогает.
- У нас: default throttle 1048576000 стоял с давних пор + легаси `throttled.replicas`-списки в 26 топиках от старых кампаний.
- **Фикс:** удалили `--entity-default` throttle (broker) и `throttled.replicas` во всех топиках. После этого jstack подтвердил второй hang уже на setThrottles/changeTopicConfigs → после чистки конфигов установка throttle стала мгновенной, executor проходит партии и throttle-removal.

**Слой 2 — «reassignments не доходят»: НЕ подтвердился при повторном прогоне.**
- Первый тестовый rebalance после чистки конфигов выглядел как рецидив (0.00% bytes, list пуст) — но замер был сделан в переходной фазе подачи задач.
- Контрольный запуск на следующий день: executor проходит партию за партией (progress-check каждые 2 мин, throttle-removal мгновенный с пустым списком `[]`), данные реально перемещаются (~40 МБ/с при replication_throttle=100 МБ/с). Т.е. после полной чистки throttle-конфигов CC execution работает штатно.
- Урок: вердикт «движения не идут» ставить только по динамике (два замера bytes с интервалом), а не по одному срезу.

**Слой 2 — ОТКРЫТАЯ ПРОБЛЕМА (актуально только для запусков ДО чистки конфигов): reassignments от CC не доходят до Kafka.**
- CC подаёт задачи ("Executor will execute 235 task(s)"), но: контроллер-лог (лидер quorum) в окно запуска НЕ содержит AlterPartitionReassignments, `kafka-reassign-partitions.sh --list` пуст, URP=0, **данные не двигаются** (bytes 0.00%), а CC мгновенно помечает задачи COMPLETED (no-op детект — целевая схема совпала с текущей с прошлых попыток).
- Ручной `kafka-reassign-partitions.sh` с того же кластера работает корректно. CC-принципал `super` — в super.users, авторизация ни при чём. Мониторинг-консьюмер CC работает (аутентификация ОК).
- **Статус: снят** — после чистки конфигов не воспроизводится (см. выше), вероятная причина стагнации — остатки throttle-конфигов и переходные фазы при подаче.
- Точная причина не установлена (подозрение на несовместимость сборки CC с Kafka 4.x в пути alterPartitionReassignments). **Workaround: движения — только ручным reassign по схеме; CC использовать на dryrun/мониторинг.**
- При этом stop_proposal_execution с НЕзаблокированным потоком работает штатно (NO_TASK_IN_PROGRESS через ~1 мин).

**Диагностика зависаний CC (чек-лист):**
1. `/state?substates=EXECUTOR` — прогресс finished/bytes растёт?
2. `kafka-reassign-partitions.sh --list` на брокере — есть ли reassignments в Kafka?
3. Лог контроллера-лидера (`kafka-metadata-quorum describe --status` → LeaderId) в окно запуска — приходили ли AlterPartitionReassignments.
4. jstack круиза: grep `waitForConfigs` — если поток там + `CruiseControlMetricsUtils.retry` → баг #2385; проверить default throttle и topic throttled.replicas.
5. abort не помогает при заблокированном потоке → рестарт cruise-control.

## Первопричина зависания executor'а (разбор логов после реставрации)

Хронология из gz-логов CC (таймзона лога = МСК, REST access log = UTC):
1. **18:40:08** юзеры вызвали `remove_broker` (removedBrokerIds=[20001..20012, 23009..23012], replicationThrottle=**9223372036854775807** = Long.MAX/unbounded — CC-конфиг `partition.migration.byte.rate.per.broker` не задан).
2. CC сгенерил 383 движения, «Executor will execute 80 task(s)», ReplicationThrottleHelper выставил per-broker throttle Long.MAX.
3. **В контроллер-логе Kafka в 18:40–18:44 НЕТ ни одного AlterPartitionReassignments** — подача CC до контроллера не дошла.
4. **18:42:10** — единственный progress-check (интервал 2 мин): 23/383 «COMPLETED» (это no-op'ы — целевая схема уже совпадала с текущей после утренних попыток юзеров), 368MB — из более ранних переливок. Затем `Removing replica movement throttles from brokers: [21007]` — и **поток executor'а замер**: следующего чека (18:44) нет, до 18:49 ни одной строки executor'а.
5. **18:49** AdminClient закрыл idle-коннекты к десятку брокеров (connections.max.idle.ms=5 мин) — executor перестал слать запросы ещё раньше.
6. Утром того же дня — та же симптоматика после предыдущей попытки юзеров (лечили миграцией контейнера круиза + рестартом).

**Итог:** executor-поток CC заблокировался сразу после первого progress-check'а (вероятно, на AdminClient-вызове снятия throttle с мёртвым коннектом после утренней миграции контейнера; таймаута нет → вечная блокировка). Stop_proposal_execution ставит флаг аборта, но заблокированный поток его не читает → рестарт единственный способ.

**Профилактика:**
- Не пускать юзеров к `remove_broker` напрямую; правильно: mdb-операция/ручной reassign по схеме.
- Задать в CC-конфиге вменяемый `partition.migration.byte.rate.per.broker` — Long.MAX в брокерские throttle-конфиги писать опасно (риски overflow token bucket квоты).
- После рестарта CC всегда проверять `NO_TASK_IN_PROGRESS` перед новыми операциями.

## Корень

Юзеры кластера сами запускали с круиз-хоста (127.0.0.1):
`POST /remove_broker?dryrun=false&brokerid=20001..20012,23009..23012` — **вместе со всеми живыми DC-брокерами**. CC не смог исполнить и завис.

**Ключевая ловушка:** брокеры 23009–23012 advertise как `9–12.broker.events-front-kafka.hc.idzn.ru` — это PC-брокеры, физически живущие на HC-хостах! `broker.id` НЕ определяет ДЦ хоста. По mdb host_state хосты 9–12.hc числятся, mcc их видит только с флагом `-c hc` (без него instances показывает один ДЦ).

Изначальная гипотеза «фантомы — вывести» была ошибочной: это живые брокеры с репликами (в ISR). Вывод пришлось откатывать.

## Как чинили

1. Рестарт CC → `NO_TASK_IN_PROGRESS`.
2. Ошибочный вывод (23009–23012): дрейн 127 партиций на 23003–23008 через `kafka-reassign-partitions.sh --execute --throttle` → stop kafka-broker на 9–12.hc → KRaft сам дерегистрировал (unregister вернул «The given broker ID was not registered» — норма после stop+дрейн).
3. Откат: `systemctl enable --now kafka-broker` на 9–12.hc → брокеры зарегистрировались → обратный reassign из сохранённого исходного describe (файл `phantom_partitions_raw.txt`). Все 127 вернулись, URP=0.

## Правильный путь снятия «фантомных» брокеров (если когда-нибудь понадобится)

Дрейн реплик (reassign) → стоп процесса → KRaft дергистрирует сам / `kafka-cluster.sh unregister --bootstrap-server ... --id N --config <client.properties>` (флаг `--config`, НЕ `--command-config`; unregister работает только для fenced брокера без реплик).

## Грабли

- **dzen namespace**: mcc `-n dzen`, FQDN `*.idzn.ru`; sshexec тоже требует `-n dzen`.
- `mcc instances` без `-c <dc>` возвращает хосты только одного ДЦ — хост «не найден» ≠ его нет.
- `mcc scp` на эти хосты молча не льёт — заливать base64-чанками ~1200 символов в ОТДЕЛЬНЫЕ файлы (`printf '%s' '<chunk>' > /tmp/p_001`), потом `cat /tmp/p_* | base64 -d`. Appending в один файл ломается при ретраях (дубли чанков).
- macOS: нет `timeout` — не оборачивать mcc в локальный timeout, использовать `timeout` ВНУТРИ sshexec-команды (Linux-хост).
- kafka-cluster.sh (4.x) — `--config`, не `--command-config`.
- Повторный `--execute` с тем же json (например, сменить throttle) требует `--additional`.
- CC REST у этого кластера на **8080** (`webserver.http.port`), а не 9090/9000; конфиг `/opt/cruise-control/config/cruisecontrol.properties`.
- Kafka CLI на брокерах: конфиг `/opt/kafka/config/client.properties`, bootstrap `$cloud_hostname:9092`.
- Оценка прогресса переливки: `kafka-log-dirs.sh --describe --broker-list <id>` — сравнивать размер партиции на target с лидером. URP=0 во время Adding Replicas — норма (переходное состояние, Replicas содержит и старую, и новую реплику с Adding/Removing).
