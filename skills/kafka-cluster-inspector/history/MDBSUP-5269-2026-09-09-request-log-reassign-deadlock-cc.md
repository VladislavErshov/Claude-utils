# MDBSUP-5269 — rt-filters-adtech-kafka: delete_hosts (rc) + дедлок reassign'ов CC на 23001 вне ISR (09.09.2026)

Кластер `rt-filters-adtech-kafka` `b1248203-3905-471c-b6b7-0dd9c6ba99c8` (adtech, infra,
fullQueue `rt-filters-adtech-kafka.adtech.db.production.mdb.prod`). 4 ДЦ: kc=20001-20008,
pc=21001-21008, rc=22001-22008, uc=23001-23008. Контроллеры pc/kc/uc (rc-контроллер выведен
27.08). Операция `6fa63911-8124-4aa3-bc1a-c95e74dae803` delete_hosts — вывод **8.broker.rc
(22008)**, `--cloud=rc --replicas=7`, isWithdrawing=false (сервис и storage остаются).
Temporal по operationId пуст (operator-флоу). Заглушка: jira-mdbsup-solver/history/MDBSUP-5269-2026-09-09.md.

## Диагноз

- Оператор one-cloud-ops **жив, но крутится в цикле**: `kafka.downscale-broker` в
  «Waiting for broker reassignment check», KafkaAdminAction каждые ~5 мин, watch fresh.
  Дренаж 22008 почти завершён — оставалась 1 партиция `request-log-10` (~88 ГБ, offsetLag=0).
- **4 партиции request-log (10/24/59/82) в активных reassignment'ах** (проверка —
  AdminClient listPartitionReassignments, мини-скрипт `ListReassign.java` на брокере,
  source-file mode + `/opt/kafka/config/client.properties`):
  - p10: union 12 реплик, adding=[21006], removing=[22008]
  - p24: [20001,21006,23001,22001], adding=[23001], removing=[22001]
  - p59: [20001,21006,23001,22004], p82: [20001,21008,23001,22004] — аналогично
- **Дедлок (корень):** во всех четырёх цель включает 23001 (1.broker.uc), а 23001 **не в ISR**
  — при этом лаг 0, брокер здоров (в ISR соседних партиций, фетчит сегменты). Реплика из
  union-цели вне ISR **не возвращается в ISR, пока reassignment активен** (лидер 20001
  добавлял в ISR только fresh-adding 21005/21006 — epochs 152→155 — а 23001 нет ни разу).
  Reassignment не может завершиться (ISR ⊉ target) → CC крутит его вечно → оператор вечно ждёт.
- **Кто заказчик reassign'ов:** Cruise Control self-healing
  (`triggeredTaskReason: Self healing for GOAL_VIOLATION RackAwareDistributionGoal`,
  executor `INTER_BROKER_REPLICA_MOVEMENT_TASK_IN_PROGRESS`, 0% из 349 ГБ). CC считал
  **все 8 rc-брокеров recentlyRemovedBrokers** (22001–22007 живы!) — протухшая модель,
  поэтому он эвакуировал rc-реплики в uc.
- Не блокеры: throttle rate 1 ГБ/с (не троттлинг); троттл-списки в конфиге топика
  (`10:…,24:…,59:…,82:…`) соответствуют union'ам застрявших reassign'ов.

## Лечение

1. **CC:** `POST /kafkacruisecontrol/STOP_PROPOSAL_EXECUTION` (без параметров! `dryRun`
   не поддержан) → executor STOPPING_EXECUTION → NO_TASK_IN_PROGRESS. Self-healing
   выключить: `POST /kafkacruisecontrol/ADMIN?disable_self_healing_for=GOAL_VIOLATION`
   (именно тип аномалии, не имя goal; ответ `true→false`). Порты CC: webserver 8080.
2. **Reassign по варианту A** (RF=3, rc-реплики выбросить, схема kc/pc/uc как у
   большинства топика; лидер 20001 первым) — скилл `kafka-reassign-partitions`,
   `--execute --throttle 104857600`:
   ```
   p10 → [20001, 21004, 23002]   # все в ISR — завершился мгновенно
   p24 → [20001, 21006, 23002]   # переливка ~60-90 ГБ на 23002
   p59 → [20001, 21006, 23003]
   p82 → [20001, 21008, 23004]
   ```
   Вариант B (сохранить 4-ДЦ покрытие, rc-реплику переехать на живой rc-брокер) —
   отклонён пользователем.
3. **Оператор докрутил сам** (без op_stop!): после завершения p10 чек прошёл —
   unregister 22008, withdraw инстанса в облаке (хост перестал резолвиться даже для
   оператора, alert «Unknown kafka role»), задача исчезла из `operators.kafka.tasks`.
   host_state-строка удалена флоу операции. Аналог 5103/saturn-target: оператор всё
   сделал сам.
4. **Операция в БД:** самосхождения не случилось (attempts_left=0, mdb-data попытки НЕ
   перезаряжал — в отличие от 5092) → закрытие SQL
   `UPDATE operations SET status='done', in_processing=false, finished_ts=now(), error_message=NULL`.
5. Верификация: 31 брокер (22008 отсутствует), URP=0, Cluster AVAILABLE, p10 RF=3.

## Грабли

- `kafka-topics.sh --describe --partition N` в Kafka 4.x **не существует** — только полный
  describe + grep.
- `kafka-log-dirs.sh`/CLI на localhost:9092 падают `SslAuthenticationException: No subject
  alternative DNS name matching localhost` — bootstrap только по FQDN (канон mcc #9).
- `mcc scp` на macOS снова молча не залил файл → base64 одной строкой через sshexec
  (файл < 1 КБ влезает).
- CC-эндпоинты этой версии: `STOP_PROPOSAL_EXECUTION` (не `stop_execution`), параметры
  ADMIN — snake_case (`disable_self_healing_for`), значения — типы аномалий
  (`GOAL_VIOLATION`), не имена goals.
- URP-подсчёт: `wc -l` на выводе describe врёт (AdminClientConfig-логи в том же выводе) —
  фильтровать по `Partition:`.

## Продуктовые баги (в тикеты не заведены — предложить)

- **Kafka 4.x:** реплика из union-цели вне ISR не ре-энтерит ISR при активном reassignment
  → reassignment не завершается никогда (похоже на семейство KAFKA-16441). Воспроизведение:
  stuck reassign + target-реплика, выпавшая из ISR.
- **Cruise Control:** recentlyRemovedBrokers содержит живых брокеров (22001–22007) —
  self-healing по RackAware эвакуирует их реплики ложно.
- **mdb-processing/mdb-backend:** get_result-флоу downscale-broker не перезаряжает попытки
  операции, пока операторная задача висит (сравнить с 5092, где перезаряжались).

## Хвосты (на момент закрытия тикета)

- Переливка p24/59/82 шла (~4–8 ГБ из ~60–90 ГБ, ~12 МБ/с при throttle 100 МБ/с — лимитер
  не в квоте; завершится за часы сама).
- После завершения переливок: снять topic-конфиги
  `leader/follower.replication.throttled.replicas` у request-log (протухшие записи
  10:/24:/59:/82:) и broker-дефолты `leader/follower.replication.throttled.rate`.
- Рестарт CC (очистить recentlyRemovedBrokers) + вернуть self-healing
  `ADMIN?enable_self_healing_for=GOAL_VIOLATION`.
