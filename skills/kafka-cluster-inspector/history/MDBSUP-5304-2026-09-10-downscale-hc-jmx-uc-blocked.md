# MDBSUP-5304 — logs-recom-wizard-kafka: ручной downscale hc-брокера, JMX до uc отфильтрован меж-ДЦ

**Дата**: 2026-09-10
**Кластер**: `logs` (`f0d28ab1-bbd6-4288-a2f3-958b635a5f3e`), ns dzen, 4 ДЦ,
очередь `logs-recom-wizard-kafka.recom-wizard.db.production.mdb.prod`
**Операция**: `9fc2e2fa-fa0f-4eda-862c-20f8989aba5f` (delete_hosts, dc=hc, 12→11 брокеров)
**Тикет**: https://jira.vk.team/browse/MDBSUP-5304

## Корневая причина зависания

Таск `kafka.downscale-broker` (`--cloud=hc --replicas=11`) с момента старта (09:48) в precondition:

```
super.precondition() false  operator().isFresh() false  isReadyForActions() false
isBrokerDrained false  instances().count() 50  getNodes().size() 49
```

`isFresh() false` — оператор не может снять JMX-состояние uc-брокеров (`Failed to retrieve
RMIServer stub ... SocketTimeoutException: Connect timed out` по всем 10 uc-брокерам).

Сетевая топология проблемы (проверено /dev/tcp с брокеров):

| путь | 7777 (JMX) | 9092 (Kafka) |
|---|---|---|
| {hc,kc,pc} → uc по внутренним fd00-именам (`*.uc.idzn.ru`) | **BLOCKED** | OPEN |
| любой → uc по wan (`*.uc.wan.idzn.ru`) | OPEN | — |
| uc → всё (обе формы имён) | OPEN | — |

Т.е. меж-ДЦ внутренняя связность в целом жива (9092 ходит, hc→kc/pc 7777 ходит), но **порт
7777 на внутреннем пути до uc отфильтрован** (firewall/СБ, не хостовое — iptables пуст).
Оператор ходит по коротким внутренним именам (это имена инстансов в его модели), поэтому
рефреш uc невозможен. Рефреш сломан минимум с **2026-09-08** (alert `kafka.update: Waiting
for operator refresh since 2026-09-08 18:03`), т.е. любая операторная операция по кластеру
встанет так же, пока сеть не починят. **Хвост не закрыт** — нужна эскалация в сетевую
команду dzen/uc.

`isBrokerDrained false` в алерте — не признак незавершённого reassign: precondition ложный,
таск ни разу не выполнил ни одного экшена, поле просто начальное.

## Целевой хост — как определяется

Код (обоснование чтения: manual flow должен совпасть с тем, что задача собиралась делать):
`DownscaleMdbReplicasTask.checkAndFillArguments` → `getDownscaleNode()` →
`KafkaOp.getNodeInCloudByTypeWithIndex(cloud, BROKER, currentReplicas)` — берёт хост,
у которого **индекс из имени == текущему числу реплик** (12) → `12.broker...hc.idzn.ru`
(broker id 20012). Не «последний по сортировке», а именно по номеру в имени.

При `isWithdrawing=false` (не последний брокер роли) финал таска — `services.stop(host)` +
`services.rescale(role, replicas-1)`, полный withdraw service/storage — только для
последнего брокера.

## Ход разбора и починки

1. Прод-БД: операция `in_progress/in_processing=false/attempts_left=0`, Temporal пуст —
   операторный флоу (канон one_cloud_ops.md).
2. `mcc op_stop ... kafka.downscale-broker` — стоп задачи ПЕРЕД ручными действиями.
   ⚠️ Сразу после op_stop get_result-процессор закрыл операцию в БД как **failed**
   («Operator task in progress»), самосхождения не будет — финальный SQL обязателен.
3. Оценка нагрузки: `kafka-topics --describe` по всем 96 топикам (373 партиции) — **на всех
   hc-брокерах 0 реплик** (данные кластера размещены только в uc/kc/pc, RF=3, 1 реплика/ДЦ
   не задета) → reassign НЕ потребовался. `du` на 20012: 82M = только `__cluster_metadata-0`.
4. `kafka-cluster.sh unregister --id 20012` → «Broker 20012 is no longer registered».
5. `mcc stop 12.broker...hc` до FINISHED (~90 сек) → `mcc rescale broker.logs-recom-wizard-kafka 11`
   (уравнение-подтверждение через pexpect, канон mcc-host-worker/commands/lifecycle.md)
   → «FORCED RESCALE to 11».
6. Верификация: облако hc = 11 инстансов; Kafka = 45 зарегистрированных брокеров;
   host_state = 49 строк, `12.broker...hc` отсутствует; таск в `operators.kafka.tasks` исчез.
7. SQL: `UPDATE operations SET status='done' ... WHERE status='failed'` (одобрено
   пользователем). DELETE из host_state дал **0 строк** — mdb-backend сам удаляет строку
   хоста при закрытии операции (даже failed) — не считать это ошибкой.

## Грабли

- `mcc -n dzen -c hc` мастер (10.216.106.30:443) флапает: часть запросов — i/o timeout /
  connection refused / EntityNotFoundException на живые инстансы. Ретраить, `--local`
  обязателен.
- Имена: облако/модель оператора — короткие `*.hc.idzn.ru`; прод-БД host_state —
  `*.hc.wan.idzn.ru`. В SQL подставлять wan-вариант.
- `grep '\bid:'` в sshexec — zsh/bsh эскейпы ломают паттерн; проще `grep 'id:'`.
- describe всех топиков: `kafka-topics --describe` БЕЗ `--topic` не даёт партиции;
  цикл по `--list` в файл на хосте, забирать `cat` через sshexec (scp ломается EOF),
  в выдаче шум AdminClient-логов — фильтровать `Partition: [0-9]`.
- Подсчёт хостов host_state: не парсить большой jsonb глазами (строки переносятся) —
  `count(*) FILTER (WHERE ...)` в SQL. Здесь из-за этого чуть не появился ложный
  «ghost uc.10» (на деле uc.10 = 23010 полноценный mdb-хост).

## Хвост

- Сеть: 7777 {hc,kc,pc}→uc по внутренним сетям — отфильтрован, оператору нужен фикс
  (эскалация в сетевую команду dzen/uc). Пока не починено: рефреш не работает,
  `kafka.update` (от отменённой sys_update 27.08) стоит с 09-08, любые операторные
  операции по кластеру будут зависать в precondition.
