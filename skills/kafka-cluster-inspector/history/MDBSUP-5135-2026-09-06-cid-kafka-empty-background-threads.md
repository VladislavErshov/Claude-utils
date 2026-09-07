# MDBSUP-5135 (2026-09-06) — cid-kafka: пустой `background.threads`, брокеры FINISHED/ATTEMPTS_LIMIT, modify не рестартит мёртвые хосты

## Кластер

- `cid-kafka` (`84352936-84cc-47c6-abc1-b8b622e5ebc2`), проект dis, rootQueue prod, ns infra.
- Хосты: `1.broker.cid-kafka-dis-kafka.{hc,pc,zc}.one-infra.ru`,
  `1.controller.cid-kafka-dis-kafka.{hc,pc,zc}.one-infra.ru`, `1.cruise...hc`.
- fullQueue: `cid-kafka-dis-kafka.dis.db.production.mdb.prod`; PMS-ключ брокеров/контроллеров:
  `cid-kafka-dis-kafka.clouds` (+ `controller.cid-kafka-dis-kafka.clouds` для sysconfig).

## Симптомы

- Кластер создан 2026-09-03, брокеры ни разу не поднялись; UI показывает 0/3 брокеров.
- В `brokerConfig.config` при создании ушёл `background.threads: ""` — Kafka на старте
  падает на невалидном int. В tикете заявитель писал про ConfigException при старте.
- Пользователь дважды дёргал modify_cluster (выставлял 10, потом 15) — операции `done`,
  «изменение не применяется».

## Диагностика (цепочка БД → PMS → облако)

1. **Прод-БД**: `operations` — create + 3×modify, все `done`, но у трёх modify
   `in_processing=t` (хвост, не блокировал что-либо на момент разбора).
   `db_cluster_version` по версиям: при создании `background.threads` = NULL/пусто,
   modify-версии: 10 → 15. То есть **в БД значение уже корректное**.
2. **PMS** (`pms-read.sh 1.broker...hc.one-infra.ru kafka.broker.properties`):
   в шаблоне `cid-kafka-dis-kafka.clouds` → `kafka.broker.properties` уже
   `background.threads=15`. **PMS тоже корректный** — modify отработал.
3. **Облако** (`mcc --local -n infra -c <dc> instances "%cid-kafka-dis-kafka.<dc>%"`):
   - контроллеры hc/pc/zc — `RUNNING`;
   - **брокеры hc/pc/zc — `FINISHED`, `outcome=ATTEMPTS_LIMIT`, outcome_text=
     «Container 'main' is dead: Starting time exceeded 1800000ms (was 'Waiting for
     port 9092 on lan')»** — старты были только вечером 03.09, после modify 04.09
     ни одной попытки старта облако не делало;
   - cruise hc — `RUNNING`, но `availability=UNAVAILABLE` («He's dead, Jim»).
4. `mcc sshexec` на FINISHED-брокер → `ServiceValidationException: ... is not
   scheduling on a minion, please start it first`; `mcc status <FQDN>` →
   `EntityNotFoundException` (нужен `instances`); `mcc logs @console` на
   FINISHED-хосте — «Connection closed by remote host» (логи упавшего старта
   напрямую не достать, конфиг exception подтверждается косвенно).

**Вывод:** modify обновил PMS/БД, но **не рестартует брокер-хосты в облаке** — а те
после ATTEMPTS_LIMIT сами никогда не перезапустятся. Кластер «застрял» в состоянии
«конфиг починен, хосты мертвы».

## Починка

```bash
# старт брокер-сервиса в каждом ДЦ (volumes НЕ удалять — данные/конфиг рендерятся заново из PMS при старте)
for dc in hc pc zc; do mcc --local -n infra -c $dc start "broker.cid-kafka-dis-kafka"; done
```

- Через ~2 мин все брокеры `RUNNING`; на хостах `systemctl is-active kafka-broker` →
  `active`, `/opt/kafka/config/broker.properties` содержит `background.threads=15`,
  в `/mnt/logs/dbms/kafka-broker.out.log` — маркер «Kafka Server started».
- Cruise сам вышел из UNAVAILABLE (availability → RESERVED, `cruise-control active`).
- Хвост-фикс в прод-БД (docker cp + `psql -f`):

```sql
BEGIN;
UPDATE operations SET in_processing=false
WHERE cluster_id='84352936-84cc-47c6-abc1-b8b622e5ebc2' AND status='done' AND in_processing=true;
COMMIT;
```

## Анатомия modify в Temporal: почему «done» при мёртвых брокерах

Workflow `modifyClusterKafka` (= operationId) — 4 child'а, все COMPLETED за 11–16 с:

| Child | Что делает | Рестарт/health-check |
|---|---|---|
| `reconcileKafkaCluster` | activity `upsertIsWanCluster` | нет |
| `modifyKafkaController` | **пусто** (controllerConfig не менялся — 0 активити) | нет |
| `modifyKafkaBroker` → `updateConfigKafkaBroker` | `upsertBrokerConfig`, `upsertSysconfig` (запись в PMS) + чтение облака (`cloud_getExistingServiceDcs/InfosForServices/InfoForInstances`) | **НЕТ** |
| `modifyKafkaCruise` → `updateConfigKafkaCruise` | `upsertCruiseControlConfig`, `upsertCruiseControlCapacity` + чтение облака | **ДА**: `kafka_host_restartCruiseInstanceSsh` + `kafka_host_pingSshRestartedCruiseInstanceReady` |

**Асимметрия:** cruise-ветка рестартит инстанс по SSH и пингует готовность, брокерная —
только пишет PMS и читает облако: ни рестарта, ни проверки готовности. «Успех»
modify = «конфиг записан в PMS». На кластере с FINISHED/ATTEMPTS_LIMIT-хостами
операция завершается done, ничего не чиня (вот почему modify «не применяется» —
он и не должен был применять: применить некому). `modifyKafkaController` при
пустом `controllerConfig` вообще без активити.

**Create:** workflow create по operationId `3a97fc50-…` в Temporal visibility
отсутствует (запрос `WorkflowId = …` → `{}`; ретеншн/удаление — точную цепочку
не достать). Таймлайн из облака (MSK, 03.09): create done 15:16:38 → последние
видимые попытки старта брокеров с 30-мин таймаутами: hc 17:32→18:03, zc
19:08→19:38, pc 21:27→21:58 → ATTEMPTS_LIMIT. Ранние попытки (во время create)
в инстансах не видны — в строке только последний run. Create закрылся done,
не дождавшись здоровья брокеров.

## Уроки / паттерн

- **«Modify прошёл, но не применился» для Kafka = сверять три слоя**: БД
  (`db_cluster_version`) → PMS (`kafka.broker.properties` на `<queue>.clouds`) →
  отрендеренный конфиг на хосте. Здесь сходились первые два, ломалось облако.
- **ATTEMPTS_LIMIT/FINISHED брокер не поднимается сам** — лечение: `mcc start`
  сервиса (без пересоздания volumes, если виноват только конфиг: confp при старте
  перерендеривает из PMS; ср. MDBSUP-4832 — пустой sysconfig тоже лечится стартом
  с перерендером).
- У `done`-операций бывает хвост `in_processing=t` — чистить одним UPDATE.
- Логи брокера на FINISHED-хосте через `mcc logs` недоступны — причину старта
  восстанавливать по outcome_text облака и конфиг-слоям.
