# logs-recom-wizard-kafka: удаление всех hc-брокеров при блокировке оператора сетью uc:9000 (16.09.2026)

**Кластер:** `logs-recom-wizard-kafka` `f0d28ab1-bbd6-4288-a2f3-958b635a5f3e` (dzen,
`*.idzn.ru`, mcc `-n dzen`), fullQueue `logs-recom-wizard-kafka.recom-wizard.db.production.mdb.prod`.
**Тикет:** MDBSUP-5462 (заглушка в jira-mdbsup-solver/history). Прецедент: MDBSUP-5092
(тот же симптом, другой кластер).

## Симптом

delete_hosts `a4586405` (удаление 10.broker.hc, `--cloud=hc --replicas=9`) висит на
`get_kafka_downscale_broker_result`: операция `in_progress`, `in_processing=false`,
error пустой, задача оператора `downscale-broker` стартовала, но **не сделала ни одного
действия** (нет поля `invoked`) — precondition false:
`operator().isFresh() false ... isBrokerDrained false`.

## Корень

`kafka.watch[refreshStatus]`: JMX (RMI, **порт 9000**, `JMX_PORT=9000` в env процесса)
недоступен до **всех 10 uc-брокеров** (`Failed to retrieve RMIServer stub ...
SocketTimeoutException: Connect timed out`) с **14.09 17:48** → watch не может обновить
user/topic state → `isFresh=false` → downscale не начинается. Третье проявление uc-сетевой
проблемы ([25.08](../../kafka-host-inspector/history/ui-cc-504-dead-vip-uc-logs-recom-wizard-kafka.md),
[29.08–02.09](2026-08-31_logs-recom-wizard-kafka-urp-uc-new-conn-broken.md)), но теперь
**порт-специфично**: pc→uc:9000 FAIL (все хосты), **uc→uc:9000 FAIL даже внутри ДЦ**,
при этом pc→uc:9092 OK и pc→hc/kc:9000 OK. iptables на брокере пуст, локально :9000 живёт
(листенер `:::9000`). Эскалация сетевикам — в списке мёртвых эндпоинтов указывать порт 9000
и «uc→uc тоже FAIL».

## Флоу вывода всех hc-брокеров (по запросу владельца)

1. **Верификация дрена**: Jolokia `ReplicaManager PartitionCount/LeaderCount` на всех 10
   hc (ID 20001–20010, `node.id` в broker.properties). 9 из 10 пустые, **2.hc имел 1
   партицию** — `vk-games-user-item-events-log-checkpoints-0` (свежий compact-топик,
   auto-assignment закинул реплику на hc уже после начала дрейна): Replicas
   `22010,23007,20002`. Ребаза вручную: `kafka-reassign-partitions.sh --execute`
   (20002 → 21002), verify, ISR полный.
2. **op_stop** `kafka.downscale-broker` → задача исчезла из `operators.kafka.tasks` →
   get_result увидел TASK_ABSENT → **операция сошлась в done сама за ~2 мин** (SQL не
   нужен; хост 10.hc mdb-data убирает из host_state на сабмите).
3. **Облачный stop** всех 10 инстансов: `mcc -n dzen -c hc stop <fqdn>`.
   9 остановились; последний живой (8.hc) падал с `EOF`. ⚠️ **EOF = неинтерактивное
   уравнение**: `--debug` показал истинную ошибку `ForceableServiceValidationException:
   STOP ... will violate availability: >9 running replicas` (правило «минимум 9 живых
   реплик» — остановка последнего живого инстанса его нарушает) и уравнение
   «8 mod 2»; mcc в неинтерактиве читает stdin → EOF. **Решение: `--auto_solve`**
   (pexpect из lifecycle.md тоже подойдёт). После stop сервиса на уровне
   systemctl внутри контейнера cloud-state НЕ меняется — только облачный stop.
4. **Withdraw сервиса**: `mcc -n dzen -c hc withdraw --type service
   broker.logs-recom-wizard-kafka --auto_solve` — валидация требует FINISHED у всех
   инстансов (поэтому порядок: сначала все stop). Паттерн — имя сервиса БЕЗ очереди
   (`broker.logs-recom-wizard-kafka`), FQDN инстанса не матчится
   (`No service matching ...`). Ждать исчезновения инстансов (~минуты).
5. **Withdraw storage**: `withdraw --type storage "<fullQueue>/broker" --auto_solve` →
   state `PURGEABLE` → через ~1.5 мин `EntityNotFoundException` = удалён, квоты
   (vcores/mem/nvme ~20.5T) освобождены.
6. **host_state**: `DELETE ... WHERE cluster_id='...' AND host LIKE '%.hc.%'` (9 строк,
   docker cp + psql -f). db_cluster_version не трогали — hc без контроллеров
   (controllerDcs касается только контроллеров).

## Верификация

- `kafka-broker-api-versions.sh`: ровно 34 брокера (21001–21012, 22001–22012,
  23001–23010), ни одного 200xx — KRaft дерегистрировал остановленных сам.
- URP=0 (`--describe --under-replicated-partitions` пусто).
- Облако hc: `instances *recom-wizard*` → 0; storage → EntityNotFound.
- host_state: 38 хостов (34 брокера + 3 контроллера + cruise), hc=0.

## Грабли

- ⚠️ **mcc stop/withdraw EOF в неинтерактиве = почти всегда уравнение-подтверждение**
  (ForceableServiceValidationException / опасное действие). Сразу `--debug` и смотреть
  хвост: если виден `error_code: ForceableServiceValidationException` и
  «Input evaluated value for (attempt 0/3)» — это `--auto_solve` (или pexpect),
  а не сетевая проблема.
- 5–6 ретраев «вслепую» на EOF — потеря времени: ошибка детерминированная.
- `grep 'Partition:0'` в описании топика не матчится (в выводе `Partition: 0` с пробелом
  и табами) — грепать `Replicas`.
- sshexec иногда отдаёт только «Connection closed by remote host» без вывода команды —
  команда при этом выполняется; перепроверять отдельным вызовом.
- Удаление всех брокеров ДЦ:-cloud-правило доступности («>9 running replicas») блокирует
  только последний живой инстанс — предыдущие стопы проходят без уравнения.
- mdb-data убирает удаляемый хост из host_state сразу на сабмите операции (host_state
  уже 9 хостов при живом in_progress).

## Хвост

- Сеть uc:9000 — эскалация (phi миньона/миньоны ни при чём; это путь к брокерам uc).
- **Границы проблемы (проверено nc с pc и uc→uc, подтверждено пользователем): недоступность
  JMX 9000 до uc затрагивает ТОЛЬКО оператора one-cloud-ops** (watch/refresh → isFresh=false
  → блок action-задач). Клиенты 9092 (v4+v6), репликация, кворум KRaft (контроллер uc 9093),
  метрики Grafana (8080) — OPEN и работают; Cruise Control не затронут (сэмплер
  `CruiseControlMetricsReporterSampler` читает метрики из топика `__CruiseControlMetrics`
  по 9092, JMX не использует). Jolokia 7777 до uc тоже недоступен извне — ручная диагностика
  uc только через ssh (jcmd/jstack локально).
- `kafka.update` alert «Waiting for operator refresh since 14.09» — update-флоу кластера
  будет висеть, пока uc:9000 недоступен. Следующие операции кластера — тоже (зависнут на
  precondition, лечится ручным флоу: ручной reassign → op_stop → withdraw руками).
- Комментарий в тикет добавлен (+ отдельный разбор для Developers, visibility role
  Developers), тикет закрывается.
