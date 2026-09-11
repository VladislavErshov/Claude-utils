# MDBSUP-5289 — communal CC: мусорный пароль cruise-user роняет контейнер (fix_htpasswd), + preprod downscale-хвост

- Дата: 2026-09-10
- communal `167c2774-1c41-494e-a427-00e11e946ba3` (fullQueue
  `communal-integr-platform-kafka.integr-platform.db.production.mdb.prod`), cruise в PC,
  cruise-хост `1.cruise.communal-integr-platform-kafka.pc.one-infra.ru`
- preprod `ba03e542-871c-4afd-8336-ac8d01dd514f` (fullQueue
  `preprod-integr-platform-kafka.integr-platform.db.production.mdb.prod`), удаление HC
- Связь: MDBSUP-5263 (вывод CC из HC накануне), каноны 4899/5092 (downscale-хвост),
  [one_cloud_ops.md](../commands/one_cloud_ops.md)

## Кейс 1: create_additional_service падает «Service cruise... is not running yet»

Операция `a16aa6cd-0b0a-4206-9ef3-d8e521b01f06`: 3 рана Temporal (CreateKafkaCruiseWorkflow),
все FAILED на `CloudWaiter.waitServiceRunning` (`CreateKafkaCruiseWorkflowImpl:91`,
`ServiceNotRunning`, RETRY_STATE_RETRY_POLICY_NOT_SET). Ретраи операции шли сами
(вечер 09.09 ×2, утро 10.09 ×1).

### Цепочка причины (по логам контейнера)

1. В форме UI создания CC в поле пароля оказался **текст ошибки** («Не удалось создать
   Cruise Control … Текст ошибки: Already has active or failed operation for cluster
   86a382dc…» — ошибка соседнего stg-kafka). У текста есть `:` и кириллица.
2. confp рендерит `/etc/nginx/.htpasswd` из шаблона
   `/etc/confp/templates.d/htpasswd.j2`: `cruise:{{ vault(utils.calculate_path_to_cluster_secret('cruise', 'kafka', 'password')) }}`
   → пароль из vault.
3. `restart_cmd` → `/opt/fix_htpasswd.py`: `(user, password) = fileData.split(":")` —
   двоеточие внутри пароля даёт 3 части → `ValueError: too many values to unpack`;
   кроме того `password.encode("ascii")` — кириллица валила бы даже без `:`.
4. confp выходит с кодом 1 → `confp-init.service` FAILED → `OnFailure=`-юнит
   «Shutdown with failure exit code» → **PID1 гасит контейнер сам** (exit 1) →
   облако: `Container 'main' is dead: Exit code = 1` → ATTEMPTS_LIMIT, retry каждые ~2 мин.
5. Сервис никогда не RUNNING → mdb-waiter не дожидается → операция failed.
6. Побочно: enable-vector.service падал (127, `/etc/vector/vector-init.sh: No such file
   or directory`) — confp умирает до рендера vector-конфигов, это следствие, не причина.

### Vault-путь секрета (шаблон)

`<vaultRoot>/cruise:password`, vaultRoot = PMS `zen.kafka.vaultRoot` кластера
(`communal-integr-platform-kafka.clouds`, app=mdb):
`zkv/mdb/integr-platform/kafka/communal-integr-platform-kafka.integr-platform.db.production.mdb.prod/cruise:password`

Требования к паролю: **ASCII без `:`** (split + ascii-encode в fix_htpasswd.py).
Продуктовый фикс (10.09, MR mdb-data !496, коммит e8f38cca): валидация при создании CC —
только латинские буквы и цифры `[a-zA-Z0-9]+` (`@Pattern` в `CreateKafkaCruiseRequest` +
`KafkaHostsValidator`), НЕ UUID — первая реализация с UUID была неверной по требованиям.

### Что сделано

- op-close: `a16aa6cd` → `in_processing=false` (статус уже failed, guard снят).
- Vault-секрет правит юзер; затем пересоздание CC через UI нормальным паролем —
  `createCruiseUserSecret` перезапишет секрет, флоу дойдёт до done.

### Диагностика-грабли (важно)

- **`mcc logs` читается, даже когда инстанс `not scheduling on a minion`** — стримы
  (@console, systemd.log, confp.log, bash.log, rsyslogd.log) отдаются мастером за окно
  попытки старта; ловить: поллить `instances -f yaml` по `state:` до STARTING/DEPLOYING,
  потом `mcc logs <host> <stream>` в фоне с kill по таймеру (logs = follow-mode).
- **`mcc sshexec` пишет `*** Connection closed by remote host ***` в хвосте УСПЕШНОГО
  вывода** — не фильтровать по этой строке (норма, exit 0).
- Манифесты cruise-сервисов communal и stg-kafka в PC **идентичны** (diff пуст) —
  разница была только в данных (секрет), не в манифесте. dzen-registry + образ
  `ubuntu20-mdb-cruisecontrol-2.5.147:1.0.2` — норма (MDBSUP-5226).
- PMS-пропсы cruise-хостов (`kafka.cruisecontrol.*` и пр.) пусты у обоих — тоже не различие.

### Продуктовые баги (предложены к заведению)

1. UI/API не валидирует `cruiseUserPassword` — текст UI-ошибки сохранился как пароль
   (и в vault, и в htpasswd).
2. `fix_htpasswd.py` + OnFailure=shutdown: невалидный символ пароля = crash-loop
   контейнера целиком, а в `error_message` операции — только «not running yet»,
   реальная причина видна лишь в confp.log хоста.

## Кейс 2: preprod delete_hosts HC — downscale-хвост (паттерн 4899/5092)

Операция `c909794e-4a3f-4cd1-a407-6140c181e9b0` (delete_hosts, HC): упала на
`get_kafka_downscale_broker_result` («Operator task in progress»), HC физически
выведен (host_state без HC, controllerDcs=[kc,pc,ec]).

Состояние на разбор: оператор держал задачу `kafka.downscale-broker --cloud=hc
--replicas=0` (с 12:39 09.09, precondition false); в облаке HC оставались
STOPPED-сервис `broker.preprod-integr-platform-kafka` + storage `…/broker` (NORMAL,
NVME 20G держал квоту). Temporal пуст (retention) — норма для operator-флоу.

Нюанс: пока шла чистка, операция **перезарядилась сама** (failed → in_progress,
attempts_left=1, in_processing=false, Temporal пуст) — паттерн 5216 (парковка).
Закрыта SQL с guard `AND status='in_progress'` (не failed!).

Фикс (10.09):
1. `mcc --local -n infra -c HC op_stop "queue://preprod-…" kafka.downscale-broker` → TASK_ABSENT.
2. `mcc withdraw broker.preprod-integr-platform-kafka` (сервис, EntityNotFound = успех).
3. `mcc withdraw --type storage "…/broker"` (уравнение `9 mod 7` через pexpect) → PURGEABLE.
4. SQL: `c909794e` → done/in_processing=false/finished_ts=now; `9e751846` (done, но
   in_processing=true — кривой bookkeeping) → in_processing=false.

Хвост: алерт оператора `PARTIALLY_AVAILABLE / No minimum recommended available brokers`
и попытки JMX к удалённому HC-брокеру — ghost-модель (4899/4895), должен уйти сам
после refresh; на кластер/операции не влияет.
