# MDBSUP-5512 — dataportalredisdb: processing недоступен с воркеров (SG) + replicaof-каскад на новом хосте

Дата: 2026-09-17. Кластер Redis Sentinel `dataportalredisdb`
(`c790ac8e-5b66-494c-9aac-7d7c1f4ade55`, ns dzen, проект data-portal,
очередь `dataportalredisdb-data-portal-redis.data-portal.db.production.mdb.batch`).
Операция add_hosts `9b447196-bf31-452c-baf5-9b7912cf591b` (добавление ec-хоста),
Temporal workflow `addRedisSentinelReplica` (`<op>_ec_1`), 4 failed run'а
(15.09 14:38, 16.09 10:02, 17.09 11:04, 17.09 12:23 UTC).

Два независимых дефекта, оба вскрываются только при add_hosts:

1. **Сетевая недоступность хостов кластера с mdb-processing-воркеров** — formReplication
   падает, операция никогда не сойдётся ретраями.
2. **Каскадный replicaof в отрендеренном redis.conf нового хоста** — даже созданный
   хост реплицируется не с мастера, оператор отдаёт «Sentinel wrong info» и держит
   кластер UNAVAILABLE.

## Диагностика (что и как проверялось)

1. **Шаг 0, прод-БД**: operation in_progress → (к концу разбора) failed,
   attempts_left=0, in_processing; таблица `tasks` пустая — redis-флоу целиком в
   Temporal, в mdb-БД только итог.
2. **Temporal UI API**: listing по `WorkflowId = <operationId>` → 4 FAILED + 1 RUNNING;
   parent стартует child `<op>_ec_1`, в parent'е TIMER + START_CHILD (паттерн ретрая).
   История failed run'ов через UI API недоступна (всегда отдаётся последний run) —
   **ошибки активити брать из `/mnt/logs/mdb-processing.app.log`** по mdc.WorkflowId
   (поля ActivityType/Attempt/identity/RunId). Там же retryState и тексты:
   «All sentinel hosts are unavailable: hc,kc,pc.wan.idzn.ru»,
   lettuce `RedisConnectionException: Unable to connect`,
   `RETRY_STATE_MAXIMUM_ATTEMPTS_REACHED`. RetryPolicy из
   ACTIVITY_TASK_SCHEDULED: 20 попыток, startToClose 300s, backoff 10s×2→120s.
3. **Вход активити** (base64 из event.input): namespace, host (новый ec),
   sentinelHosts (hc/kc/pc wan), порты 26379/6379, vaultPasswordPath, clusterName.
   TaskQueue = `redis-activities-queue` (отдельная от общих).
4. **TCP-матрица с воркеров**: все 6 mdb-processing хостов
   (`1-2.mdb-processing.java.{hc,pc,uc}.one-infra.ru`; kc-воркеров вообще нет) —
   `bash /dev/tcp` на 26379/6379 хостов dataportalredisdb → CLOSED/TIMEOUT (v6-wan
   2a00:b4c0::/32; DNS резолвится). **Контрольный выстрел**: с того же воркера
   `1.mdb-processing.java.pc` хосты dzen `dzen-wagtail-redis-prod`
   (ec/kc/pc/rc.wan.idzn.ru) — OPEN в тот же момент. → не «dzen недоступен из infra»
   и не «sentinel завис», а **per-cluster SG в облаке**. У wagtail add_hosts прошёл
   16.09 (dzen-кластер), т.е. это не системный запрет, а разные политики на кластерах.
5. **«Sentinel wrong info»**: `/etc/redis/redis.conf` на ec —
   `replicaof 1.db...hc.wan.idzn.ru 6379` (рендер при провижининге 16.09 10:06 UTC),
   при мастере pc. hc видел ec своим слейвом (каскад), в `INFO replication` мастера
   ec отсутствовал → модель оператора (ec = слейв мастера) не сошлась с sentinel-слоем,
   алерт watch-таски `redis-sentinel.watch[refreshAvailabilityState]`:
   «Cluster is UNAVAILABLE, details='Sentinel wrong info in host: ...ec.idzn.ru'».
   Взгляд оператора: `mcc -n dzen -c <dc> ops "queue://<queue>" -f json`.
6. **Sentinel-слой при этом здоров** на всех 4 хостах: master pc, num-slaves 2
   (до фикса), num-other-sentinels 3, кворум 2, link-pending-commands 0. Урок из
   MDBSUP-5053 подтвердился: «sentinel wrong info» = рассинхрон sentinel-слоя с
   облачной моделью, а не поломка sentinel'ов.
7. **Фон (не причина)**: хронический DNS-флейм «Failed to resolve hostname
   *.wan.idzn.ru» в `/mnt/logs/dbms/redis-sentinel.log` — на pc ~100–180 фейлов/сутки
   по hc стабильно с 08.2025 (`options rotate`, 2 nameserver'а, эпизодические сбои).
   К инциденту не привёл, но объясняет исторические «sentinel wrong info»-волны.

## Фикс (порядок)

1. На ec (пароль — `master` из `/etc/redis/acl/users.acl`):
   `REPLICAOF 1.db...pc.wan.idzn.ru 6379` → `CONFIG REWRITE` (в redis.conf теперь
   replicaof pc; без REWRITE рестарт вернул бы каскад). Данные кластера копеечные
   (2.5 МБ) — полный ресинк секунды; при больших данных помнить про full sync.
2. SQL прод-БД: `operations → done/in_processing=false/finished_ts=now/error_message=NULL`
   (⚠️ первый UPDATE с guard `status='in_progress'` дал `UPDATE 0` — операция уже ушла
   в failed после исчерпания ретраев child'а; сверять актуальный статус перед guard'ом);
   `INSERT host_state` ec по шаблону соседних строк (params `{"dc":"ec"}`,
   onecloud_ui_link `https://cloud.vk.team/cloud/EC/ns/dzen/service/db...`,
   grafana_dashboard_link NULL — у соседей тоже NULL).
3. Parent workflow завершился сам (FAILED после исчерпания child-ретраев) — cancel
   не нужен. Cancel через UI API всё равно закрыт CSRF-токеном.
4. Пересборка модели оператора: `mcc -n dzen -c pc op_stop "queue://<queue>"
   redis-sentinel.watch` → `op_start` (приём из MDBSUP-5053). Через ~1 мин алерт
   сменился на «Cluster is AVAILABLE».
5. Верификация: `INFO replication` pc — connected_slaves=3 (hc/kc/ec, lag 0);
   все 4 sentinel'а — num-slaves=3, num-other-sentinels=3; host_state — 4 хоста;
   mcc ops — AVAILABLE.

Грабли команд: `sentinel masters <имя>` через sshexec периодически калечится
mcc-обёрткой в `sentinel|masters` (ERR wrong number of arguments) — надёжнее
`sentinel masters` без имени (мастер в sentinel'е один) и вывод обрабатывать локально.

## Хвосты

- **MDBDEV-3455** — сетевой доступ mdb-processing → 26379/6379 dzen-хостов
  (или перевод formReplication на путь без прямых подключений). Без него любая
  следующая processing-операция по кластеру (modify, пересоздание и т.п.) упадёт
  так же.
- **MDBDEV-3456** — баг рендера replicaof при провижининге нового хоста
  (указывает на реплику вместо мастера). Где именно ломается (confp-шаблон /
  PMS-переменная / факты хоста) — не копали, в тикете наблюдение.
- Хронический DNS-флейм wan-имён — не чинили.
