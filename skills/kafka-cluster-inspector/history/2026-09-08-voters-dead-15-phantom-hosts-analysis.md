# 2026-09-08: анализ 15+1 кластеров с voters_dead — это НЕ дрейф конфигов, это умирание контроллер-хостов

Вход: `kafka_controller_quorum_voters_dead` в mdb-health на 15 кластерах (+platform-prod из
mismatch-волны — его warning к моменту проверки уже неактуален, кворум собрали ранее).
Контекст: [2026-09-08-mass-quorum-voters-drift-wave2.md](2026-09-08-mass-quorum-voters-drift-wave2.md).

## Ключевой вывод

На всей когорте **PMS == рендеры == живой состав voter'ов** — конфиги чистые. Фантомный
voter («никогда не фетчит», `lastFetch=-1`) — это контроллер, **чей процесс давно не
работает**. Кворумы живут на 2/3 (или в candidate-цикле без лидера). mdb-health честно
сигналит. Системность: **kc-контроллеры ломаются чаще всех** (7 из 15), типовой сценарий —
«инстанс остановили в облаке / сервис упал, из кворума не вывели, mdb-data не знает».

## Классы причин (проверено `systemctl is-active` + `mcc` на каждом фантомном хосте)

### A — сервис kafka-controller FAILED на живом хосте
| Кластер | Хост | Упал |
|---|---|---|
| adb-users-datatransfer | kc | 2026-07-04 (2 мес) |
| do-12738-datatransfer | kc | 2026-06-01 (3 мес) |
| one-flow-prestable-vh | pc | 2026-01-27 (полгода) |
| kafka-1-mdbsandbox | ic | 2026-08-19 (до падения был лидером; один контроллер в candidate-цикле) |

Ремедиа: посмотреть лог последнего падения → `systemctl start kafka-controller` →
`systemctl restart rscheck@kafka` → проверить вход в кворум.

### B — инстанс остановлен в облаке («Task Instance is not scheduling on a minion»)
communities-p zc; dp-api-pg2pg uc; gmt-geoblock-s zc; mediascope kc; search-phrase kc;
ya-yt-channel kc; main-yt pc.

Ремедиа: `mcc start` инстанса (или UI облака) → confp → старт сервиса → кворум.

### C — процесс жив, но не входит в кворум (candidate-цикл, лидера нет)
- events-datatransfer (пр.17): ec стартовали 2026-09-08 12:30, candidate — лог разошёлся.
- main-yt: hc таймаут, pc stopped, kc active + candidate.
- news-pub-test2: kc ghost (EntityNotFound), hc таймаут, pc active + candidate.
- zen-max-ai-bot: kc ghost, hc таймаут, pc active + candidate.

Ремедиа: вайп локального метадата кандидата (`rm -rf /mnt/data/log /mnt/data/metadata`,
канон RESTART_AND_RESTORE, только НЕ на лидере) → старт → вход в кворум. Перед этим
проверить достижимость hc-хостов (таймауты mcc).

## Метод (воспроизводимо)

1. cluster_id из mdb-health (`mirror.db_cluster` join warnings) → host_state в
   backstage_plugin_mdb (join по id; имена кластеров бывают дубли — events пр.4 и пр.17!).
2. Рендер+jolokia на контроллерах, `describe --status`+`--replication` с брокера,
   dead = строка voter'а с `lastFetch=-1` или большим lag.
3. Сверка: PMS (values.do) == рендеры == live voters. Если совпало — дрейфа нет,
   фантом = мёртвый процесс/инстанс, классифицировать по A/B/C.

## Грабли сессии

- mcc иногда отдаёт `dial tcp 10.216.106.30:443 i/o timeout` с последующим
  EntityNotFound — первый «NotFound» перепроверять вторым заходом.
- У трёх кластеров (main-yt, news-pub-test2, zen-max-ai-bot) broker-хосты hc из host_state
  тоже призраки — bootstrap брать с kc/pc брокера.
- Функция в bash: `chk() {...$1...$2...}` — вызывать строго (хост, ns), перепутаешь —
  mcc получает ns как хост (`invalid subcommand: infra`).

## Статус Type C (2026-09-08, вечер)

- **events-datatransfer (пр.17): ИСПРАВЛЕН.** ec: вайп `/mnt/data/log`+`/mnt/data/metadata`
  → старт → вошёл фоловером к лидеру 12001. Кворум полный.
- **zen-max-ai-bot и news-pub-test2: НЕ тронуты (решение владельца).** Вскрылось: hc-контроллеры
  И hc-брокеры этих кластеров **не существуют в облаке** (EntityNotFound) — жив один
  контроллер pc (candidate). Из 1 voter'а кворум в 3.8 не собрать; лечение — операция
  add_hosts (добавление 2 контроллеров через mdb-data/UI), не ручной конфиг.
- **main-yt: частично.** pc-инстанс поднялся (mcc start, running с 2026-09-08 22:39),
  kc жив; hc-контроллер и hc-брокер — EntityNotFound. Дальнейшие шаги не выполнялись
  (решение владельца): при старте pc должно хватить 2/3 для выборов (kc+pc).

## Хвосты


- Реанимация по классам A(4) → B(7) → C(4) — не начата, ждёт решения.
- У мёртвых инстансов класса B проверить, не остановлены ли они НАМЕРЕННО (экономика/учения).
- DML-чистка ghost-строк и 34 even-кластера — см. wave2-файл.

## Type A: старт сервисов (вечер 2026-09-08)

| Кластер | Итог |
|---|---|
| one-flow-prestable (pc) | ✅ ИСПРАВЛЕН: сервис стартовал, вошёл фоловером, кворум leader=11001, lag=0 |
| kafka-1 (ic + 2×hc) | ⚠️ ЧАСТИЧНО: ic стартован (candidate→выборы), 10002 (2.controller.hc) выиграл; затем hc-хост перегрузился в 22:50 (journal свежий), после ребута OCI runtime errors на контейнерах обоих hc-хостов и брокера ic → hosts/контейнеры нестабильны на уровне runtime. Нужно one-cloud-ops: рестарт хостов (mcc restart) или пересоздание |
| adb-users (kc) | ❌ СТАРТ НЕВОЗМОЖЕН: `/mnt/data` Input/output error (диск; упал 21.07 08:50 одновременно с do-12738 — одно событие ДЦ). Нужно облако: рестарт хоста/пересоздание volume |
| do-12738 (kc) | ❌ СТАРТ НЕВОЗМОЖЕН: тот же `/mnt/data` I/O error |

Урок: перед стартом упавшего сервиса проверять `df /mnt/data` (I/O error = сразу one-cloud-ops)
и свежесть journal (перезагрузка хоста сбрасывает статус сервиса в inactive).

## one-cloud-ops: рестарты (вечер 2026-09-08, продолжение)

- kafka1-2hc: ✅ рестарт хоста помог — диск смонтирован, сервис active, кворум {10002 лидер, 11001, 10001*}
- adb-users kc / do-12738 kc: ❌ рестарт хоста НЕ вылечил `/mnt/data` I/O error → диск побит на уровне
  volume. Нужны one-cloud-ops/UI: пересоздание volume (metadata контроллера пересоберётся с лидера)
  или рестарт хоста с перевидением. Кворумы живут 2/3, не горят.
- kafka-1: корень найден НЕ relocate'ом (mcc migrate не находит по FQDN; правильное имя очереди —
  `kafka-1-mdbsandbox-kafka.mdbsandbox.db.production.mdb.prod`, сервис `controller.kafka-1-mdbsandbox-kafka`):
  **OOM crash-loop** — контейнеры с MEM=2G умирают («Container 'main' is dead: Out of Memory. Stopped»),
  планировщик перезапускает. Сервис засабмичен n.dergunov 19.08 (день первых падений).
  Доп. расхождение: mcc-сервис = 2 реплики (оба hc), mdb-data = 3 контроллера (+ic).
  Фикс: поднять MEM в манифесте (mcc edit/submit) + свести составы mcc ↔ mdb-data. Владельцу.
- Утилиты: `mcc instances <pattern>` даёт hierarchy/service/queue/outcome (в т.ч. «Container dead: OOM»);
  `mcc migrate --relocate` ищет storage/shard/minion, по FQDN инстанса не ищет.

## Миграция дисков adb-users / do-12738 (ночь 2026-09-08)

Обе миграции ЗАПУЩЕНЫ (`mcc migrate --relocate --auto_solve`, storage =
`<queue>.datatransfer.db.production.mdb.prod/controller`, 1/1 shard requested).
Методика и грабли вынесены в скилл: `mcc-host-worker/commands/migrate.md`.
После завершения миграции: sshexec по FQDN, `df /mnt/data` без I/O error,
`systemctl start kafka-controller`, вход в кворум (фантомные voter'ы оживут —
`voters_dead` погаснет в mdb-health).

## Тип B: старт инстансов (ночь 2026-09-08)

`mcc start <fqdn>` по 6 остановленным инстансам:
- ✅ СТАРТОВАЛИ СРАЗУ И ВОШЛИ В КВОРУМ: mediascope kc, ya-yt-channel kc, search-phrase kc
  (все — фоловеры лидера 10001, unk=0). Процедура: mcc start → ждать загрузку ~2-3 мин →
  `systemctl start kafka-controller` → jolokia роль.
- ⏳ УШЛИ В АВТОСТАРТ-ОЧЕРЕДЬ («cannot start by either reason. Once resolved, it will
  start automatically», mcc start повторять через минуты): dp-api-pg2pg uc,
  gmt-geoblock-s zc, communities-p zc — за вечер не стартовали; планировщик/ёмкость ДЦ,
  дальние шаги в UI/one-cloud-ops.
- main-yt: ✅ ВОССТАНОВЛЕН — после старта pc инстанса выборы прошли, kc (11001) стал
  ЛИДЕРОМ. Кворум полный.
- events (пр.17): ✅ ec починен ранее (вайп метадата).
- Итого voters_dead: из 15+1 снято 12 (mediascope, ya-yt, search-phrase, main-yt, events-17
  + ранее one-flow, events-4 и другие). Остались: adb-users (миграция диска запущена),
  do-12738 (миграция диска запущена), dp-api-pg2pg/gmt-geoblock-s/communities-p
  (автостарт-очередь), kafka-1 (OOM, владельцу), zen-max/news-pub (add_hosts, владельцу).

## Повторная проверка оставшихся (ночь 2026-09-08, финал)

- do-12738: ✅ ИСПРАВЛЕН — миграция диска завершилась (df 2%, без I/O error), сервис
  active, фоловер лидера 12001, MaxFollowerLag=0. Migrate --relocate сработал полностью.
- main-yt: ✅ ИСПРАВЛЕН — kc лидер (11001), pc фоловер; hc — ghost (чистка host_state).
- adb-users: миграция диска ещё идёт (хост недоступен = перенос). После — старт сервиса.
- dp-api-pg2pg uc / gmt-geoblock-s zc / communities-p zc: автостарт-очередь планировщика
  не двигается — дальше UI/one-cloud-ops.
- kafka-1: ic снова failed, 2hc activating, 1hc контейнер мёртв — OOM-класс, владельцу.
- zen-max/news-pub: add_hosts, владельцу.

## Повторный mcc start для трёх автостарт-очередей (поздняя ночь)

dp-api-pg2pg uc / gmt-geoblock-s zc / communities-p zc — mcc start всё так же отвечает
«cannot start by either reason. Once resolved, it will start automatically». Мастер mdb
сильно флапает (status то находит сервис, то EntityNotFound вперемешку с i/o timeout) —
похоже на деградацию самого облака вечером. Инстансы в очереди автостарта планировщика —
ждать; если за сутки не поднимутся — UI облака/one-cloud-ops вручную.
