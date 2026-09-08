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

## Хвосты

- Реанимация по классам A(4) → B(7) → C(4) — не начата, ждёт решения.
- У мёртвых инстансов класса B проверить, не остановлены ли они НАМЕРЕННО (экономика/учения).
- DML-чистка ghost-строк и 34 even-кластера — см. wave2-файл.
