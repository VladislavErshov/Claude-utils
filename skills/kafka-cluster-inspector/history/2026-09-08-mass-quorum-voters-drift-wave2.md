# 2026-09-08: вторая волна массовой чистки кворумов (26 кластеров юзера + 3 mismatch)

Продолжение [2026-09-04-mass-quorum-phantom-voters-cleanup.md](2026-09-04-mass-quorum-phantom-voters-cleanup.md)
(процедура MDBSUP-5044). Вход: список из 26 кластеров с дрейфом рендеров (метрика appinfo
заказчика) + 3 кластера с `voters_mismatch` из mdb-health (metrics, platform-prod,
prod-stat-events).

## Новые находки (чего не было в 5044)

1. **Dzen wan-ловушка**: в `host_state` дзен-кластеров FQDN с `.wan.idzn.ru`
   (events-front pc/uc, kafka-seo все 3, pub-processing все 3), а живые хосты — БЕЗ wan
   (`*.idzn.ru`). mcc по wan-именам → `EntityNotFoundException`, рендер «пустой».
   Всегда пробовать оба варианта FQDN. Пустой рендер = сначала проверить, что хост вообще
   существует (`mcc instances`/sshexec hostname).
2. **Ghost-хосты в host_state**: 11 контроллеров в БД не существуют в облаке
   (frontlogs dc/kc, mailer hc, events pc/uc wan, kafka-seo wan-тройка, pub-processing wan-тройка).
   У frontlogs при этом ДВА cluster_id (d96066af — дубль от 17.02, 3cbe4366 — актуальный):
   ghost-строки принадлежат дублю. Отсюда же `FencedBrokerCount=45` на sdkkafka.
3. **PMS может быть неправ, а кворум — прав** (обратная задача): stats-banner-1 —
   живой кворум {13001@pc, 12001@rc, 14001@uc}, PMS-кластерный ключ требовал {kc, pc, rc}.
   Решение владельца: выровнять PMS под живых (`update.do`, байт-в-байт рендер лидера),
   kc выведен. В PMS остались пер-хост ключи `2./6.controller...pc` (мусор прошлых
   масштабирований, `kafka.controller.quorum=<NOT_SET>` — не мешают, но грязь).
4. **kafka-seo**: лидер pc рендер 3 voter'а (ок), фоловеры hc/ec — 4 с лишним `11001@kc`;
   конфп+рестарт фоловеров, лидера не трогали.
5. **events-front**: лишний `10001@dc` сидел в рендере **лидера uc** → рестарт лидера,
   выборы переехали на pc (11001). Именно этот кластер горел в mdb-health
   (`voters_dead`+`voters_mismatch`, пр.4).
6. **Тройка mismatch из mdb-health** (metrics-calls-prod, platform-prod-cloudtraining,
   prod-stat-events-vk-support): у лидера + одного фоловера старый 4-voter рендер с voter'ом
   удалённого хоста (rc/kc/hc — все EntityNotFound). Фикс: confp+рестарт фоловера →
   confp+рестарт лидера → выборы переехали на живых (dc/pc/dc), кворумы собраны по 3 == PMS.
   Урок: **параллельный вывод нескольких sshexec перемешивается** — атрибуция «какой хост
   какой рендер» была неверной, на доводке нашли второй stale-хост (metrics/uc,
   vk-support/rc). Верификацию делать пофамильно (per-host файлы), не доверять
   interleaving-выводу.

## Проверенные грабли

- `kafka-metadata-quorum.sh --bootstrap-server <controller>:9093` на 3.8 НЕ работает
  (`The remote node is not a BROKER that supports the METADATA api`) — только через брокера.
- `describe --replication/--status` НЕ видит дрейф фоловера (взгляд лидера);
  `kafka-metadata-shell` требует эксклюзивный лок живой директории (обход — копия снапшота
  в /tmp), но voter set в 3.8 в metadata log отсутствует — путь тупиковый.
- Комбинированный вывод `sed; echo ===; curl jolokia | awk \x01` — macOS awk не знает \x01,
  парсинг молча ломался → «все UNREACHABLE». Простые отдельные вызовы надёжнее.
- zsh: `set -- $pair` не сплитит переменную; `echo ===` в zsh пытается исполнить `===`.
- Сторожевой `.done`-файл от упавшего прогона вводит в заблуждение — удалять перед стартом.
- pms-read.sh может показать `<NOT_SET>` сразу после update.do (кэш/синхронизация) —
  верифицировать прямым values.do.

## Итог

- 26/26 кластеров юзера: рендеры == PMS, кворумы живы (2 случая с рестартом лидера:
  apps-installed ec, target-3 uc; 1 с рестартом лидера: events-front uc).
- +3 mismatch-кластера mdb-health погашены (metrics-calls-prod — лидер dc,
  cloudtraining — лидер pc, vk-support — лидер dc).
- PMS-правка: stats-banner-1 (update.do, выравнивание на живой состав pc/rc/uc).

## Хвосты (не закрыто, требует решений)

- 15 кластеров с `voters_dead` из mdb-health (список 2026-09-08): adb-users, apps-staging,
  communities-p, do-12738, dp-api-pg2pg, events(пр.17!), gmt-geoblock-s, kafka-1, main-yt,
  mediascope, news-pub-test2, one-flow-prestable, search-phrase, ya-yt-channel,
  zen-max-ai-bot — механика та же, конвейер обкатан.
- 34 кластера `voters_even` — отдельный трек (удаление контроллера через UI).
- DML-чистка ghost-строк host_state (показать SQL до выполнения): frontlogs d96066af (3),
  events wan (2), stats-banner kc (1), mailer hc (1); events `controllerDcs` содержит rc
  без хоста — upscale или правка Dcs.
- events (пр.4) и stats-banner-1 — завести тикеты (повторяющаяся деградация).
- Преждевременный выводmdb-health-чеков: `KafkaQuorumVotersCheck` слеп к дрейфу фоловеров —
  детектор возможен только чтением рендера с хоста (rscheck/host_checker или ssh-чек,
  см. разбор в сессии 2026-09-08).
