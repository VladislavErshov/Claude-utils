# 2026-09-11: B-recheck party — downscale-брокеров на финальном влитом коде

Ре-проверка B-сценариев после вливания ветки MDBDEV-2900 в master и мерджа
mdb-data !496. Фокус — save-контракт (remaining-map) в полном флоу + новые
валидации mdb-data.

## Что изменилось с партии 08.09 / save-партии 10.09

- processing `8bf5f3fa` (11.09 12:07, master): save `saveDownscaledKafkaBrokerInfo`
  последней фазой parent; data-api bump 1.101.3→**1.104.0** (релиз содержит
  `DownscaleKafkaBrokersRequest` — mavenLocal-грабля снята, оба пина
  `data-api:1.104.0` / `processing-api:3.62.1` содержат новый контракт, проверено
  по jar-ам в gradle-кеше).
- processing `050cdb6b`: rename `brokersPerDc`→`remainingBrokersPerDc` + рефакторинг
  (уже покрыто save-партией 10.09).
- mdb-data `a2e643f2` (merge !496): internal save `saveDownscaledKafkaBrokers`
  (map-контракт), валидаторы downscale, v2 endpoint, `isWan` в DTO,
  `excessBrokerHostNames` (жертвы = старшие hostIndex, детерминированно).
- mdb-data `8ec56506`/`140690ec`/`f00d9bcb`: isWan при save cruise, topic
  otherProperties, MDBDEV-3287 (только PostgreSQL, Kafka не касается).
- processing `1341292a` (topic otherProperties): НЕ задевает AdminClient-пути downscale.

## Инфра

Оба сервиса пересобраны и перезапущены (старые висели с 10.09, лог processing
последний раз писан 10.09 16:48 — классика «процесс крутит старый код», проверять
lstart при ре-чеках). ABC:3000, temporal:8233, pg:6434 живы с прошлых партий.
PMS-снапшот до/после: `kafka.layout`/`kafka.isWanCluster` без изменений
(isWanCluster=false, upsert идемпотентен).

## Сценарии

| # | Сценарий | wfId | Результат |
|---|---|---|---|
| prep | upscale ic 1→2 (modify3) | `f866fa8c` | PASS, 2.broker.ic в host_state |
| B1+save | happy ic 2→1 | `bc4bc5af` | PASS: reconcile-child→discovery×4→describe×2→reassign-child→drain→unregister→4×InDc→**save последним**; host_state чист сразу после COMPLETED |
| B4 | no-op той же целью | — | PASS (новое): **400 «Nothing to downscale»**, workflow не стартует |
| B9 | guard увеличения ic:2 | — | PASS (новое): **400 «must not be greater than current»**; processing-side DOWNSCALE_NOT_ALLOWED остался defense-in-depth (код не менялся) |
| min-3 | удаление до 2 брокеров | — | PASS (новое): **400 «min value of brokers is 3»** |
| B12 | контракт | `bc4bc5af` | PASS: remainingBrokersPerDc абсолютные, queueInfo полный, connectionParams = 5 bootstrap (вкл. жертву) + vault super, **isWan=false**, TTL=21600с; reassign-child: topics=null, targetRF=null, survivors=[21001,23001,22001,26001] (жертва 23002 исключена) |
| B2 | пачка pc+ic 2×2→1 | `4156a19e` | PASS: ОДИН reassign-план [25001,21001,22001,23001] (обе жертвы исключены), параллельные InDc, save снёс оба хвоста |
| B8 | partial: terminate pc-InDc | `919d1d47`→`49f5097f` | PASS: non-retryable «Broker downscale failed in 1 DC(s): [pc]»; ретрай: reassign/unregister skip (жертвы вне реестра), pc-child дослал rescaleService, save снёс оба хвоста (ic-хвост от дошедшего sibling тоже) |
| B13 | десинк host_state | `04a0588e` | PASS ×2: (1) downgrade7 ic-сервис отсутствовал в облаке (хвост с 08.09), а ic был в host_state → флоу с целью ic:0 → discovery пуст → save удалил хвост — самолечение; (2) каждый COMPLETED больше не оставляет хвостов |
| B15 | mid-withdraw terminate | `effad900`→`224f9594` | PASS: terminate после stopService (ловля — поллинг ic-child SCHEDULED-активностей); ретрай: stopService идемпотентен на остановленном → withdrawService → withdrawStorage → save → host_state чист |

Финал: все 3 кластера в базовом 4×1, операции done, PMS не тронуты.

## Пропущено (обоснование)

- B5/B6/B7/B14/B16/B17: код reassign/drain/unregister-путей не менялся с 08.09
  (единственное изменение флоу — save ПОСЛЕ детей), ретрай-семантика с save
  подтверждена в B8/B15; окна до save семантически те же.
- B10 processing-side (survivors<RF): не менялся, через mdb-data теперь
  недостижим (min-3 ловит раньше 400-й).
- B11 (drain timeout): нет локального рычага, отложен на стенд (как и 08.09).

## Находки

1. **Хвост с 08.09 в downgrade7**: ic-брокер был в host_state, но сервиса в облаке
   не было (B3/B15 партии 08.09 завершались withdraw, финальный rollback,
   видимо, не пересоздал сервис, а «4×1» в отчёте касался host_state). Сегодняшний
   remaining-save самолечил (запуск с ic:0 → discovery пуст → save чистит).
   Вывод: после партий с withdraw-путём сверять не только host_state, но и облако.
2. **Валидации mdb-data (B4/B9/min-3) — новый контракт**: падение до Temporal
   (400), тесты 08.09 на processing-side guard'ы остаются как defense-in-depth.
3. **Temporal history API**: в COMPLETED-событиях `activityType.name=null` на
   этом инстансе — имена брать из SCHEDULED-событий (для поллинга-ловли окон).
4. **Сервисы висят со старым кодом**: перед ре-чек-партией проверять lstart
   процессов против даты последнего коммита (processing сегодня стартовал
   заново только после kill).
