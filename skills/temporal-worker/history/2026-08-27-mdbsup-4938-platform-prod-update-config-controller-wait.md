# 2026-08-27: MDBSUP-4938 — platform-prod add_hosts «завис» updateConfigKafkaController

## Запрос

Кластер platform-prod (9633434d), операция add_hosts a0de0782 (хост в DC uc).
L1-диагноз: root upscaleKafkaControllerInCluster TIMED_OUT → перезапущен; ребёнок
updateConfigKafkaController «RUNNING с 14:34 UTC». Тикет создан 15:20 UTC.

## Итог на момент разбора

Операция уже сошлась: run 2 COMPLETED 15:54:02 UTC, статус в прод-БД — done
(finished 18:54 МСК, без ошибки). Тикет закрыт комментарием.

## Что было на самом деле (не зависание)

- per-DC дети и broker-часть завершились ещё в run 1 (11:32–11:35 UTC).
- updateConfigKafkaController: рестарт нового uc-контроллера (9 сек) → цикл
  executeReloadCycle поллил cloud_getInfoForInstances каждые 15 сек: контроллеры
  kc/rc были RUNNING/PREFAIL много часов → KafkaIteratePolicy кидал
  NO_AVAILABLE_HOSTS («All remaining hosts are in PREFAIL state»), пустой батч,
  сон 15с, снова poll. Это корректное ожидание восстановления, не дедлок.
- Root TTL = 3ч (workflowTtl=10800 в input) → run 1 TIMED_OUT 14:32 UTC, ребёнок
  TERMINATED (parent close policy) → авто-ретрай run 2 в 14:34 (тот же паттерн).
- ~15:51-15:53 kc/rc вышли из PREFAIL → оба перезагружены (15:53:11 и 15:53:51) →
  COMPLETED. В run 2 per-DC/broker дети скипнулись WORKFLOW_ALREADY_EXISTS —
  норм (runIgnoringAlreadyStarted).

## Ключевые точки кода

- `UpdateConfigKafkaControllerWorkflowImpl` — dcs=[rc,pc,kc,uc], но контроллеры
  нашлись только в rc/kc/uc (в pc сервиса нет).
- `KafkaHostReloadHelper.executeReloadCycle` — RELOAD_HOST_PAUSE=15s, ждёт
  бесконечно (без общего таймаута на «нет доступных хостов»).
- `KafkaIteratePolicy.takeNext` — берёт только state=RUNNING и availability !=
  PREFAIL; RESERVED не фильтруется (uc-контроллер был RESERVED — взяли).

## Урок процесса (внесён в jira-mdbsup-solver, «Шаг 0»)

При тикетах про «зависшую операцию» ПЕРВЫМ делом смотреть статус операции в прод-БД
(`SELECT ... FROM operations WHERE id='<operationId>'`). Если `done` — не копать глубоко:
подтвердить в Temporal (или операторе) COMPLETED последнего run'а, написать комментарий
и закрыть тикет. В этом кейсе операция сошлась через 34 мин после создания тикета, а
полный разбор 3500 событий истории занял заметно дольше, чем был нужен для закрытия.

## Наблюдения / кандидаты улучшений

1. Бесконечное ожидание PREFAIL-хостов в reload-цикле: операция может «висеть»
   часами без видимого прогресса и умирать по TTL рута (что и видел L1). Нет
   таймаута/аларма на долгое отсутствие доступных хостов.
2. workflowTtl=10800 передаётся в input операции — 3ч жёсткий потолок всего
   upscale-флоу; при долгом PREFAIL ретраи начинают «с нуля» (правда, идемпотентны).
3. Вопрос вне MDB: почему kc/rc были PREFAIL ~4 часа — облако/инфраструктура.

## Грабли API (подтверждено)

- Пагинация истории: nextPageToken обязателен крутить (3500 событий, 250/стр).
- eventType в UI-API — `EVENT_TYPE_*` (caps), не camelCase.
- jq sort eventId — как число (`tonumber`), иначе порядок страниц ломается.
