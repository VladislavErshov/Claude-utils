# MDBSUP-5137 (2026-09-04, КЕЙС ОТКРЫТ — ждём фикса сети) — preprod-vk-support-kafka: add_hosts ec упала, новый EC-хост изолирован по IPv4

Кластер `preprod-vk-support-kafka` (`e67f499f-6b72-4234-bf33-aef81c7b8476`, ns vk-support,
fullQueue `preprod-vk-support-kafka.vk-support.db.production.mdb.prod`, домен one-infra).
Брокеры pc/kc/hc + контроллеры pc/kc/hc + cruise kc; операция add_hosts добавляла **первого
брокера в ec** — `5b4f76bd-2f02-47ec-a3b5-e74bf8c27ce2`, failed 15:24 МСК, attempts_left=0,
in_processing=t. Temporal: run `01a06c54-...` FAILED, детей `_pc/_kc/_hc` ok, `_ec` FAILED —
`ServiceNotRunning` после 10 поллов getServiceInfo (~13 мин). host_state ec-строки нет
(чисто). Тикет: https://jira.vk.team/browse/MDBSUP-5137

## Корневая причина: сетевая изоляция нового EC-хоста по IPv4

- ec → контроллеры hc/pc/kc: **любой порт v4 (9093/7777/9092) — SYN молча дропается**,
  JVM висит в SYN_SENT; **v6 ULA — работает** (поэтому DNS/миньон/teleport живы).
- Обратно hc → ec: 9092 и даже **22 — дроп** (time timeout = 4 c, не RST).
- Диагностический приём: bash `/dev/tcp` с хоста «проходит» т.к. getent отдаёт IPv6
  первым — **всегда сверять v4 и v6 по отдельности** (literal-адрес), иначе ложное «связность есть».
- KRaft-кворум здоров: лидер 10001 (hc), HWM растёт, append ~2/с, ActiveControllerCount=1.
- Брокер 23001: crash-loop каждые ~31 мин (миньон; systemd Restart=no), 4 попытки
  15:13/15:47/16:18/16:48 МСК, все идентичны: `Unattached(epoch=0)` → «still don't know
  the high water mark» → 60 c (`initial.broker.registration.timeout.ms`) →
  `Unable to register with the controller quorum` / `TimeoutException: fetchMetadata`.
  На контроллерах — ноль упоминаний брокера (запросы физически не доходят).
- Оператор one-cloud-ops тоже не может в хост: JMX `SocketTimeoutException`, алерт
  `PARTIALLY_AVAILABLE`. Диагноз из тикета «нехватка ёмкости/квоты» — НЕ подтвердился:
  планировщик разместил хост сразу (JVM поднялась 15:12), demand=allocated.

## Что делать дальше (когда вернёмся)

1. Эскалация в one-cloud/сеть: у нового EC-хоста не применяется/нет политики
   микросегментации v4 для очереди. Лечится на стороне облака (применение политики /
   пересоздание хоста — прецедент MDBSUP-4867 LOST_MINION).
2. После появления v4-связности: брокер сам поднимется на ближайшем рестарте minion,
   зарегистрируется в кворуме.
3. Перезапустить add_hosts через UI/API mdb (НЕ руками: операция сама вставит ec-строку
   в host_state и прогонит конфиг-релод) → done. Правки SQL сейчас бесполезны.

## Статус 2026-09-05 13:35 МСК: сеть НЕ починена

- v4 ec → hc/pc:9093 по-прежнему FAIL; брокер продолжает crash-loop (уже 8 фейлов
  регистрации, последние 13:29→13:31 МСК).
- Облако активно: сервис флуктуирует STARTING/DEPLOYING, контейнер пересоздавался.
- Строка progress «there are 37991 subnets for peer pl-i-sg_...» — **шум**, не признак
  застрявшей сетевой политики (см. known_issues.md «Прогресс there are N subnets»).
- Действие: эскалация к дежурным облака на сетевую изоляцию v4 (без привязки к peer'ам),
  после восстановления связности — перезапуск add_hosts.

## Грабли

- «Service created in облаке, но не размещён» в UI-формулировке ≠ реально: инстанс может
  быть размещён и crash-loop'иться — сервис навсегда STARTING, updated=1970. Смотреть
  `mcc status` + логи хоста.
- `initial.broker.registration.timeout.ms=60` — быстрые (60 с) фейлы «unable to register»
  при живом кворуме = не проблема кворума, искать путь broker→leader.
- Живой Crash-loop маскируется: лог одной JVM-сессии маленький, но `grep ERROR` покажет
  4+ идентичных цепочки shutdown — считать попытки для оценки period рестартов.
