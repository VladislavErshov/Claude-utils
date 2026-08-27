# T9 Конкурирующие запуски + находки в downscale (2026-08-25)

**Результат: T9 PASS (конкуренция безвредна), но найдены 2 проблемы в downscale/синхронизации**

## Setup

Downscale kc (e528662a) для свежей цели → COMPLETED. Затем два ОДНОВРЕМЕННЫХ
`POST …/hosts/controllers?dc=kc` → оба 202, два parent-workflow параллельно
(c6ca4137…, d36f14fe…).

## T9: конкуренция — PASS

- Оба parent COMPLETED, все child (dc/hc/kc у обоих) COMPLETED.
- НИ ОДИН rescale не выполнился (все child — только discovery: current==target → skip).
- Деплой не задвоился, дублей хостов/кворума нет. `ignoreAlreadyStarted`/idempotent-skip
  удержали консистентность. Оба workflow сделали полный reload + save — side-effect двойной
  reload (безопасен, но лишний).

## Находка 1 (баг downscale): workflow COMPLETED при недостигнутой цели

Downscale e528662a: `cloud_rescaleService(kc → replicas=1)` отработал, но следующий
`getServiceInfo` (спустя 1.5с) вернул ЕЩЁ 2 инстанса (удаление асинхронное) — workflow
это не проверяет и завершается COMPLETED. Cloud сошёлся к 1 позже (~минуты).
Последствия: короткое окно, когда mdb-data/host_state считают хост удалённым, а он жив
(или наоборот — в нашем случае T9-скип сработал корректно, т.к. видел 2 инстанса).
→ Предложение: в downscale добавить waitServiceRunning-аналог (проверка фактического
числа инстансов после rescale).

## Находка 2 (рассинхрон): host_state/PMS содержат удалённый хост

После T9-прогона: облако kc=1 (2.controller.kc уничтожен, mcc EOF), но host_state=6
(включая 2.controller.kc — его туда записал save шага T9, который выполнялся до
фактического удаления инстанса) и PMS quorum=6 (12002 остался: T9-upsert добавил, никто
не убрал). Кворум содержит voter, чей хост не существует.
→ Требуется cleanup: перезалив seed или точечное удаление хоста + рестарт кворума.
→ Предложение в код: upscale-флоу не должен слепо union-merge — сверять с фактом из облака.

## Состояние на конец прогона (ТРЕБУЕТ CLEANUP)

- облако: dc=2, hc=2, kc=1; host_state: 6 (лишний 2.controller.kc); PMS: 6 voters (лишний 12002).

## Cleanup рассинхрона (2026-08-25)

1. Удалил фантом 2.controller.kc из host_state (SQL DELETE).
2. Закрыл висящую in_progress операцию d36f14fe (иначе 409).
3. Запустил upscale kc через mdb-data (cacc898b) → COMPLETED: инстанс перевыпущен
   (FQDN тот же → nodeId 12002 корректен), reload отрендерил конфиг, save вернул host_state=6.
4. Итог: облако=PMS=host_state=6, 2.controller.kc active (follower). Полная консистентность.

Паттерн cleanup'а фантома: SQL-удаление из host_state + закрытие операции + повторный
upscale целевого ДЦ через mdb-data (не direct-start!). Новый инстанс получает тот же FQDN,
кворум сходится без ручной правки PMS.
