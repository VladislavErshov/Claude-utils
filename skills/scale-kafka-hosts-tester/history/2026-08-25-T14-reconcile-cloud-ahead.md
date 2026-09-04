# T14 Reconcile: облако опережает host_state (2026-08-25)

**Результат: PASS — запуск с целью=факт сходится без DOWNSCALE_NOT_ALLOWED и без дублей**

Источник: пункт «Синхронизация №2» скилла (облако опережает локальную БД после упавшей операции
без save) + попутно подтверждён в T10 (downscale строил 2→0 на рассинхроне).

## Прогон

1. Симуляция «save не выполнился»: `DELETE FROM host_state WHERE host='2.controller…kc…'`
   → host_state: kc=1, при этом в облаке kc=2, PMS-кворум 7 voters.
2. `POST …/hosts/controllers?dc=kc` → op `79805fab`. mdb-data построил controllersPerDc из
   host_state: `{dc:2, hc:2, kc:2, ic:1}` (цель kc=2 = фактическое состояние облака).
3. Child kc: `Controller service in DC kc already has 2 replicas, nothing to submit` —
   reconcile-ветка отработала, никакого нового сабмита и никакого DOWNSCALE_NOT_ALLOWED.
4. Reload брокеров/контроллеров прошёл штатно, save = 7 хостов.

## Верификация

- host_state: 7 контроллеров, 7 уникальных (удалённый хост восстановлен).
- PMS-кворум: 7 voters, без дублей ( union-merge не продублировал 11002/12002).

## Вывод

Рекомендованный remediation «перезапустить операцию с целью=факт» работает через штатный
mdb-data endpoint: discovery берёт фактические реплики из облака, скипает сабмит, save
чинит host_state. Ручная правка PMS/облака не требуется. Контр-кейс из T10: если операция
DOWNscale запускается на рассинхроне — строится неверная цель (2→0) и INVALID_REPLICAS_COUNT,
т.е. лечить рассинхрон нужно upscale-reconcile'ом (или правкой host_state), не downscale.
