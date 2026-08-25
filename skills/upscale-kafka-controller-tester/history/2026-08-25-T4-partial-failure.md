# T4 Partial failure одного ДЦ + рестарт (2026-08-25)

**Результат: PASS**

## Часть 1: симуляция падения child

1. Цель `{hc:2}` → workflow `555c4de8-aaaf-4402-9e04-5de11e323fae`.
2. Wiremock-маппинг 500 на `/service/rescale` НЕ сработал: cloud-вызовы идут через proxylib
   в реальный one-cloud, маппинг ловит только ручной curl. Симуляцию сделал terminate
   child `555c4de8…_hc` (refresh csrf-cookie → POST …/workflows/{wid}_hc/terminate).
3. **Ожидаемое поведение подтверждено**: parent FAILED `Controller upscale failed in 1 DC(s): [hc]`
   (= PARTIAL_UPSCALE_FAILURE, non-retryable), dc/kc child COMPLETED (skip), reload НЕ выполнялся,
   save НЕ выполнялся.
4. Состояние после: PMS-кворум уже 5 voters (upsert до deploy — так задумано), host_state=4.

## Часть 2: рестарт

1. Закрыл failed-операцию (UPDATE operations status='done' — иначе 409), повторный POST ?dc=hc
   → workflow `8a5716f5-ecca-41f8-805f-31d81158982e`.
2. **COMPLETED**: discovery → upsert×3 (идемпотентно) → 3 child (hc доведён: rescale 1→2
   завершился ещё до terminate в первой попытке; dc/kc skip) → reload → save.
3. Post-проверки:
   - host_state: 5 контроллеров, 2.controller…hc записан, дублей нет;
   - PMS quorum: 5 voters, каждый один раз;
   - controller.properties на 2.controller…hc: 5 voters (reload применил);
   - кластер жив: ACC=0 на новых/старых (лидер — один из контроллеров, кворум собран).

## Грабли

- Wiremock не перехватывает cloud-API (proxylib идёт мимо) — для симуляции падений использовать
  terminate child через UI API, а не маппинги.
- terminate требует свежего csrf-cookie (брали заново GET'ом перед вызовом).
