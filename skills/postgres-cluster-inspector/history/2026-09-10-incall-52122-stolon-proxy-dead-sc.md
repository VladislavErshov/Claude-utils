# 2026-09-10 — INCALL-52122: stolon proxy is dead на sc (proxy вышел с кодом 0 после рестарта)

## Кратко
- **Хост:** `1.db.admin-exps-test-odkl-pgsql.sc.idzn.ru` (реплика, ДЦ sc, namespace dzen).
- **Кластер:** `admin-exps-test-odkl-pgsql` (id `33128c18-2fac-48fe-8da5-71a469a6311d`;
  rc = мастер, sc/kc = реплики).
- **Симптом:** алерт «RUNNING UNAVAILABLE replica for 8h» → INCALL-52122 (P3, impact 0%);
  mcc: `availability: UNAVAILABLE`, `availability_details: 'pgsql-availability error:
  stolon proxy is dead'`.
- **Диагноз:** 09.09 в 17:10 облако рестартнуло инстанс (диск сохранился, база НЕ
  переливалась). stolon-proxy стартовал с новым UID, не был в `enabledProxies` и
  **завершился с кодом 0**; юнит `Restart=on-failure` нулевой код не рестартует →
  прокси мёртв 15 часов, `pg_isready :7432` не отвечал.
- **Фикс:** `systemctl restart stolon-proxy` — один рестарт, прокси зарегистрировался,
  сентинел включил, начал проксировать. Облачный статус → RESERVED.
- **Отдельный хвост (не трогали):** host-checker не может достучаться до
  `health.mdb.one-infra.ru:443` (TCP-FAIL, резолвится только в IPv6 fd00:b4c2::*),
  последний успешный пуш — 03.09. Данные mdb-health по хосту протухли; на алерт
  это не влияло (алерт идёт от облачного availability из rscheck).

## Отличия от кейса comments2 (INCALL-52118, тот же день)

| | comments2 (ec) | admin-exps (sc) |
|---|---|---|
| Рестарт инстанса | 16:28, **новый пустой диск** | 17:10, диск сохранился |
| Следствие | полная переналивка pg_basebackup ~8.5 ч | proxy вышел с кодом 0 |
| Что лежало | postgres (recovery), прокси жив | stolon-proxy, postgres жив |
| host-checker.log | честно UNAVAILABLE весь период | UNAVAILABLE в логе нет вообще |
| Фикс | сам завершился | `systemctl restart stolon-proxy` |

## Диагностика (краткая цепочка)

1. `mcc instances -f yaml` → `availability_details: 'pgsql-availability error: stolon
   proxy is dead'` — сразу указывает на чек `pg_isready :7432` из checkpgsql.
2. `systemctl status stolon-proxy` → `inactive (dead)`; остальные сервисы active.
3. `journalctl -u stolon-proxy` → `Started Stolon Proxy` + `stolon-proxy.service:
   Succeeded` в одну секунду — процесс вышел сам с кодом 0.
4. `stolonctl status` → наш proxy UID отсутствует в Active proxies (там только kc/rc).
5. Лог прокси перед смертью: `not proxying to master address since we aren't in the
   enabled proxies list` (после рестарта UID новый, сентинел не успел включить).
6. `systemctl cat stolon-proxy` → `Restart=on-failure` — код 0 не рестартует, мёртв
   навсегда без ручного вмешательства.

## Извлечённые уроки

1. **`stolon proxy is dead` — в первую очередь `systemctl status stolon-proxy` и
   journalctl юнита**, а не пересоздание. Прокси, вышедший с кодом 0 из-за
   `enabledProxies`, лечится одним рестартом (первый старт регистрирует UID в
   clusterdata, сентинел включает, прокси начинает слушать).
2. **После рестарта инстанса stolon-proxy может не подняться сам** — проверять юнит
   отдельно от keeper/sentinel (те с `Restart=always`-подобным поведением выживают,
   прокси с `on-failure` — нет).
3. **Отсутствие UNAVAILABLE в host-checker.log ≠ хост здоров**: облачный
   availability идёт из rscheck (checkpgsql → minion → облако), а не из
   host-checker-пушей. Пуши в mdb-health могут молча гнить неделями (см. хвост ниже).
4. **Пуши в mdb-health — отдельный путь**: `tail host-checker.log` + `grep 'Success
   sending'` показывает свежесть данных. TCP до `health.mdb.one-infra.ru` проверять
   `timeout 3 bash -c 'echo > /dev/tcp/health.mdb.one-infra.ru/443'`.
5. Один и тот же алерт + один день = два разных корня. Сверять детали, не тащить
   диагноз из соседнего инцидента.

## Ссылки
- `history/2026-09-10-incall-52118-running-unavailable-disk-recreated.md` — соседний
  кейс того же дня (пересоздание диска, полная переналивка).
- `commands/diagnostics.md` — чек checkpgsql: pg_isready 6432 (pgbouncer) и 7432
  (stolon proxy).
