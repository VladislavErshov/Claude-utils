# 2026-09-10 — INCALL-52120: RUNNING UNAVAILABLE реплика (comments4) + зависшая add_hosts

## Кратко
- **Хост:** `1.db.comments4-social-pgsql.ec.idzn.ru` (реплика, ДЦ ec, namespace dzen).
- **Кластер:** `comments4-social-pgsql` (id `a02f3d27-7c43-43f9-aa57-6766373745b1`;
  4 кивера: rc = мастер, ec/dc/pc = реплики).
- **Симптом:** алерт «RUNNING UNAVAILABLE replica for 8h» → INCALL-52120 (P3, impact 0%).
- **Диагноз:** тот же корень, что в comments2 (INCALL-52118): облако 09.09 ~16:28
  пересоздало диск, keeper увидел пустой data dir → штатная переналивка
  `pg_basebackup` с мастера (~780 ГБ, max-rate 25M ≈ 8.7 ч, 16:28 → 01:07), потом
  recovery из wal-g архива + стриминг, реплика догнала мастера.
- **Отличие от 52118 (новый паттерн):** добавлявшая хост операция `add_hosts`
  (создана 09.09 14:57) упала с `"Service db.comments4-social-pgsql is not running
  yet"` и зависла: `status=failed, in_processing=true, attempts_left=0,
  finished_ts=NULL` — блокировала кластер для новых операций. Хост при этом
  **отсутствовал в `host_state`** (UI не знал про ec).
- **Фикс (DML в прод `backstage_plugin_mdb`):** операцию закрыть
  (`status='done', in_processing=false, finished_ts=now(), error_message=NULL`) +
  INSERT ec-хоста в `host_state` по шаблону соседей (`onecloud_ui_link`/`grafana_dashboard_link`
  копируются из строк dc/pc/rc с заменой ДЦ; `params: {"dc": "ec"}`, FQDN — wan-форма
  `1.db.comments4-social-pgsql.ec.wan.idzn.ru`).
- **Закрытие:** в беседу myteam `/s <статус>` + `/close` (08:15, инцидент → [C]).

## Хронология

| Время (MSK) | Событие |
|---|---|
| 09.09 14:57 | mdb-data `add_hosts` `24563b52` — добавление ec-хоста. |
| 16:28 | keeper на новом хосте: `db UID ""` vs `cdDB 53d65379` → `pg_basebackup` с мастера rc. |
| ~00:57 10.09 | Алерт RUNNING UNAVAILABLE 8h → INCALL-52120. Waiter операции к этому моменту уже отфейлил операцию («Service is not running yet»). |
| 01:07 | keeper: `sync succeeded` → postgres стартовал, recovery из архива. |
| 01:13 | `consistent recovery state reached` → read-only, стриминг с мастера. |
| ~08:05 | DML: операция done, host_state + ec. Реплика догнала мастера (replay в одном сегменте с receive). |
| 08:14–08:15 | `/s` + `/close` в беседе → инцидент закрыт. |

## Диагностика (грабли)

- `psql` на хосте под load 20–40 вешался: `PGPASSWORD=$(cat ...)` в
  `mcc sshexec "<cmd>"` **разворачивался локально на маке** (двойные кавычки) —
  пустой пароль → промпт → таймаут. Экранировать `\$(...)` или весь cmd в
  одинарных кавычках. Рабочий путь health-проверки: `pg_isready -h 127.0.0.1 -p 5432/-p 6432`.
- Прямой `psql root` через pgbouncer под нагрузкой тоже может висеть (md5 auth) —
  это не признак лежащей базы, keeper в базу ходит нормально.
- Прогресс переналивки смотреть по `ps aux | grep "startup recovering"` (сегмент
  реплея) vs `walreceiver streaming` (LSN приёма): если в одном-двух сегментах —
  реплика догнала.

## Извлечённые уроки

1. **Алерт «RUNNING UNAVAILABLE» на свежедобавленном хосте — сначала проверить
   `operations` в прод-БД** (`WHERE cluster_id=... ORDER BY created_ts DESC`):
   длительный pg_basebackup выглядит как лежащий хост, а waiter операции при этом
   может уже отфейлиться и зависнуть (`in_processing=true`), блокируя кластер.
2. **После фейла `add_hosts` хост может отсутствовать в `host_state`** — операция
   падает раньше записи. Без строки в host_state UI/алерты работают по неполной
   топологии; после закрытия операции хост добавить INSERT'ом по шаблону соседей.
3. Закрытие зависшей операции и INSERT host_state не лечат сам хост — сначала
   убедиться, что переналивка идёт штатно (keeper-лог: resync → pg_basebackup →
   `sync succeeded`), потом прибирать хвосты в БД.
4. `/s` и `/close` — в беседу myteam (подтверждено: бот обновил сводку и закрыл
   инцидент); Jira-тикет при этом остаётся в своём статусе.

## Ссылки
- `history/2026-09-10-incall-52118-running-unavailable-disk-recreated.md` —
  двойник (comments2 ec, тот же день): пересоздание диска, переналивка без хвостов.
- `history/2026-09-10-incall-52122-stolon-proxy-dead-sc.md` — третий кейс того же
  алерта за день (proxy код 0, рестарт юнита).
- Инструкция из алерта: Confluence «RUNNING UNAVAILABLE реплика постгреса»
  (pageId=1967549817).
