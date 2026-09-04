# MDBSUP-5080 (2026-09-03): clickbench — серверные настройки async_load_databases + merge_tree lazy

**Кластер:** clickbench (b9984165-ffa1-45ee-9324-af402b2b1680), проект YT (project_id 26), ns infra, CH 25.8, 1 реплика + 3 кипера, всё в hc.

**Суть:** пользователь попросил проставить серверные настройки, недоступные через UI:
`async_load_databases=false`, `merge_tree.primary_key_lazy_load=0`, `merge_tree.columns_and_secondary_indices_sizes_lazy_calculation=0`.

**Разбор:** правки в PMS (`zen.clickhouse.additional_config.xml`, ключ `clickbench-yt-ch.clouds`) кто-то уже внёс до нас + прошла операция `reload_config` (done за 23 сек). Проверка подтвердила применение на хосте. Кластерной починки не требовалось — только верификация и закрытие тикета.

## Рецепт верификации «проставить настройку в CH» (переиспользуемый)

1. PMS: `pms-read.sh <cluster>-<project>-ch.clouds zen.clickhouse.additional_config.xml infra mdb` — есть ли настройки.
2. Прод-БД: `SELECT ... FROM operations WHERE cluster_id=...` — была ли недавняя `reload_config` (значит, кто-то уже работает по тикету — не дублировать правки).
3. Рендер на хосте: `grep ... /etc/clickhouse-server/config.d/zz_additional_config.xml` (additional_config рендерится именно в `zz_additional_config.xml`).
4. Эффективные значения в живом CH:
   - серверные (`<clickhouse>` top-level): `SELECT name, value, changed FROM system.server_settings WHERE name='async_load_databases'`
   - merge_tree (`<merge_tree>`): `SELECT name, value, changed FROM system.merge_tree_settings WHERE name IN (...)`
   - `changed=1` = значение взято из конфига, а не дефолт сборки.
5. Пароль backup-admin — `/etc/rscheck/checkclickhouse.conf` (см. SKILL.md).

Замечание: `SYSTEM RELOAD CONFIG` применил и merge_tree-настройки без рестарта сервера (проверено на CH 25.8) — таблицы подхватили новые дефолты.
