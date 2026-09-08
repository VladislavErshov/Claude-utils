# Known gotchas (удалённая БД mdb-data)

## 1. `db_versions.version` — колонка не существует

```
[42703] ERROR: column dv.version does not exist
```

В таблице `db_versions` **нет** колонки `version`. Реальные колонки (проверено на локальной БД, на удалённой схема та же):

```
db_versions:
  id           integer (PK)
  type         db_type
  sharded      boolean
  version_name varchar(64)
  is_default   boolean
```

Дополнительная находка: `db_cluster_version.db_version` — это **jsonb**, а не FK-число. Поэтому фильтр `dv.version IN (SELECT db_version FROM db_cluster_version ...)` вдвойне некорректен.

**Безопасный шаблон** — тянуть все `db_versions` нужного типа (их немного):

```sql
'db_versions', (SELECT jsonb_agg(t) FROM (
  SELECT * FROM db_versions WHERE type='kafka'
) t)
```

Либо вообще не тащить `db_versions` — это справочник, на локальной БД он уже заполнен flyway-миграциями (для kafka локально есть id=91 `4.3`). Если нужно — добавишь недостающие версии руками после получения seed-данных.
