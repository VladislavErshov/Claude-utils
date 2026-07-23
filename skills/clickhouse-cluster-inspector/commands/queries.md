# Полезные SQL-запросы для диагностики ClickHouse

Подключение: `clickhouse-client --user backup-admin --password '<pass>'` (пароль в
`/etc/rscheck/checkclickhouse.conf`).

⚠️ Если CH перегружен (`TOO_MANY_SIMULTANEOUS_QUERIES`) — запросы могут не выполниться.
Сначала чиним корень (обычно Keeper), потом запросы пойдут.

## Проверка доступности Keeper

```sql
SELECT * FROM system.zookeeper_connection FORMAT Vertical;
```

Если пусто или ошибка — Keeper недоступен, см. `known_issues.md` → «Не доступен Keeper».

## Число партов в таблице

```sql
SELECT count() AS parts_count
FROM system.parts
WHERE (table = '<table_name>') AND (database = '<database_name>') AND (active = 1);
```

## Потенциальные проблемы репликации

```sql
SELECT database,
       table,
       is_leader,
       is_readonly,
       is_session_expired,
       future_parts,
       parts_to_check,
       columns_version,
       queue_size,
       inserts_in_queue,
       merges_in_queue,
       log_max_index,
       log_pointer,
       total_replicas,
       active_replicas
FROM system.replicas
WHERE active_replicas < total_replicas
    AND (parts_to_check > 0
         OR merges_in_queue > 20)
FORMAT Vertical;
```

**Что проверяет** — реплики таблиц, где:
- `active_replicas < total_replicas` — не все реплики в наборе репликации активны.
- `parts_to_check > 0` — есть части, требующие проверки/восстановления (повреждение или
  проблемы синхронизации).
- `merges_in_queue > 20` — значительная очередь слияний ( система не справляется с нагрузкой).

## Ошибки за последний час

```sql
SELECT *
FROM system.errors
WHERE last_error_time > (now() - toIntervalHour(1))
FORMAT Vertical;
```

Ключевые поля:
- `name` — название ошибки
- `code` — код ошибки
- `value` — сколько раз произошла
- `last_error_time` — когда в последний раз
- `last_error_message` — текст
- `last_error_trace` — трассировка

## Запущенные запросы/процессы

```sql
SELECT *
FROM system.processes
WHERE user = 'some_user'
ORDER BY elapsed DESC
FORMAT Vertical;
```

Часто полезно:
```sql
SELECT count(), max(elapsed) FROM system.processes;
```
Если `count()` близко к 1000 и `max(elapsed)` большой — запросы висят (часто на Keeper).

## KILL зависших запросов

```sql
-- Все запросы дольше 300 секунд
KILL QUERY WHERE elapsed > 300;

-- Конкретного пользователя
KILL QUERY WHERE user = 'some_user';
```

## Состояние merges/mutations

```sql
SELECT database, table, count() AS merges, sum(progress) / count() AS avg_progress
FROM system.merges
GROUP BY database, table
ORDER BY merges DESC
LIMIT 20;
```

```sql
SELECT database, table, command, parts_to_do, is_done, latest_failed_part
FROM system.mutations
WHERE NOT is_done
ORDER BY parts_to_do DESC
LIMIT 20;
```

## Detached parts

```sql
SELECT database, table, count() AS detached
FROM system.detached_parts
GROUP BY database, table
ORDER BY detached DESC;
```

## Размер таблиц и число партов

```sql
SELECT
    database,
    table,
    formatReadableSize(sum(bytes_on_disk)) AS size,
    count() AS parts
FROM system.parts
WHERE active = 1
GROUP BY database, table
ORDER BY sum(bytes_on_disk) DESC
LIMIT 20;
```
