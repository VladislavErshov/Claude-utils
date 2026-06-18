---
name: backstage-local-tester
description: Используй этот скилл, когда нужно локально протестировать API или workflow в Backstage MDB. Для запуска инфраструктуры используй команду /setup-local-backstage.
allowed-tools: [bash, read_file]
---

# Скилл для локального тестирования Backstage MDB

## Запуск инфраструктуры

Используй команду `/setup-local-backstage`

## База данных

### Два контейнера postgres
- **postgres** (localhost:6432) — БД для Backstage
- **pg_backstage_plugin_mdb** — БД для MDB Data (порт 6434)

### Дублирование данных
Для интеграционного тестирования данные нужно дублировать в обе БД:
```bash
# Backstage DB
docker exec postgres psql -U dev -d backstage_plugin_mdb

# MDB Data DB
docker exec pg_backstage_plugin_mdb psql -U dev -d backstage_plugin_mdb
```

### Типичные запросы
```bash
# Найти kafka кластер
docker exec pg_backstage_plugin_mdb psql -U dev -d backstage_plugin_mdb -c \
  "SELECT id, name, type FROM db_cluster WHERE type = 'kafka';"

# Текущие параметры кластера
docker exec pg_backstage_plugin_mdb psql -U dev -d backstage_plugin_mdb -c \
  "SELECT cluster_params FROM db_cluster_version WHERE cluster_id = '...' ORDER BY create_ts DESC LIMIT 1;" -t

# Operations
docker exec pg_backstage_plugin_mdb psql -U dev -d backstage_plugin_mdb -c \
  "SELECT id, status FROM operations WHERE cluster_id = '...';"
```

## Интеграция с Temporal

Для работы с workflows используй команду `/setup-local-temporal`

После запуска:
- Temporal UI: http://localhost:8233
- Processing API: http://<host-ip>:8080
- Подменяется `mdb-processing.host` в `app-config.mdb.local.yaml`

## Интеграция с MDB Data

Для тестирования MDB Data эндпоинтов используй скилл `/mdb-data-local-tester`

## Правила

1. **Enum values** — всегда lowercase (kafka, in_progress, done)
2. **Дублируй данные** — для интеграционных тестов нужны записи в обеих БД
3. **Temporal UI** — http://localhost:8233 для проверки workflows