---
name: setup-local-backstage
description: Запусти локальный Backstage с инфраструктурой для тестирования MDB интеграций.
---

# Команда локального запуска Backstage

## Шаги

1. Запустить инфраструктуру (postgres, redis, kafka):
```bash
cd /Users/vl.ershov/Documents/Git/backstage/stubs && docker compose up -d
```

2. Создать БД для очереди задач (не создаётся автоматически):
```bash
docker exec postgres psql -U dev -d postgres -c "CREATE DATABASE pg_boss"
```

3. Проверка: `docker exec postgres psql -U dev -d backstage_plugin_mdb -c "\dt"`

## Инфраструктура

- **postgres** (localhost:6432) — БД для Backstage
- **pg_backstage_plugin_mdb** — БД для MDB Data (порт 6434)
- **Temporal UI** — http://localhost:8233 (если запущен `/setup-local-temporal`)

## Откат

```bash
cd /Users/vl.ershov/Documents/Git/backstage/stubs && docker compose down
```