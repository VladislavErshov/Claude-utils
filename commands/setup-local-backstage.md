---
name: setup-local-backstage
description: Запусти локальный Backstage с инфраструктурой для тестирования MDB интеграций.
---

# Команда локального запуска Backstage

## Шаги

1. Запустить инфраструктуру (postgres, redis, sentinel, clickhouse):
```bash
cd /Users/vl.ershov/Documents/Git/backstage/stubs && docker compose up -d
```

2. Создать БД для очереди задач (идемпотентно — не падает на повторном запуске):
```bash
docker exec postgres psql -U dev -d postgres -tAc \
  "SELECT 1 FROM pg_database WHERE datname='pg_boss'" | grep -q 1 || \
  docker exec postgres psql -U dev -d postgres -c "CREATE DATABASE pg_boss"
```

3. Запустить Backstage:
```bash
cd /Users/vl.ershov/Documents/Git/backstage && yarn mdb-dev > /tmp/backstage.log 2>&1 &
```
**Важно**: использовать `yarn mdb-dev`, **не** `yarn dev`. `yarn dev` падает с `Missing required config value at 'backend.apphostConfsProjectId'` — не подгружает `app-config.mdb.yaml`/`app-config.mdb.local.yaml`.

4. Проверка:
```bash
sleep 70 && grep -E "Listening on :7007" /tmp/backstage.log
docker exec postgres psql -U dev -d backstage_plugin_mdb -c "\dt" | head -5
```

## Инфраструктура

- **postgres** (localhost:6432) — БД для Backstage (`backstage_plugin_mdb`, `pg_boss`, `backstage_plugin_auth`)
- **redis** (localhost:6379) — кеш проектов/namespaces
- **stubs-sentinel-1** (localhost:26379) — Redis Sentinel (нужен и mdb-data)
- **clickhouse-server** (localhost:8123/9000) — логи
- **Backstage backend** — http://localhost:7007
- **Backstage frontend** — http://localhost:3000
- **Temporal UI** — http://localhost:8233 (если запущен `/setup-local-temporal`)

## Подводные камни

- **Seed → рестарт** — Redis-кеш проектов и PMS-конфиг строятся при boot. Если сидируешь данные после старта Backstage, перезапусти backend, иначе `Unknown namespaceId`/`getProjectByName` упадут.
- **`hardware_presets.database_preset`** — должен содержать `mongodbPreset` для каждого пресета, иначе `POST /version/` падает с 500 в DbParamsValidator (даже для Kafka).
- **Service auth для curl** — таблица `services_auth.access_type = 'w'` (НЕ `'write'`); enum `AccessType.WRITE = "w"`.

## Откат

```bash
pkill -f "mdb-app start\|mdb-backend start\|backstage-cli.*mdb"
cd /Users/vl.ershov/Documents/Git/backstage/stubs && docker compose down
```
