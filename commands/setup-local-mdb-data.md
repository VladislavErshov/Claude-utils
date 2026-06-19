---
name: setup-local-mdb-data
description: Запусти MDB Data локально для тестирования Backstage интеграций.
---

# Команда локального запуска MDB Data

## Шаги

1. Запустить инфраструктуру (только postgres — не пытаемся поднимать sentinel):
```bash
cd /Users/vl.ershov/Documents/Git/mdb-data && docker compose up -d pg_backstage_plugin_mdb
```

Если поднимать compose целиком (`docker compose up -d`), `redis_sentinel` падает с `Bind for 0.0.0.0:26379 failed: port is already allocated` — порт уже держит `stubs-sentinel-1` из `backstage/stubs`. Это **не блокирует** работу (mdb-data приложение подключается к sentinel из stubs), но `docker compose` пишет красную ошибку в stderr, которая выглядит фатально. Узкий запуск убирает шум.

2. Запустить приложение (порт 8081 — 8080 занят mdb-processing):
```bash
cd /Users/vl.ershov/Documents/Git/mdb-data && \
  BOOT_RUN_JVM_ARGS="--add-opens java.base/java.lang=ALL-UNNAMED" \
  ./gradlew bootRun --args='--spring.profiles.active=local --server.port=8081' > /tmp/mdb-data.log 2>&1 &
```

3. Проверка health (займёт ~60с на холодный старт):
```bash
sleep 60 && curl -s http://localhost:8081/actuator/health
# Ожидается: {"status":"UP",...}
```

## Откат

```bash
pkill -f "MdbDataApplication"
cd /Users/vl.ershov/Documents/Git/mdb-data && docker compose down
```

## Конфигурация

- **Порт**: 8081
- **Профиль**: local
- **БД**: `pg_backstage_plugin_mdb` (postgres:16) на localhost:6434, БД `backstage_plugin_mdb`
- **Logs**: /tmp/mdb-data.log
- **Endpoints для Kafka**: `PATCH /api/v2/mdb/kafka/clusters/{clusterId}/modify` (см. `KafkaClusterApi.java`)
