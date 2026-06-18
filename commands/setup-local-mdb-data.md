---
name: setup-local-mdb-data
description: Запусти MDB Data локально для тестирования Backstage интеграций.
---

# Команда локального запуска MDB Data

## Шаги

1. Перейти в корень mdb-data:
```bash
cd /Users/vl.ershov/Documents/Git/mdb-data
```

2. Запустить инфраструктуру (postgres, redis):
```bash
cd /Users/vl.ershov/Documents/Git/mdb-data && docker compose up -d
```

3. Запустить приложение:
```bash
cd /Users/vl.ershov/Documents/Git/mdb-data && ./gradlew bootRun --args='--spring.profiles.active=local' > /tmp/mdb-data.log 2>&1 &
```

4. Проверка health:
```bash
sleep 10 && curl -s http://localhost:8081/actuator/health
```

## Откат

```bash
# Остановить java процесс
pkill -f "MdbDataApplication"

# Остановить инфраструктуру
cd /Users/vl.ershov/Documents/Git/mdb-data && docker compose down
```

## Конфигурация

- **Порт**: 8081
- **Профиль**: local
- **БД**: pg_backstage_plugin_mdb (postgres:16)
- **Logs**: /tmp/mdb-data.log