---
name: setup-local-temporal
description: Подключи Backstage к локальному Temporal через Processing-сервис.
---

Подключи Backstage к локальному Temporal через Processing-сервис.

## Шаги

1. Запустить инфраструктуру Processing:
```bash
cd /Users/vl.ershov/Documents/Git/mdb-processing/localrun && docker-compose up -d
```

2. Инициализация Vault и Temporal search attributes:
```bash
cd /Users/vl.ershov/Documents/Git/mdb-processing/localrun && ./localrun.sh
```
Если localrun.sh не отрабатывает, создать search attributes вручную:
```bash
docker run --rm --network localrun_local-dev-network temporalio/admin-tools:latest \
    temporal operator search-attribute create \
    --address temporal:7233 \
    --name OperationId --type Keyword \
    --name ClusterId --type Keyword
```

3. Запустить Processing-сервис:
```bash
BOOT_RUN_JVM_ARGS="--add-opens java.base/java.lang=ALL-UNNAMED" \
/Users/vl.ershov/Documents/Git/mdb-processing/gradlew \
    -p /Users/vl.ershov/Documents/Git/mdb-processing \
    bootRun --args='--spring.profiles.active=local'
```
Проверка: `curl -s http://<host-ip>:8080/actuator/health`

4. Переключить Backstage на локальный Processing. В `app-config.mdb.local.yaml` заменить:
```yaml
    mdb-processing:
      host: 'host'
```
на:
```yaml
    mdb-processing:
      host: 'http://<host-ip>:8080'
```

5. Перезапустить Backstage.

## Откат

1. Остановить Processing: `pkill -f "MdbProcessingApplication"`
2. Вернуть `mdb-processing.host: 'host'` в `app-config.mdb.local.yaml`
3. Перезапустить Backstage
4. Остановить инфраструктуру: `cd /Users/vl.ershov/Documents/Git/mdb-processing/localrun && docker-compose down`

## Подводные камни

- **IPv6** — использовать `http://<host-ip>:8080`, не `http://localhost:8080`. Node.js резолвит localhost в `::1`, а Processing слушает только IPv4.
- **`--add-opens`** — обязательный JVM-аргумент для Java 21+. Передаётся через `BOOT_RUN_JVM_ARGS`.
- **`pg_boss`** — БД для очереди задач Backstage: `docker exec postgres psql -U dev -d postgres -c "CREATE DATABASE pg_boss"`
- **Search attributes** — без `OperationId`/`ClusterId` Processing падает с `INVALID_ARGUMENT`. Запуск `localrun.sh` обязателен.