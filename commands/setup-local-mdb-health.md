---
name: setup-local-mdb-health
description: Запусти сервис mdb-health локально (порт 8082) с pg_health (5434) для UI tiers/warnings.
---

# Команда локального запуска mdb-health

Отдельный сервис на порту **8082**. UI дергает `/api/mdb-health/*` (v1 — напрямую в mdb-health) и `/api/v2/mdb-health/*` (tiers/warnings/hosts/pg-shards) — v2 обслуживает **mdb-data (8081)**, проксируя в mdb-health по v1-путям. Требуется `mdb-health.base-url: http://localhost:8082` в `application-local.yml` mdb-data (дефолт 8080 — порт processing, неверно). Подробности — скилл `mdb-local-tester`.

## Шаги

1. Запустить инфраструктуру (только pg_health — 5434, БД health; `pg_backstage`/`redis_storage` из этого compose НЕ нужны — читает 6432 stubs-postgres и redis 6379 stubs из `backstage/stubs`):
```bash
cd /Users/vl.ershov/Documents/Git/mdb-health && docker compose up -d pg_health
```

2. Предусловие: Backstage stubs (postgres 6432) уже запущен (`/setup-local-backstage`). Добавить колонку для mirror-джобы:
```bash
docker exec postgres psql -U dev -d backstage_plugin_mdb -c \
  "ALTER TABLE operations ADD COLUMN IF NOT EXISTS last_status_sync_ts timestamp;"
```

3. Запустить сервис (JDK 21, лог `/tmp/mdb-health.log`):
```bash
cd /Users/vl.ershov/Documents/Git/mdb-health && \
  BOOT_RUN_JVM_ARGS="--add-opens java.base/java.lang=ALL-UNNAMED" \
  ./gradlew bootRun --args='--spring.profiles.active=local --server.port=8082 --rtconfig.local-config.path=src/main/resources/rtconfig/local.hjson --spring.ssl.bundle.pem.pms-client.keystore.certificate=~/.mccloud/client.cert --spring.ssl.bundle.pem.pms-client.keystore.private-key=~/.mccloud/client.key --spring.ssl.bundle.pem.pms-client.truststore.certificate=~/.mccloud/ca.crt' \
  > /tmp/mdb-health.log 2>&1 &
```

4. Проверка:
```bash
sleep 60 && curl -s http://localhost:8082/actuator/health/liveness
```

## Данные

- mirror-джоба сама синкает `mirror.*` из 6432 (Backstage stubs).
- Tier/warnings сеять из прод-health (туннель 53482, подключение — см. скилл `db-worker`): `tier.tier_state`, `tier.tier_history` (setval id_seq!), `warnings.cluster_warnings` — фильтр `mirror.db_cluster WHERE project_id=160 AND type='kafka'` (только Kafka из mdbdev, остальное — по явному запросу). Без tier-данных контроллер отдаёт 404 — это нормальный ответ «нет данных», не роутинг-баг.

## Подводные камни

- **rtconfig** — локальный файл `src/main/resources/rtconfig/local.hjson` (уже в репо); прод-PMS локально не читается.
- **ssl-бандл pms-client** — перекрывается `--spring.ssl.bundle.pem.pms-client.*` на `~/.mccloud/*` (mTLS).
- **Контроллеры обслуживают пути С префиксом `/api/mdb-health/...`** — vite-proxy не должен его срезать (targetUrl с суффиксом).
- Устаревший `create-jitter: 10s` в `application-local.yml` удалён (теперь Map с дефолтами) — если вернулся, убрать.

## Откат

```bash
lsof -ti:8082 | xargs -r kill -9
cd /Users/vl.ershov/Documents/Git/mdb-health && docker compose down
```
