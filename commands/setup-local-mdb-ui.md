---
name: setup-local-mdb-ui
description: Запусти UI MDB (репозиторий mdb, vite:3012) + vkone-stub (8090) для локальной связки.
---

# Команда локального запуска UI MDB

UI — отдельный фронт (репозиторий `mdb`), API берёт с Backstage (7007) / mdb-data (8081) / mdb-health (8082) через vite-proxy.

## Шаги

1. Запустить vkone-stab (auth для UI, иначе «Необходимо авторизоваться»):
```bash
node ~/.claude/skills/mdb-local-tester/scripts/vkone-stub.mjs > /tmp/vkone-stub.log 2>&1 &
```

2. Проверить `.env` в корне `/Users/vl.ershov/Documents/Git/mdb` (файл в `.gitignore`, правки безопасны):
```
MDB_API_URL=http://localhost:7007
VKONE_API_URL=http://localhost:8090
MDB_DATA_LOCAL_URL=http://localhost:8081
MDB_HEALTH_LOCAL_URL=http://localhost:8082
PROXY_API_PREFIX=/proxy
```
`MDB_DATA_LOCAL_URL`/`MDB_HEALTH_LOCAL_URL` — opt-in: без них vite-прокси на 8081/8082 не включается (no-op).

3. Запустить dev-сервер (Node ^22, порт **3012**):
```bash
cd /Users/vl.ershov/Documents/Git/mdb && pnpm run dev > /tmp/mdb-ui.log 2>&1 &
```

4. Проверка:
```bash
sleep 10 && curl -s -o /dev/null -w '%{http_code}\n' http://localhost:3012   # 200
curl -s http://localhost:8090/api/v1/user/info | head -c 200                 # JSON юзера vl.ershov
```

## Зависимости

- Backstage (7007) — `/setup-local-backstage`, иначе `/api/mdb/*` из UI вернёт ошибку.
- mdb-data (8081) / mdb-health (8082) — opt-in через `.env` (см. выше).
- Grafana-виджеты тянутся напрямую с прода `goc.vk.team` — нужен корпоративный доступ из браузера, локально настраивать нечего.

## Подводные камни

- **Копировать прод-куки `vk-one-*` бесполезно** — подписи замаскированы (`__SECRET_N__`), vkone падает на `failed to get one-cloud user roles`. Только локальный стаб.
- **404 в `/tmp/vkone-stub.log`** — стаб не знает роут; дописать по контракту `src/shared/api/vkone/__generated__/data-contracts.ts`.
- После правки `.env` перезапустить `pnpm run dev` и перегрузить страницу.

## Откат

```bash
lsof -ti:3012,8090 | xargs -r kill -9
```
