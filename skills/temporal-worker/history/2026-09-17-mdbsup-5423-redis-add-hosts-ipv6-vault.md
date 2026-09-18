# MDBSUP-5423: add_hosts social-redis — vault + IPv6-баг meetAndAwaitJoin

Дата: 2026-09-17. Кластер: social-redis (e0f7bf15-6fea-42fe-9c19-4b6345c7118d), project
social, **ns=dzen**, dual-stack IPv6, Wf `addRedisClusterReplica` (операция add_hosts
6c25421e-6fbd-45ec-89da-2dfc5842ba38, 5 запусков 09.09–17.09, все FAILED).

## Цепочка фейлов

1. Запуски 09–14.09: `Vault secret is missing for path: zkv/mdb/social/redis/<queue>/master`
   (403 permission denied) — секрета не существовало. Лечится созданием секрета
   (ключ `password`, значение = masterauth с живого хоста). Путь валидирован кодом
   `RedisCredentialProvider` (SECRET_KEY="password") и cluster_links.database_links.secrets.
2. Запуск 17.09: все чилды прошли vault/updateHosts, но `meetAndAwaitJoin` упал
   «New shard nodes have not completed cluster handshake yet ... or not visible in topology»
   — при том что ноды были joined и синхронизированы.

## Корневая причина (баг mdb-processing, MDBDEV-3457)

- `RedisClusterTopologyParser.parseHostPort`: `value.split(",", 2)[0]` отбрасывает
  announced hostname → `Node.host()` = сырой IPv6 `2a00:b4c0:8c1::187:0`.
- `RedisFqdnResolver.hostMatches`: строковое сравнение IP — Java `getHostAddress()`
  даёт развёрнутый IPv6, Redis сжатый → матч всегда мимо → «not visible».
- Проявляется только на IPv6/dual-stack (dzen); IPv4 (mdbdev) работает.
- У child-workflow нет retry-политики (`RETRY_STATE_RETRY_POLICY_NOT_SET`) — retryable
  активность всё равно валит операцию с первой попытки.

## Доводка руками (паттерн для будущих кейсов)

Кластерная часть уже была OK (оператор сам делает MEET/REPLICATE) — не хватало только
регистрации в MDB:
- `host_state`: INSERT 5 хостов (params {dc, shard}, shard_id, ns/dzen в onecloud_ui_link,
  grafana-ссылка как у свежих строк кластера).
- `cluster_links.database_links.connectionUrl`: дополнить новые хосты по шардам
  (jsonb_set + `to_jsonb(... ::text)` — без каста падает polymorphic type).
- `operations`: failed → done, in_processing=false.
- PMS `zen.redis.hosts` — НЕ трогать (для redis не заведён, оператор берёт хосты из облака);
  `zen.redis.backupHosts` — только мастера, реплики не добавлять.

## Грабли

- dzen-хосты: в БД `*.wan.idzn.ru`, в mcc `*.idzn.ru` (без wan) — иначе EntityNotFoundException
  (записано в mcc-host-worker SKILL.md).
- `meetAndAwaitJoin` ~1.5 ч висела в SCHEDULED (воркер не подхватывал) — наблюдение для
  MDBDEV-3457 (пул redis-activities-worker).
