# Администрирование Redis Sentinel

**Канон — Confluence «Дежурство MDB: Redis», Sentinel-секции «Частые запросы»** (SSOT):
https://confluence.vk.team/pages/viewpage.action?pageId=1351178658

Покрывает: смену мастера (`sentinel failover`), Sentinel wrong info (MDBDEV-1418),
старые ip/hostname дубликаты, возврат лежащей ноды, ноду в loading, ACL (забанить команду,
default-пользователь через оператор `redis-sentinel.upsert-user`, права для диагностики,
долгое добавление, удаление), параметр вне UI, isPersistent, access-логи, скрипт
изменения параметра конфига кластера без перезагрузки, обновление 7→8 (с sentinel-шагами:
`locale-collate "C"` в `/etc/redis/sentinel.conf` и `/mnt/redis/senti/sentinel.conf`),
бэкап другого кластера, учения, миграцию 6.2→7.0 (отдельная страница, ссылка из вики).
Вики живая — править там.

## Заметки

- `CLUSTER FORGET` здесь не применяется (это Cluster-механика) — у Sentinel вместо него
  `SENTINEL RESET` (см. [sentinel_reset.md](sentinel_reset.md)).
- Оператор для sentinel-кластеров — задачи с префиксом `redis-sentinel.*`
  (у шардированных — `redis-cluster.*`).
