# Административные задачи ClickHouse

**Канон — Confluence «Дежурство MDB: Clickhouse», секция «Административные задачи»** (SSOT):
https://confluence.vk.team/pages/viewpage.action?pageId=1348619034

Покрывает (с полными XML-шаблонами): проставить настройку, удаление реплики, макросы,
system.text_log, named_collections, словари (+геобазы), обновление до 24.8 (и 25.8),
перенос кипера в другой ДЦ (server_id, request_leadership.py), интеграции с Kafka
(серт пользователя → `user_scripts/kafka_ca.crt`). Вики живая — править там.

Основной алгоритм большинства задач: **проставить настройку в PMS → релоад конфигов на
хостах** (кнопка в UI mdb-data на вкладке «хосты», либо mcc-перебор — ниже).

## Конфиги ClickHouse в PMS (наша сводка)

Основные:
- `zen.clickhouse.config.xml` — конфиг сервера
- `zen.clickhouse.users.xml` — конфиг пользователей

Дополнительные (через `<merge>` в основном):
- `zen.clickhouse.additional_config.xml` — доп. настройки (merge_tree, text_log, named_collections, dictionaries)
- `zen.clickhouse.macros.xml` — макросы
- `zen.clickhouse-keeper.config.xml` — конфиг Keeper (на каждый хост свой, т.к. `server_id` уникален)

## Наши дополнения к вики

### Релоад конфигов на всём кластере (mcc-перебор)

Перебрать хосты × ДЦ через скилл [`mcc-host-worker`](../../mcc-host-worker/SKILL.md)
(`mcc sshexec`; шаблон перебора — в скилле). Команда на хосте:
`confp --oneshot; clickhouse-client --user backup-admin --password $(grep -oP 'password:\s*\K[^ ]+' /etc/rscheck/checkclickhouse.conf) --query 'SYSTEM RELOAD CONFIG'`.
Либо `python3 /usr/scripts/reload-config.py` на хосте. Шаблон хоста:
`1.shard${shardN}-db.<cluster>-${project}-ch.$cloud.one-infra.ru`.

### Заливка файлов на хосты (геобазы и др.)

Перенос файлов на хосты — через [`mcc-host-worker`](../../mcc-host-worker/SKILL.md)
(`mcc scp`). Загружать в директорию, не по пути-файлу (грабли `scp` — в mcc-host-worker).
