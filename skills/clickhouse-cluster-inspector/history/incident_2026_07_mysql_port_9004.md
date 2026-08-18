# Инцидент 2026-07: Включение MySQL emulation port (9004) на кластере uchiru-bi-dwh

Кластер: `uchiru-bi-dwh`
Тикет: `MDBSUP-3886`
Версия: ClickHouse 24.x (MDB).

## Симптомы

- Нужно было дать клиентам ходить в ClickHouse по MySQL-протоколу (порт 9004).
- Проверка `telnet localhost 9004` на `1.shard1-db.uchiru-uchiru-bi-dwh-ch.hc.one-infra.ru`:
  ```
  Trying 127.0.0.1...
  telnet: Unable to connect to remote host: Connection refused
  ```
  Порт 9000 (native) при этом отвечал нормально — значит CH-сервер жив, проблема именно в отсутствии `<mysql_port>`.

## Причина

В MDB-конфиге ClickHouse по умолчанию `<mysql_port>` не прописан — порт не слушает.
Чтобы его включить, **недостаточно** добавить настройку в `zen.clickhouse.config.xml`:
нужно ещё открыть порт в манифесте сервиса (т.к. MDB-инфра фильтрует порты на уровне
сервисного манифеста, а не только PMS).

## Фикс (что было сделано)

1. **PMS.** В `zen.clickhouse.config.xml` для кластера `uchiru-bi-dwh` добавить внутрь
   `<clickhouse>...</clickhouse>`:
   ```xml
   <mysql_port>9004</mysql_port>
   ```
2. **Манифест сервиса.** Добавить порт 9004 в список проброшенных портов:
   ```
   '9004': 'lan,tcp'
   ```
   Без этого порт остаётся закрытым на уровне сервисной сетевой конфигурации,
   даже если CH его слушает.
3. **Применить на хостах:**
   ```bash
   confp --oneshot && systemctl restart mdb-clickhouse-server
   ```
   ⚠️ Здесь нужен именно `restart`, а не `SYSTEM RELOAD CONFIG` — потому что
   добавился новый listen-port. `RELOAD CONFIG` в 24.x применяет настройки, но
   новый сетевой порт поднимается только при полном рестарте процесса.

## Проверка после фикса

```bash
telnet localhost 9004
# Trying 127.0.0.1...
# Connected to localhost.
```
Также `ss -tlnp | grep 9004` должен показать слушающий сокет.

## Чему учит

- Включение нового сетевого порта в MDB ClickHouse = **три шага**, а не один:
  PMS + манифест сервиса + рестарт (не reload).
- `telnet localhost <port>` — корректный способ проверки прослушки, если на хосте
  нет `clickhouse-client` или не хочется тащить пароль `backup-admin`.
- `Connection refused` vs `Connection timed out` vs `Connected + closed`:
  - refused → порт не слушает (настройки нет);
  - timed out → режет файрвол/security-group;
  - connected + closed → бинд есть, но что-то отбрасывает (смотреть err.log).

## Ссылки

- Скилл `clickhouse-cluster-inspector/commands/administration.md` — общий алгоритм
  «проставить в PMS → релоад конфигов»; для сетевых портов требуется доп. шаг
  (манифест сервиса + рестарт).
