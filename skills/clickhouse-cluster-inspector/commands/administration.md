# Административные задачи ClickHouse

Основной алгоритм для большинства задач: **проставить настройку в PMS → релоад конфигов на хостах**.
Более сложные инструкции — https://docs.vk.team/mdb/docs/clickhouse/ch-administration.html.

## Проставить настройку `<a>` в значение `b`

У кликхауса два основных конфига в PMS:
- `zen.clickhouse.config.xml` — конфиг сервера
- `zen.clickhouse.users.xml` — конфиг пользователей

Дополнительные (через `<merge>` в основном):
- `zen.clickhouse.additional_config.xml` — доп. настройки (merge_tree, text_log, named_collections, dictionaries)
- `zen.clickhouse.macros.xml` — макросы
- `zen.clickhouse-keeper.config.xml` — конфиг Keeper (на каждый хост свой, т.к. `server_id` уникален)

Порядок:
1. Найти настройку в PMS (возможно уже есть — поменять значение для кластера) или
   смотреть в документации CH, куда её ставить.
2. Обновить значение в PMS.
3. Релоад конфигов — кнопка в UI mdb-data на вкладке «хосты», либо вручную через
   скилл [`mcc-host-access`](../../mcc-host-access/SKILL.md) (`mcc sshexec`).
   Команда на хосте:
   `confp --oneshot; clickhouse-client --user backup-admin --password $(grep -oP 'password:\s*\K[^ ]+' /etc/rscheck/checkclickhouse.conf) --query 'SYSTEM RELOAD CONFIG'`
   Либо `python3 /usr/scripts/reload-config.py` на хосте.

## Удаление реплики

1. Удалить реплику из таблицы хостов в mdb-data.
2. Поправить `zen.clickhouse.config.xml` — убрать хост из нужного `<shard>`.
3. В облаке: остановить хост, `withdraw` сервиса и стораджа.
4. Запустить обновление конфига кнопкой в UI на вкладке «хосты».

## Добавить / обновить макрос(ы)

В PMS `clickhouse.macros.xml`. Hostname — `{cluster}-{project}-ch.clouds`.
Внутри `<clickhouse>...</clickhouse>` добавить/обновить `<macros>...</macros>`.
Релоад конфигов.

## Добавить system.text_log

В PMS `zen.clickhouse.additional_config.xml` добавить (или смержить с текущим):
```xml
<clickhouse>
    <text_log>
        <database>system</database>
        <table>text_log</table>
        <partition_by>toYYYYMM(event_date)</partition_by>
        <ttl>event_date + INTERVAL 7 DAY</ttl>
        <flush_interval_milliseconds>7500</flush_interval_milliseconds>
        <max_size_rows>1000000</max_size_rows>
    </text_log>
</clickhouse>
```

## Добавление named_collections

**Сначала предложить пользователю сделать через DDL** —
https://docs.vk.team/mdb/docs/clickhouse/ch-integrations.html. Если не устраивает — через конфиг.

1. В секретах кластера создать папку `named-collections`, добавить секрет с названием
   коллекции и полем `password`. Можно использовать `one-secret` шаблоны.
2. Добавить проперти `zen.clickhouse.additional_config.xml` для кластера. Пример:
```xml
<clickhouse>
   <named_collections>
       <mysql_conn>
           <user>admin</user>
           <password>{{ vault('zkv/mdb/mdbsandbox/ch/hybrid-mdbsandbox-ch.mdbsandbox.db.production.mdb.prod/named-collections/mysql_conn:password', '') }}</password>
       </mysql_conn>

       <remote1>
         <host>1.shard-1-db.hybrid-mdbsandbox-ch.pc.wan.idzn.ru</host>
         <port>9000</port>
         <database>repl_test</database>
         <user>default</user>
         <password>123</password>
       </remote1>
   </named_collections>
</clickhouse>
```
3. Перезапустить хосты кластера, чтобы подтянулись и применились новые конфиги.

Итог: 1 значение в PMS + 1 в vault.

## Добавление конфигурации словарей

**Сначала настоятельно просим пользователя сделать через DDL.** Если не устраивает — через конфигурацию.

1. Указать, где конфигурация словарей — в PMS `zen.clickhouse.additional_config.xml`:
```xml
<clickhouse>
     <dictionaries_config>/etc/clickhouse-server/dict/config_dictionary.xml</dictionaries_config>
</clickhouse>
```
2. Задать конфигурацию словарей — в PMS `zen.clickhouse.config_dictionary.xml`:
```xml
<clickhouse>
    <dictionary>
        <!-- Конфигурация словаря -->
    </dictionary>
    ...
</clickhouse>
```
3. Перезапустить хосты.
4. Проверить:
```sql
SHOW dictionaries;
SELECT * FROM system.dictionaries FORMAT Vertical;
```
Перезагрузить словари:
```sql
SYSTEM RELOAD DICTIONARIES;
SYSTEM RELOAD DICTIONARY my-dictionary;
```

## Добавить словари геобаз

1. Получить от пользователей файлы словарей.
2. Перенести на хосты через скилл [`mcc-host-access`](../../mcc-host-access/SKILL.md)
   (команда `scp`, см. `commands/scp.md`). Файлы — `regions_hierarchy.txt` и
   `regions_names_<ru/en/...>.txt`, путь назначения — `/var/lib/clickhouse/1/regions`
   (на эту папку настроен rsync на случай потери диска). Загружать в директорию, не
   по пути-файлу (см. грабли `scp` в `mcc-host-access`).

3. В PMS `zen.clickhouse.additional_config.xml`:
```xml
<clickhouse>
   <path_to_regions_hierarchy_file>/var/lib/clickhouse/1/regions/regions_hierarchy.txt</path_to_regions_hierarchy_file>
   <path_to_regions_names_files>/var/lib/clickhouse/1/regions/</path_to_regions_names_files>
</clickhouse>
```
4. Релоад конфигов кнопкой в UI.

## Обновить CH на версию 24.8

Если нет логов:
1. Добавить в манифест стораджа `logs`.
2. Обновить PMS `zen.clickhouse.additional_config.xml`:
```xml
<clickhouse>
    <access_control_improvements>
        <on_cluster_queries_require_cluster_grant>false</on_cluster_queries_require_cluster_grant>
        <select_from_system_db_requires_grant>false</select_from_system_db_requires_grant>
        <select_from_information_schema_requires_grant>false</select_from_information_schema_requires_grant>
        <settings_constraints_replace_previous>false</settings_constraints_replace_previous>
        <table_engines_require_grant>false</table_engines_require_grant>
    </access_control_improvements>
</clickhouse>
```
4. В `zen.clickhouse.users.xml` → `default` profile:
```xml
<yandex>
    <profiles>
        <default>
            ...
            <enable_analyzer>0</enable_analyzer>
            <optimize_functions_to_subcolumns>0</optimize_functions_to_subcolumns>
            <allow_deprecated_error_prone_window_functions>1</allow_deprecated_error_prone_window_functions>
        </default>
```
5. В `zen.clickhouse.config.xml` поправить logger если нужно:
```xml
<clickhouse>
    <logger>
        <level>trace</level>
        <log>/mnt/logs/dbms/clickhouse-server.log</log>
        <errorlog>/mnt/logs/dbms/clickhouse-server.err.log</errorlog>
        <size>1000M</size>
        <count>5</count>
    </logger>
```
6. Обновить docker-образы — **сначала кликхаусы, потом киперы**.

## Перенести кипер в другой датацентр

1. Скопировать манифест стораджа кипера, сабмитнуть манифест в другой ДЦ.
2. Если с квотами OK — править конфиги. У кипера на каждый хост свой конфиг (`server_id`
   уникален среди хостов), поэтому копируем с одного из хостов конфиг
   `zen.clickhouse-keeper.config.xml` через кнопку **COPY** в UI PMS. Меняем `host` на нужный
   ДЦ, прописываем новый `<server_id>` (скорее всего будет 4) и дописываем новый хост в его
   же конфиг как один из `<server>`.
3. Добавить новый хост в конфиг всех киперов, перезагрузить киперы по очереди:
   ```bash
   python3 /usr/scripts/reload-config.py
   ```
4. Скопировать манифест сервиса кипера, сабмитнуть в нужный ДЦ.
5. Ждать, когда поднимется кипер в новом ДЦ.
6. Проверить по графане киперов, что число `znode` у новой ноды засинхронизировалось.
7. Убрать нецелевой хост из конфигов 3 киперов, перезагрузить. **Важно**: перезагрузить 1 хост,
   сразу сделать его лидером — `python3 /usr/scripts/request_leadership.py` — и перезагрузить остальные.
8. Остановить нецелевой хост, в бэкстейдже поменять ДЦ в таблице хостов.
9. В PMS в конфиге кликхауса поправить ДЦ кипера — внутри тега `<zookeeper>`
   (`zen.clickhouse.config.xml`).
10. `confp --oneshot` на всех хостах кликхауса.

Если 4-й кипер не подключается сразу — помогает поделать смены лидера на разных хостах:
```bash
python3 /usr/scripts/request_leadership.py
```

## Интеграции с Kafka

- Своя кафка MDB → https://docs.vk.team/mdb/docs/clickhouse/ch-integrations.html#kafka
- Своя кафка пользователя → положить их серт на всех инстансах по пути
  `/var/lib/clickhouse/1/user_scripts/kafka_ca.crt` (синхронизируется при добавлении новых хостов).
  В PMS `zen.clickhouse.config.xml`:
  ```xml
  <clickhouse>
      <kafka>
          <ssl_ca_cert_file>/var/lib/clickhouse/1/user_scripts/kafka_ca.crt</ssl_ca_cert_file>
      </kafka>
  </clickhouse>
  ```
  Рестарт инстансов: `python3 /usr/scripts/reload-config.py`, либо операция обновления конфига,
  либо оператор.
