# Сплит-брейн: фенсинг выпавшей ноды и разбор расхождения

Конкретные команды из инцидента rb-reklama-vkfeed-adtech-mysql (2026-09-02),
см. [../SKILL.md](../SKILL.md) → «Сплит-брейн» и [../history/](../history/).

## Фенсинг (порядок важен: сначала proxysql, потом read_only)

```bash
# 1. proxy-admin на живом source:
/usr/local/percona/bin/mysql --defaults-extra-file=/etc/percona/.my-proxy-admin.cnf -h 127.0.0.1 -P 6032 \
  -e "UPDATE mysql_servers SET status='OFFLINE_SOFT' WHERE hostname='<выпавший>' AND hostgroup_id IN (1,2); LOAD MYSQL SERVERS TO RUNTIME; SAVE MYSQL SERVERS TO DISK"

# 2. на выпавшей ноде:
/usr/local/percona/bin/mysql --defaults-extra-file=/etc/percona/.my-admin.cnf \
  -e 'SET GLOBAL super_read_only=1; SET GLOBAL read_only=1'
```

Проверка, что записи прекратились — `show master status` дважды с интервалом 30с
(позиция бинлога должна замереть).

Грабля: watch-таска оператора через ~минуту вернёт ноду ONLINE в HG2 (чтения) —
это штатно; HG1 (writers) она не возвращает.

## Какие таблицы расходились (mysqlbinlog по GTID)

```bash
/usr/local/percona/bin/mysqlbinlog -vv --base64-output=DECODE-ROWS \
  --include-gtids='<uuid-хоста>:<start>-<end>' binlog.06[7-9][0-9] \
| sed -n 's/.*[Tt]able[_ ]\{0,1\}map: `\([^`]*\)`\.`\([^`]*\)`.*/\1.\2/p' \
| sort | uniq -c | sort -rn
```

- Запускать через nohup на хосте (13ГБ бинлогов ≈ минуты): обёртка base64-скриптом,
  вывод в `/tmp/*_tables.txt`.
- Границы диапазона — из `show master status` обеих сторон (чего нет у одного).
- Retention бинлогов ~6–8 дней: ранняя часть расхождения — только в wal-g бекапе.

## Перестройка ноды (данные теряемы)

Канон — дока «Дежурство MDB: MySql» → «Восстановление реплики из бекапа».
Ключевые шаги: stop mysql → wipe `/mnt/data/mysql` → data/tmp/redo-archive + chown →
`wal-g --config /etc/backups/wal_g_config.env backup-fetch <ИМЯ>` (не LATEST вслепую —
смотреть backup-list, чужие ручные пуши возможны) → GTID_PURGED из
`xtrabackup_binlog_info` → правка `/etc/percona/init.py` → `systemctl start mysql`.

После: `show replica status` (source = мастер), orchestrator `/api/clusters`
(лишний алиас схлопнется), proxysql HG1, rscheck → REPLICA.

## Грабли

- `pkill -f xtrabackup` через sshexec убивает свою сессию — bracket-трюк:
  `pkill -f 'xtrabacku[p] --backup'`.
- Долгие задачи (wal-g, mysqlbinlog) — только nohup + опрос файла.
