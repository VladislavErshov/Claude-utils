# Инцидент 2026-08: Хост CH не поднимается — Dependency failed (диск /mnt/logs забит на 100%)

Кластер: `uv-content-id-meta-dev-uv-ch`
Тикет: `MDBSUP-4673`
Хост: `1.shard2-db.uv-content-id-meta-dev-uv-ch.kc.one-infra.ru`
Версия: ClickHouse 24.x (MDB).

## Симптомы

- Хост в статусе UNAVAILABLE, ClickHouse не отвечает.
- `systemctl is-active mdb-clickhouse-server` → `inactive (dead)`.
- В journalctl:
  ```
  Aug 18 14:24:49 ... systemd[1]: Dependency failed for Clickhouse Server.
  Aug 18 14:24:49 ... systemd[1]: mdb-clickhouse-server.service: Job mdb-clickhouse-server.service/start failed with result 'dependency'.
  ```
- `rscheck@checkclickhouse` живёт и спамит `Connection refused` на 127.0.0.1:8123.

## Причина

`mdb-clickhouse-server` зависит от `dir-init.service` (создаёт директории под логи).
`dir-init` падает на старте:
```
mkdir: cannot create directory '/mnt/logs/analytics/probes': No space left on device
```

Диск `/mnt/logs` (15G) забит на 100%. Виновник — `/mnt/logs/dbms/clickhouse-syslog.err.log`
(root-owned, пишется отдельным syslog-демоном, не CH-сервером) разросся до ~15GB
за ~5 дней (последний write Aug 14). CH-сервер тут ни при чём — он даже не запущен.

```
15G  /mnt/logs/dbms/clickhouse-syslog.err.log   ← root-owned, активный writer
178M /mnt/logs/dbms/clickhouse-server.log
177M /mnt/logs/dbms/clickhouse-server.err.log
27M  *.log.{0,1,2,3}.gz                          ← ротация логов CH-сервера (живёт)
```

`df -h /mnt/logs`:
```
/dev/mapper/cloud.nvme-f258816c9dea11f085c017883041a323  15G  15G  20K  100% /mnt/logs
```

## Как диагностировали

1. `systemctl status mdb-clickhouse-server` → подсветил `Dependency failed`.
2. `systemctl list-dependencies mdb-clickhouse-server` → нашёл `dir-init.service` в списке.
3. `systemctl status dir-init.service` → увидели `mkdir: No space left on device`.
4. `df -h` → `/mnt/logs` 100%.
5. `du -sh /mnt/logs/*` → `dbms` занимает 15G.
6. `ls -lah /mnt/logs/dbms/` → `clickhouse-syslog.err.log` 15G.

## Фикс

```bash
mcc --local sshexec -n infra 1.shard2-db.uv-content-id-meta-dev-uv-ch.kc.one-infra.ru \
  "systemctl stop mdb-clickhouse-server rscheck@checkclickhouse 2>/dev/null; \
   rm -rf /mnt/logs/dbms/* /mnt/logs/analytics/* /mnt/logs/system/* /mnt/logs/vector/*; \
   systemctl start mdb-clickhouse-server"
```

После фикса:
- `/mnt/logs`: 524M/15G (4%).
- `mdb-clickhouse-server`: `active (running)`.
- `curl http://127.0.0.1:8123/ping` → `Ok.`
- `dir-init` отработал, dependency снята — CH стартовал сам.

## Чему учит

- **`Dependency failed for <X>`** — сначала смотри `systemctl list-dependencies <X>` и статус
  завязанного юнита (`dir-init`, `rscheck@*`, `sysinit.target`). Часто CH жив, а dependency-юнит
  упал по посторонней причине (диск, права, PMS-рендер).
- **CH-сервер не пишет `clickhouse-syslog.err.log`** — этот файл root-owned, пишет системный
  syslog-демон. Ротация логов CH-сервера (`*.log.N.gz`) ничего не делает для syslog-файла,
  поэтому он может бесконтрольно расти. На dev-кластерах без тяжёлого трафика это типичная
  грабля.
- **`/mnt/logs` — отдельный small disk (обычно 15G)**. Не путать с `/var/lib/clickhouse/1`
  (диск с данными). Логи и данные живут на разных LVM-томах.
- **Фикс "удалить логи и рестартнуть"** безопасен: `dir-init` пересоздаст директории, CH-сервер
  начнёт писать логи с нуля. Теряются только старые логи — для разбора инцидента их обычно
  уже не нужно.
- **Стоит проверить рецидив**: если `clickhouse-syslog.err.log` снова разрастается за дни —
  значит какой-то системный компонент активно пишет ошибки (часто rsyslog ловит что-то
  из systemd-journald или CH кидает в syslog). Лечится настройкой logrotate на
  `/mnt/logs/dbms/clickhouse-syslog.*.log` или поиском источника спама.

## Каталог известных проблем — обновить

Добавить в `commands/known_issues.md`:
- **Хост CH не поднимается, `Dependency failed for Clickhouse Server`** — проверить
  `dir-init.service` (часто `No space left on device` на `/mnt/logs`). Виновник обычно
  `clickhouse-syslog.err.log` (root-owned, не ротируется CH). Фикс: `rm -rf /mnt/logs/dbms/*`
  + `systemctl restart mdb-clickhouse-server`.

## Ссылки

- Скилл `clickhouse-cluster-inspector/SKILL.md` — пути на хосте (`/mnt/logs/dbms/`).
- `mcc-host-worker/commands/sshexec.md` — неинтерактивный запуск команд на CH-хосте.
