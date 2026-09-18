# 2026-09-10 — INCALL-52118: RUNNING UNAVAILABLE реплика (облако пересоздало диск)

## Кратко
- **Хост:** `1.db.comments2-social-pgsql.ec.idzn.ru` (реплика, ДЦ ec, namespace dzen).
- **Кластер:** `comments2-social-pgsql` (id `12ba8a60-eeae-45c6-ba35-343e29d135da`;
  4 keeper'а: dc = мастер, pc/rc/ec = реплики).
- **Симптом:** алерт Grafana OnCall «MDB PostgreSQL cluster has RUNNING UNAVAILABLE
  replica for 8h» → INCALL-52118 (P3, impact 0%). mdb-health видел хост UNAVAILABLE
  с 16:28 09.09 до 01:01 10.09.
- **Диагноз:** облако в 16:28 09.09 пересоздало инстанс с **новым пустым nvme-диском**
  → keeper не нашёл локальный db UID → сам запустил полную переналивку `pg_basebackup`
  (~772 ГБ, max-rate 25M ≈ 8.5 ч). База поднялась в 01:01 и догнала мастера.
- **Действия:** ручной починки не потребовалось — переналивка шла штатно и завершилась
  сама; алерт сработал за 12 минут до её окончания. В беседу инцидента отправлены
  `/s` + `/close`.

## Хронология инцидента

| Время (MSK) | Событие |
|---|---|
| 09.09 16:26–16:28 | Облако пересоздало инстанс: новый `runid`, новый volume (uuid = runid), все контейнеры/сервисы перезапущены, логи созданы с нуля, wtmp сброшен. Аптайм хоста при этом 2.5 дня (не reboot, а пересоздание VM-инстанса с новым диском). |
| 16:28:32 | stolon-keeper: `current db UID different than cluster data db UID {"db": "", "cdDB": "c31ae5e0"}` → `database cluster not initialized` → `resyncing the database cluster` → `pg_basebackup` с мастера (dc), max-rate 25M. |
| 16:28 → 00:52 | pg_basebackup копирует 772 522 471 kB. Всё это время postgres лежит: pgbouncer `sbuf_connect failed ... /tmp/.s.PGSQL.5432: No such file or directory`, rscheck checkpgsql → UNAVAILABLE, host-checker рапортует в mdb-health UNAVAILABLE каждые 5 с. |
| 10.09 00:49 | Grafana OnCall: «RUNNING UNAVAILABLE replica for 8h» → INCALL-52118. |
| 00:52:48 → 00:56:17 | Keeper: `sync succeeded` → `starting database`; postgres.log: `database system was interrupted; last known up at 00:52:48`, entering standby mode, redo starts at 6212/E44DF068. |
| 01:01:27 | `consistent recovery state reached at 6219/A50878C8` → `database system is ready to accept read-only connections`. |
| 01:01:37 | pg_isready через pgbouncer проходит → host-checker шлёт AVAILABLE. |

## Диагностика

- `mcc --local -n dzen -c ec instances <host> -f yaml` — state=RUNNING,
  availability=RESERVED, `availability_details: "node role: SECONDARY, etcd role:
  SECONDARY"`; **маркер пересоздания**: `runid: 0efc2f60-...-f9b71d2087` и имя диска
  `/dev/mapper/cloud.nvme-a2736b4d...f9b71d2087` — общий суффикс, т.е. volume создан
  этим же run'ом. `started` инстанса = момент рестарта сервисов.
- `stolonctl status` — после завершения все keeper'ы healthy, мастер dc, наш ec standby.
- `/mnt/logs/system/host-checker.log` — точные границы UNAVAILABLE/AVAILABLE и
  что именно шлётся в mdb-health (`health.mdb.one-infra.ru/api/mdb-health/host/`).
  `grep -c UNAVAILABLE` → 5819 записей в окне 16:28:23 → 01:01:32.
- `/mnt/logs/dbms/pgbouncer.log` — ошибки коннекта к юникс-сокету постгреса в окне
  недоступности; в 01:01:37.598 первый успешный login.
- `/mnt/logs/dbms/stolon-keeper.log` — причина и ход переналивки (resync →
  pg_basebackup с прогрессом → `sync succeeded`).
- `/mnt/logs/dbms/postgres.log` — текущий файл создан заново при рестарте; история
  до пересоздания диска недоступна (это нормально для данного сценария).
- `curl -s http://127.0.0.1:81/getstatus` на хосте — отдаёт только строку роли
  (`node role: SECONDARY, etcd role: SECONDARY`); ранг AVAILABLE/UNAVAILABLE ищется
  в host-checker.log, не в getstatus.
- Все сервисы стартовали одной секундой (16:28:19–22) — признак пересоздания
  инстанса, а не падения отдельного сервиса.

## Извлечённые уроки

1. **Алерт «RUNNING UNAVAILABLE реплика > N часов» — сначала host-checker.log и
   stolon-keeper.log**, а не починка хоста: длительная штатная переналивка выглядит
   для mdb-health точно так же, как лежащий хост. Если pg_basebackup идёт с
   постоянным прогрессом — хост «чинится сам», остаётся дождаться.
2. **Маркер пересоздания диска облаком**: `runid` инстанса == суффикс имени volume
   (`cloud.nvme-<uuid>`), все логи в `/mnt/logs/` созданы одним timestamp'ом,
   wtmp начинается с момента рестарта. Причина пересоздания — только в cloud-audit,
   с хоста не видна; `mcc ops` для postgres-партиций пуст (оператор — только Kafka).
3. **Полная переналивка ~772 ГБ на max-rate 25M ≈ 8.5 ч** — алерт с порогом 8h
   срабатывает до её естественного завершения. Это норма, не второй инцидент.
4. **`/s` и `/close` для INCALL — в беседу myteam**, в Jira-тикет не писать: команды,
   отправленные в Jira, статус не меняют (проверено на этом инциденте — сначала
   написали в тикет, закрылось только из беседы). Jira-тикет INCALL — зеркало
   алерта, комментарии туда не нужны.
5. dzen-хосты (`*.idzn.ru`) в mcc — namespace `dzen` (`mcc --local -n dzen -c <dc> ...`),
   не `infra`.

## Ссылки
- `jira-mdbsup-solver/SKILL.md` — раздел «Инциденты INCALL» (поиск беседы, /s, /close).
- Инструкция из алерта: Confluence «RUNNING UNAVAILABLE реплика постгреса»
  (pageId=1967549817) — пороги, backupNode, диск 100%.
- `history/2026-07-23-timeline-gap-shard1.md` — ручная переналивка (здесь stolon
  сделал её сам, без removekeeper — диск был уже пуст).
