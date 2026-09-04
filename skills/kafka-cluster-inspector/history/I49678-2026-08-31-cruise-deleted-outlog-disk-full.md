# Забитый /mnt/logs на cruise-хосте: удалённый cruise-control.out.log на 10GB, удерживаемый процессом

Дата: 2026-08-31
Тикет: I49678 (по словам пользователя; в Jira запрос не найден — 404, разбора под тикетом не делали)
Кластер: `vk-notify-status-events-uv-kafka` (KRaft, 3×broker pc/rc/uc + 3×controller pc/rc/uc + 1 cruise в dc)
Хост: `1.cruise.vk-notify-status-events-uv-kafka.dc.one-infra.ru`

## Симптом

В UI mdb-data у cruise-хоста диск `/mnt/logs` = 100% (10G/10G, свободно 20K). Хост при этом
AVAILABLE, роль «Proposal ready». Сам кластер полностью здоров:

| Проверка | Значение |
|---|---|
| Quorum (`kafka-metadata-quorum describe --status`) | LeaderId 12001 (uc), voters [12001, 10001, 11001], epoch 172 |
| Зарегистрированные брокеры (`kafka-broker-api-versions.sh`) | 20001, 21001, 22001 — все 3 |
| `--unavailable-partitions` | пусто |
| `--under-replicated-partitions` | 0 |
| CC `/state` | proposal ready, все goals ready, monitoredWindows 5/5, coverage 100% |
| Диски брокеров | /mnt/data 41%/33%/44%, /mnt/logs 4–6% |
| Диски контроллеров | 2–4% |

## Диагностика

1. `df -h /mnt/data /mnt/logs` по всем 7 хостам (mcc sshexec) — забит только `/mnt/logs` cruise-хоста.
2. `du -sh /mnt/logs/*` на cruise-хосте → всего ~180MB (`/mnt/logs/system` с host-checker.log*).
   **Расхождение df (10G) vs du (180MB) → место держат удалённые открытые файлы.**
3. `lsof +L1 | grep -i deleted`:

```
java 853 cruisecontrol 1w REG 252,25 10414923776 0 135 /mnt/logs/dbms/cruise-control.out.log (deleted)
```

Процесс CC держит удалённый stdout-лог на ~9.7GiB (fd 1).

## Корень

- Лог `cruise-control.out.log` рос из-за спама host_checker'а: `GET /state?verbose=true`
  каждые ~5 сек, каждый запрос пишет в лог несколько INFO-строк с полным JSON AnalyzerState
  (~2–3KB) → ~50MB/сутки.
- 30-го (~19:15) лог удалили руками (видимо, для освобождения места), но CC не перезапустили
  → fd остался открытым, место не освободилось, файл продолжил расти через открытый fd.
  Классика: rm без рестарта процесса = диск как был забит, так и остался.

## Фикс

```bash
mcc --local sshexec -n infra 1.cruise.<cluster>.dc.one-infra.ru \
  "systemctl restart cruise-control; df -h /mnt/logs"
```

Рестарт CC безопасен (операций нет; proposals прогреваются ~25 мин после старта).
Результат: `/mnt/logs` 10G/10G → 244M (3%), CC поднялся, отвечает 200 на `/state`.

Альтернатива без рестарта (если CC трогать нельзя): `truncate -s 0 /proc/<pid>/fd/1` —
освобождает место мгновенно, но файл продолжит расти и без ротации снова забьёт диск.

## Ротация

На cruise-хостах есть `/etc/logrotate.d/cruisecontrol-logs.conf`:

```
/mnt/logs/dbms/*.log
{
    su root root
    daily
    size 100M
    rotate 10
    missingok
    compress
    notifempty
    copytruncate
    sharedscripts
    extension .log
    dateext
    dateformat .%Y-%m-%d-%H.%M.%S
}
```

`copytruncate` корректно работает с процессом, держащим файл (не переименовывает, а
копирует+обнуляет). Проверка: `logrotate -d /etc/logrotate.d/cruisecontrol-logs.conf`.
10GB-файл вырос до того, как ротация заработала (файлы в /mnt/logs/dbms датированы 6-го
августа); при повторении сначала проверить, что logrotate вообще запускается
(`systemctl status logrotate.timer`, `/var/lib/logrotate/status`).

## Грабли

1. **df 100% при маленьком du → сразу `lsof +L1`** (удалённые открытые файлы), не искать
   «потерянное» место по директориям.
2. **`/proc/<pid>/fd/N` удалённого файла читаем** — `tail -c 3000 /proc/853/fd/1` показал,
   что CC пишет прямо сейчас (полезно понять, спам ли это), до принятия решения о рестарте.
3. **На брокерах этого кластера нет `/etc/kafka/kafka-console-consumer.properties`** (в
   `/etc/kafka/` только `get-user-info.sh`). Правильный client-config для CLI:
   `/opt/kafka/config/client.properties`. CLI-инструменты не в PATH — вызывать полным путём
   `/opt/kafka/bin/kafka-topics.sh` и т.п.
4. **CLI-инструменты пишут INFO AdminClientConfig в stdout** — `2>/dev/null` не помогает,
   вывод «утопает» в конфиге AdminClient. Фильтровать по сути:
   `grep -E "ClusterId|LeaderId|CurrentVoters|CurrentObservers"` для quorum,
   `grep -E "Topic:|Partition"` для topics, `grep -vE "INFO|Warning"` как общий фильтр.
5. Локальные awk-однострочники внутри sshexec-команды с вложенными кавычками ломаются
   (`zsh: unmatched '` — экранирование `$7` и кавычек внутри одиночной строки). Простые
   команды + grep надёжнее.
