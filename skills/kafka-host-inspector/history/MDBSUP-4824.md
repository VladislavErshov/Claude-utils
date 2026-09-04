# MDBSUP-4824 — Забит диск на брокере (100%) из-за stray-партиций после прерванной ребалансы

**Кластер:** `extdbpu-oneme-kafka` (kc)
**Хост:** `57.broker.extdbpu-oneme-kafka.kc.one-infra.ru`
**Дата:** 2026-08-25

## Симптом

Диск `/mnt/data` забит на 100% (3.0T, Avail 196K).

## Диагностика

```bash
mcc --local -n infra sshexec 57.broker.extdbpu-oneme-kafka.kc.one-infra.ru \
  "df -h /mnt/data; du -sh /mnt/data/log/*-stray 2>/dev/null | sort -h | tail -20"
```

Логи (`/mnt/logs`) чистые (635M из 10G) — проблема не в logrotate. В `/mnt/data/log`
оказалось ~1.9 ТБ каталогов `*-stray` — последствия прерванной ребалансы Cruise Control /
reassign. Крупнейшие: `oneme_core_NavigationEvents-7.*-stray` (349G), 8× `oneme_core_PushEvents-*`
(~121G каждый), `userReadMarks`, `OnemeCallsEvents`.

## Решение (по инструкции дежурного MDB: Kafka, «Кончилось место на брокерах»)

1. Подсчитать и удалить stray:
   ```bash
   mcc --local -n infra sshexec <host> \
     "cd /mnt/data/log && du -sk *-stray | awk '{s+=\$1} END {print s+0}' && rm -rf *-stray"
   ```
   → удалено 2 029 794 560 KB (~1.9 ТБ), диск 100% → 37%.
2. Рестарт брокера (иначе память может долго не обновляться):
   ```bash
   systemctl restart kafka-broker
   ```
   → active.

## Как применять

При 100% диске на Kafka-брокере **сначала** проверять `*-stray` в `/mnt/data/log` — это
безопасно удаляемые данные прерванной ребалансы (конфлюенс-инструкция дежурного это
канонизирует). Логи в `/mnt/logs` — отдельный случай (баг logrotate, чистить `kafka-*`).

После очистки — рестарт `kafka-broker`. Дальше выяснять причину прерывания ребалансы
(CC / reassign на кластере).

Конфлюенс: https://confluence.vk.team/pages/viewpage.action?pageId=1348619075
(раздел «Кончилось место на брокерах»).
