# test-downgrade7 (2026-09-03): URP-петля после даунгрейда — батчи с partitionLeaderEpoch вне leader-epoch-checkpoint

## Симптом

После даунгрейда 4.3→3.8 кластер test-downgrade7-mdbdev-kafka: 3 недореплицированные
партиции (test3-0, test3-2, __consumer_offsets-21), лидер 22001 (1.broker.pc), ISR=[22001].
В UI/UAP-таблице брокеры AVAILABLE, CC «Proposal ready».

В логах реплик (hc=20001, kc=21001) — шторм (десятки/сек):

```
ReplicaFetcher replicaId=20001, leaderId=22001 ... Reset fetch offset for partition
__consumer_offsets-21 from 47 to the current local replica's end offset 47
Current offset 47 ... is out of range, which typically implies a leader change.
```

## Разбор

1. На лидере `kafka-get-offsets` (latest): test3-0=76, test3-2=84, offsets-21=51.
   Реплики зациклены ровно на этих же оффсетах (47 = граница второго сегмента offsets-21).
2. `kafka-dump-log.sh` головного сегмента лидера `__consumer_offsets-21/00000000000000000000.log`
   (файл 363B, имя = базовый оффсет 0): внутри батчи **оффсетов 43..46 с
   `partitionLeaderEpoch: 7`**, при этом leader-epoch-checkpoint лидера = {2:0, 3:47, 5:51}
   (текущая эпоха 5). Эпоха 7 — остаток 4.3-эры, в чекпойнте 3.8 её нет.
3. Механика петли: реплика копирует батчи с epoch 7 → её lastFetchedEpoch=7 → лидер не
   валидирует такой fetch → вечный OFFSET_OUT_OF_RANGE → reset на локальный end offset → цикл.
   Consumer'ы не страдают (не шлют epoch, читают [47..50] нормально) — только репликация.
4. Почему test3-0/test3-2 вылечились после вайпа реплик, а offsets-21 нет: логи test3-*
   на лидере ПУСТЫЕ (один сегмент с 0 байтами, [76..76)/[84..84)) — копировать нечего,
   poison-батчей нет. offsets-21 единственный имел реальные данные с инопланетной эпохой.

## Фикс

1. На **лидере** (pc): stop kafka-broker → убрать (в бэкап) головной сегмент
   `00000000000000000000.{log,index,timeindex,snapshot}` из партишн-директории → start.
   Лог стал начинаться с 47 (logStartOffset=47, LEO=51). Потеря: 4 записи offsets-21
   (тестовые consumer-группы от Aug 12). Бэкап: /tmp/offsets21-poison-backup/.
2. На **репликах** (hc, kc): stop kafka-broker → `mv` партишн-директории (бэкап
   `*.bak-reset2-20260903`) → start. Реплики пересоздались и догнали лидера с 47.
3. Верификация: UnderReplicatedPartitions=0 (Jolokia 7777) на всех 3 брокерах,
   `kafka-topics --under-replicated-partitions` пусто, новых Reset-строк в логах нет.

Важно: вайп ТОЛЬКО реплик не помогает — они заново копируют poison-сегмент и петля
возвращается. Чистить надо источник (лидер). При удалении головного сегмента нового
реплики-с-нуля не ломаются: fetch с 0 при leader logStart=47 → Kafka сама делает
truncateFullyAndStartAt(47) на реплике (ветка doOffsetOutOfRange).

## Грабли

- **grep бинарного вывода**: dump-log + ANSI → `grep -av` (иначе "Binary file matches").
- `kafka-console-consumer` принимает `--consumer.config`, НЕ `--command-config`.
- Перезаписанные бэкап-директории на брокерах переименовываются внешним чистильщиком
  в `*<hash>-stray` (mdb-data/CC) — не пугаться, это норма.
- Console-consumer при out-of-range молча прыгает по auto.offset.reset=latest —
  тест «consumer прочитал и затаймлился» НЕ доказывает, что fetch по оффсету работает.

## Правило на будущее (добавить в чеклист даунгрейда)

После ре-формата/переименования партишн-директорий проверить на каждом брокере:
`kafka-dump-log.sh --files <партиция>/*.log | grep partitionLeaderEpoch` против
`leader-epoch-checkpoint` — батчи с эпохой, отсутствующей в чекпойнте (или выше текущей),
= гарантированная URP-петля после старта. Чистить сегмент на лидере до включения кластера
в работу.
