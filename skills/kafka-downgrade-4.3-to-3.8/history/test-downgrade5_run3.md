# test-downgrade5 — run3 (2026-08-11)

Даунгрейд `test-downgrade5-mdbdev-kafka` 4.3 → 3.8. **Применён Method A** (удаление `.index`/`.timeindex`/`.snapshot`/`leader-epoch-checkpoint` на Этапе 6) — грабля #18 НЕ сработала, `retention.ms=-1` НЕ понадобился.

## Условия

- 6 хостов: 3 broker (hc/kc/pc) + 3 controller (hc/kc/pc)
- Cruise-хост НЕ трогался
- Топики: `test`, `test2`, `test3`, `__consumer_offsets` (50 partitions)
- Records в `test`/`test2`/`test3` с `CreateTime` = 2026-08-06 (5 дней назад от даты даунгрейда, в пределах default retention.ms=604800000 = 7 дней)

## Применённый метод

Этап 6 — rename_stray_v1.sh. Для каждой topic-папки:
```bash
rm -f "$d"/*.index "$d"/*.timeindex "$d"/*.snapshot "$d"/leader-epoch-checkpoint "$d"/*-checkpoint
printf "version: 0\ntopic_id: %s\n" "$TID" > "$d/partition.metadata"
```
Оставлены только `.log` + `partition.metadata`. `retention.ms` НЕ менялся — default 604800000 (7 дней).

## Результат

- Все 3 брокера стартовали без удаления `.log` retention'ом.
- `ls -la /mnt/data/log/test-*/*.log` показывает ненулевые размеры на всех 3 брокерах.
- `kafka-dump-log.sh --files .../test-0/*.log` показывает records с реальными `CreateTime` (2026-08-06), НЕ 1970-01-01.
- `kafka-consumer-groups --all-groups --describe` показывает все partitions с офсетами как в бэкапе Этапа 0.
- `kafka-get-offsets` офсеты совпадают с бэкапом.

## Почему сработало

Без `.index`/`.timeindex`/`.snapshot` Kafka 3.8 не может загрузить segment metadata из индексов → вынуждена сделать **полный log recovery**: построчно сканирует `.log` и восстанавливает `maxTimestampSoFar` из реальных records (`CreateTime` каждого record). После этого `largestRecordTimestamp` = реальный timestamp последней записи (2026-08-06), retention видит сегменты как свежие → не удаляет.

В Method B (где `.index`/`.timeindex` оставлены) Kafka использует индексы для восстановления metadata, не сканирует `.log`, и `largestRecordTimestamp` остаётся = 0 → retention удаляет сегменты.

## Время старта

- broker.hc — ~15 сек (контроллеры уже active)
- broker.kc — ~20 сек (задержка из-за log recovery на segment-ах ~1 GB)
- broker.pc — ~30 сек (container recreation delay + log recovery)

Для прод-кластера с segment-ами 1 GB × 1000 partitions — ожидается несколько минут на брокера. Если недопустимо — применять Method B (с `retention.ms=-1`).

## Выводы

- **Method A — основной и рекомендуемый метод.** Грабля #18 НЕ срабатывает, retention.ms=-1 не нужен, retention можно вернуть на default.
- Минус — полный log recovery при старте. На тестовом кластере с небольшими segment-ами — секунды. На проде с большими segment-ами — минуты.
- `kafka-dump-log` — надёжная проверка `largestRecordTimestamp`: baseOffset/lastOffset/CreateTime поля показывают реальные timestamps, не 1970.
