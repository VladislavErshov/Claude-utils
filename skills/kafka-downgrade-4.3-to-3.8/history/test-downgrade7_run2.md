# test-downgrade7 — run2 (2026-08-11)

Повторный даунгрейд `test-downgrade7-mdbdev-kafka` 4.3 → 3.8. Использовался **Method B** (без удаления `.index`/`.timeindex`, только `.snapshot`) — грабля #18 сработала на 2 из 3 брокеров.

## Условия

- 6 хостов: 3 broker (hc/kc/pc) + 3 controller (hc/kc/pc)
- Cruise-хост НЕ трогался
- Бэкап KRaft meta-log с контроллеров сделан на живом кластере
- Топики: `test` (p0=30, p1=0, p2=276), `test2` (p0=0, p1=117, p2=0), `test3` (p0=0, p1=158, p2=68), `__consumer_offsets` (50 partitions)
- Consumer groups: `consumer-group`, `consumer-group2`, `consumer-group3`

## Применённый метод

Этап 6 — переименование stray → canonical с переписыванием `partition.metadata`. **Удалены только `.snapshot` и `leader-epoch-checkpoint`** (Method B). `.index`/`.timeindex` оставлены. `retention.ms=-1` НЕ ставился при `--create` (в скилле на тот момент ещё не было предупреждения).

## Что произошло

После старта брокеров:
- broker.hc — `.log` сохранены (retention не успел сработать)
- broker.kc — `.log` обнулён на test/test2/test3 partitions через ~30 сек после старта
- broker.pc — `.log` обнулён аналогично

`kafka-get-offsets` показывал офсеты как в 4.3 (`test:2:276`), но `ls -la /mnt/data/log/test-2/*.log` = 0 байт. Грабля #18.

## Восстановление

1. Поставлен `retention.ms=-1` на test, test2, test3 через `kafka-configs --alter --add-config retention.ms=-1`.
2. Брокеры остановлены.
3. `recover_log.sh` запущен на broker.hc (источник) для каждой partition test/test2/test3 — копировал `.log`/`.index`/`.timeindex`/`leader-epoch-checkpoint` на broker.kc и broker.pc.
4. Брокеры стартованы.
5. `kafka-dump-log` подтвердил records на всех 3 брокерах.

## Потери

- `consumer-group3/test3:1` — records 0..157 потеряны безвозвратно. LOG-END-OFFSET сместился на 158 до восстановления. Reset к 96 выдал `New offset (96) is lower than earliest offset for topic partition test3-1. Value will be set to 158`. Финальный reset — к 158, LAG=0.

## SCRAM users и ACLs

Восстановлены из дампа Этапа 0 через `kafka-configs --alter --add-config 'SCRAM-SHA-256=[password=...,iterations=8192]'`. Супер-пользователь `super` прописан в broker.properties через mdb-data — не воссоздавался. `kafka_exporter` ACLs восстановлены автоматически через mdb-data при старте `kafka-exporter.service`.

## Выводы

- Грабля #18 недетерминирована — на broker.hc данные сохранились, на kc/pc — нет. Зависит от порядка старта и скорости retention check.
- Method B (без удаления `.index`/`.timeindex`) НЕ надёжен — на 2 из 3 брокеров retention удалил `.log`.
- Реактивный фикс через `recover_log.sh` с непострадавшего брокера работает, но требует ручной проверки размеров `.log` сразу после старта.
- Следующий даунгрейд (`test-downgrade5_run3`) — протестирован **Method A** (удаление `.index`/`.timeindex`/`.snapshot`) — грабля #18 НЕ сработала.
