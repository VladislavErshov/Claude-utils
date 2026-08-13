# test-downgrade7 — run3 (2026-08-12)

Даунгрейд `test-downgrade7-mdbdev-kafka` 4.3-IV0 → 3.8-IV0. Третий прогон на этом кластере.

## Параметры кластера
- 3 broker: hc (node.id=20001), kc (21001), pc (22001)
- 3 controller: hc (10001, leader), kc (11001, follower), pc (12001, follower)
- cluster.id=23f108ac-1907-434e-a67b-dda01df316f4 (сохранён)
- Cruise-хост игнорировался (feedback memory)

## Топики в 4.3
- test (3p, retention.ms=-1)
- test2 (3p, retention.ms=-1)
- test3 (3p, retention.ms=-1)
- __consumer_offsets (50p, compact)
- __CruiseControlMetrics (9p, retention.ms=18000000) — Kafka создаст сама при старте CC
- __share_group_* — отсутствовали (не использовались)

## Процедура
- Этап 0: снят свежий дамп (state_4_3.tar.gz, 8 файлов). SCRAM users: test-user, test-user13 (оба iterations=4096). ACLs — пустые.
- Этап 1: бэкап KRaft meta-log со всех 3 контроллеров (controller-hc/kc/pc/metadata/ + log/).
- Этап 2: пользователь переключил docker-образ на 3.8.
- Этап 3: format_broker.sh + format_controller.sh на 6 хостах, cluster.id сохранён.
- Этап 4: контроллеры + брокеры стартовали, quorum OK (LeaderId=10001, 3 voters + 3 observers).
- Этап 5: create_topics.sh создал test/test2/test3/__consumer_offsets с параметрами из дампа. 59 stray на каждом брокере (50+3+3+3).
- Этап 6: rename_stray.sh — 0 stray, partition.metadata переписан, .index/.timeindex/.snapshot/leader-epoch-checkpoint удалены (метод variant1, feedback memory).
- Этап 7: брокеры стартовали. SCRAM users восстановлены: test-user, test-user13 (iterations=4096).
- Этап 8: verify — см. ниже.
- Этап 9: host-check + rscheck + vector + rsyslog + systemd-journald на 6 хостах. share-group-lag-exporter оставлен active (не disabled, по feedback memory).

## Результат verify
- Все 5 топиков созданы с правильными параметрами.
- Under-replicated partitions: пусто.
- 3 брокера в api-versions: 20001 (hc), 21001 (kc), 22001 (pc).
- .log размеры на 3 брокерах стабильны через 40 сек после старта (грабля #18 НЕ сработала):
  - test-0=1800B, test-1=0B (нет records в 4.3), test-2=16437B
  - test2-0=0B, test2-1=6927B, test2-2=0B
  - test3-0=4437B, test3-1=9357B, test3-2=4794B (hc) / 837B (kc/pc)
- Офсеты consumer groups совпадают с 4.3, КРОМЕ `consumer-group3/test3:1` — пропала (CURRENT=158 в 4.3, нет строки в 3.8). LOG-END-OFFSET test3:1=158 — данные в `.log` есть, но group coordinator не загрузил commit. Это грабля #21 (4.3 records в `__consumer_offsets-1` не десериализуются 3.8).
- 0 stray.
- SCRAM users: test-user, test-user13 — восстановлены.
- В UI облака все хосты AVAILABLE, роли корректные (broker=observer, controller.hc=leader, kc/pc=follower).

## Известные проблемы
- `consumer-group3/test3:1` — отсутствует commit в 3.8. Фикс по грабле #21: удалить `__consumer_offsets-1` на 3 брокерах (с остановкой) и сделать reset offsets. В данном прогоне не применён (тестовый кластер, пользователь не запрашивал).

## Файлы процедуры
- `~/kafka_4.3_backup/test-downgrade7/etap0_state/` — дамп 4.3
- `~/kafka_4.3_backup/test-downgrade7/controller-{hc,kc,pc}/` — бэкапы KRaft meta-log
- `~/kafka_4.3_backup/test-downgrade7/{format_broker,format_controller,rename_stray,create_topics,restore_scram,start_infra}.sh` — скрипты процедуры
- `~/kafka_4.3_backup/test-downgrade7/new_topics.txt` — новые topic_id после --create
