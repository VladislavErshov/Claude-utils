# test-downgrade5-mdbdev-kafka — даунгрейд 4.3→3.8 (2026-08-09, run 2)

Второй даунгрейд `test-downgrade5-mdbdev-kafka` 4.3 → 3.8 выполнен 2026-08-09 (после повторного подъёма кластера до 4.3). Процедура по скиллу `kafka-downgrade-4.3-to-3.8` отработала штатно.

**Параметры кластера:**
- `cluster.id=184ac05d-64e7-4276-ad59-017475bf4f4a` (в env хоста, совпадает с meta.properties 4.3)
- Brokers: 20001 (hc), 21001 (kc), 22001 (pc)
- Controllers: 10001 (hc, leader), 11001 (kc, follower), 12001 (pc, follower)
- Топики 4.3: `test` (3p, retention.ms=-1), `test2` (3p, default retention), `__consumer_offsets` (50p), `__CruiseControlMetrics` (9p)
- Share-group топиков НЕТ (несмотря на share.version=1 feature)
- Consumer group `consumer-group`: test p0=234/p1=397/p2=291, test2 p0=114/p1=0/p2=0

**Эксперимент с retention.ms (главный вывод):**
По просьбе пользователя `test2` создан с `--config retention.ms=604800000` (1 неделя) вместо `-1`. Цель — проверить, сработает ли грабля #18 (retention удаляет `.log` с `largestRecordTimestamp=0` после удаления `.snapshot`).

**Результат: грабля #18 НЕ сработала.** Через 45 сек после старта брокеров `.log` test2-0 = 8397 байт (69 records), test-0 = 13917 байт (115 records). `kafka-dump-log` подтверждает: `CreateTime: 1786274165735` (Aug 9 2026 ~14:16 UTC) — Kafka 3.8 читает реальные timestamp из `.log` записей, `largestRecordTimestamp` восстанавливается корректно без `.snapshot`. Retention видит свежие записи (<1 недели) и не удаляет.

**Why:** На свежем кластере с недавними записями Kafka 3.8 при log recovery читает `CreateTime` из batch records и восстанавливает `largestRecordTimestamp` в `LogSegment`. `.snapshot` (producer state) для этого НЕ нужен — он хранит только producer epoch/batch info. Грабля #18 на `test-downgrade3` сработала, вероятно, из-за других условий (старые данные, обнуление при множественных restartах, или `.log` был повреждён). На test-downgrade5 с свежими данными retention.ms=1 неделя безопасен.

**How to apply:** Для prod-кластеров с недавними данными (записи за последние дни) `retention.ms=604800000` при `--create` может быть безопасной альтернативой `retention.ms=-1`. НО это не гарантирует сохранность на кластерах с давними записями или при повторных рестартах — оставлять `retention.ms=-1` как страховку надёжнее. Не экстраполировать успех test-downgrade5 на prod без бэкапа `log.dirs`.

**Бэкапы:** `~/kafka_4.3_backup/test-downgrade5/` (etap0_state/, controllers/{hc,kc,pc}/, new_topics.txt, rename_stray.sh). Предыдущий бэкап в `test-downgrade5_prev_<timestamp>/`.
