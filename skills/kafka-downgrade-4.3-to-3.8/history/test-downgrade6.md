# test-downgrade6-mdbdev-kafka — даунгрейд 4.3→3.8 (2026-08-10)

Кластер `test-downgrade6-mdbdev-kafka` (3 broker + 3 controller + 1 cruise, KRaft). Cruise-хост в процедуре не трогали (по инструкции пользователя).

**Даунгрейд 4.3 → 3.8 (2026-08-10):** прошёл успешно, бизнес-данные сохранены полностью (грабля #18 не сработала). Топики созданы БЕЗ `retention.ms=-1` (по просьбе пользователя) — retention не удалил данные.

**Why:** Тестовый кластер для отработки процедуры даунгрейда. На 4.3 были: 1 бизнес-топик (test, 3 partitions), 1 consumer-group (consumer-group), 1 SCRAM user (test-user) с ACLs.

**How to apply:**
- cluster.id 4.3: `69204f9d-723c-4ad7-848c-efcd1b2389bd` (использован при format 3.8 — совпадает с env KAFKA_CLUSTER_ID)
- node.id: brokers 20001/21001/22001 (hc/kc/pc), controllers 10001/11001/12001 (hc/kc/pc), controller leader = 10001 (hc)
- Бизнес-данные: test (p0=0, p1=217, p2=201) — все сохранены
- Consumer group `consumer-group`: p0=0/0, p1=208/217 (lag 9), p2=201/201 — полностью совпадает с 4.3
- Топики созданы БЕЗ retention.ms=-1 (по просьбе пользователя) — грабля #18 не сработала
- SCRAM user test-user восстановлен с паролем `6VP15UawqWZMXC2N` (тот же что на test-downgrade7). Verify показал iterations=4096 вместо 8192 — Kafka 3.8 игнорирует iterations из CLI и использует default 4096. На аутентификацию не влияет.
- ACLs для test-user (READ/WRITE TOPIC, READ GROUP) и kafka_exporter (DESCRIBE/DESCRIBE_CONFIGS — восстановились автоматически через mdb-data)
- Бэкапы: `~/kafka_4.3_backup/etap0_state_downgrade6/` (topics_structure, consumer_groups, users_scram, acls, new_topics.txt, rename_stray.sh), `~/kafka_4.3_backup/controller_downgrade6/{hc,kc,pc}/` (KRaft meta-log)

**Особенности даунгрейда vs test-downgrade7:**
- Cruise-хост НЕ трогали (по инструкции пользователя) — `cruise-control.service` не запускали, в `host-check` cruise не включали
- Подтверждает паттерн: на свежем кластере retention не удаляет .log с largestRecordTimestamp=0 (4-й кейс подряд без потери данных)
- SCRAM user воссоздан с тем же паролем что на test-downgrade7 — кластеры идентичны по тестовым данным
