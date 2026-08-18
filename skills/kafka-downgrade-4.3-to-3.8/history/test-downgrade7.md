# test-downgrade7-mdbdev-kafka — даунгрейд 4.3→3.8 (2026-08-10)

Кластер `test-downgrade7-mdbdev-kafka` (3 broker + 3 controller + 1 cruise, KRaft).

**Даунгрейд 4.3 → 3.8 (2026-08-10):** прошёл успешно, бизнес-данные сохранены полностью (грабля #18 не сработала).

**Why:** Тестовый кластер для отработки процедуры даунгрейда. На 4.3 были: 3 бизнес-топика (test, test2, test3 по 3 partitions), 2 consumer-groups (consumer-group, consumer-group2), 1 SCRAM user (test-user) с ACLs.

**How to apply:**
- cluster.id 4.3: `23f108ac-1907-434e-a67b-dda01df316f4` (использован при format 3.8 — совпадает с env KAFKA_CLUSTER_ID)
- node.id: brokers 20001/21001/22001 (hc/kc/pc), controllers 10001/11001/12001 (hc/kc/pc), controller leader = 10001 (hc)
- Бизнес-данные: test (p0=30, p1=0, p2=276), test2 (p0=0, p1=117, p2=0), test3 (p0=0, p1=0, p2=68) — все сохранены
- Топики созданы БЕЗ retention.ms=-1 (по просьбе пользователя) — retention не удалил данные, грабля #18 не сработала на свежем кластере
- SCRAM user test-user восстановлен с паролем (не с salt/key — в 3.8 kafka-configs не принимает salt/stored_key/server_key, только password). Пароль: `6VP15UawqWZMXC2N`
- ACLs для test-user (READ on TOPIC/GROUP, WRITE on TOPIC) и kafka_exporter (DESCRIBE/DESCRIBE_CONFIGS — восстановились автоматически через mdb-data)
- Бэкапы: `~/kafka_4.3_backup/etap0_state/` (topics_structure, consumer_groups, users_acl_dump, restore_users.sh, new_topics.txt, rename_stray.sh), `~/kafka_4.3_backup/controller-{hc,kc,pc}/` (KRaft meta-log)

**Особенности даунгрейда vs test-downgrade5:**
- Впервые сохранены SCRAM users и ACLs (новый Stage 8/9 в скилле)
- Грабля #18 не сработала (как и на test-downgrade5 run 2) — подтверждает паттерн: на свежем кластере retention не удаляет .log с largestRecordTimestamp=0
- host-check.service на cruise-хосте падал с HTTP 500 пока CC прогревался — это нормально, не требует глубокого разбора
