# test-downgrade6-mdbdev-kafka — повторный даунгрейд 4.3→3.8 (2026-08-10, run 2)

Кластер `test-downgrade6-mdbdev-kafka` (3 broker + 3 controller + 1 cruise, KRaft). Cruise-хост в процедуре не трогали (по инструкции пользователя). Это повторный даунгрейд — после первого даунгрейда (run 1, 2026-08-10) кластер снова подняли до 4.3, добавили топик test2 (6 partitions), consumer-group2, SCRAM user test-user2.

**Даунгрейд 4.3 → 3.8 (2026-08-10, run 2):** прошёл с одним осложнением — грабля #18 сработала на broker.hc и broker.pc (но НЕ на broker.kc). Бизнес-данные восстановлены копированием .log/.index/.timeindex с broker.kc на broker.hc/pc.

**Why:** Повторный тест процедуры даунгрейда с расширенным набором данных (2 бизнес-топика, 2 consumer-groups, 2 SCRAM users). Подтвердил что грабля #18 может сработать даже на свежем кластере (4 предыдущих кейса были без потери, этот — с потерей на 2 из 3 брокеров).

**How to apply:**
- cluster.id 4.3: `69204f9d-723c-4ad7-848c-efcd1b2389bd` (использован при format 3.8 — совпадает с env KAFKA_CLUSTER_ID, тот же что в run 1)
- node.id: brokers 20001/21001/22001 (hc/kc/pc), controllers 10001/11001/12001 (hc/kc/pc), controller leader = 10001 (hc)
- Бизнес-данные до даунгрейда: test (p0=0, p1=217, p2=201), test2 (p0=0, p1=0, p2=0, p3=205, p4=0, p5=0)
- После даунгрейда + восстановления: все офсеты совпадают с 4.3
- Consumer groups: consumer-group (test: p0=0/0, p1=208/217 lag 9, p2=201/201), consumer-group2 (test2: p3=143/205 lag 62) — полностью совпадают с 4.3
- Топики созданы БЕЗ retention.ms=-1 (по умолчанию). После старта грабля #18 сработала — поставили retention.ms=-1 реактивно на test и test2
- SCRAM users: test-user и test-user2 (оба iterations=4096, Kafka 3.8 игнорирует iterations из CLI), пароль `6VP15UawqWZMXC2N` для обоих
- ACLs: test-user (READ GROUP, READ+WRITE TOPIC), kafka_exporter восстановился автоматически через mdb-data
- Бэкапы: `~/kafka_4.3_backup/etap0_state_downgrade6/` (topics_structure, consumer_groups, users_scram, acls, new_topics.txt, rename_stray.sh), `~/kafka_4.3_backup/controller_downgrade6/{hc,kc,pc}/` (KRaft meta-log), `~/kafka_4.3_backup/kc_recovery/{test-1,test-2,test2-3}/` (.log/.index/.timeindex/leader-epoch-checkpoint с broker.kc для восстановления на hc/pc)

**🚨 Грабля #18 сработала (частично):**
- broker.kc: test-1 (12989B), test-2 (12028B), test2-3 (12207B) — .log целы (исходные 4.3 данные, файл 00000000000000000000.log со starting offset 0)
- broker.hc: те же partition-ы — .log = 0 байт (retention удалил, Kafka создала пустой 00000000000000000217.log / 201.log / 205.log со starting offset = старому log-end-offset)
- broker.pc: те же partition-ы — .log = 0 байт (аналогично hc)
- __consumer_offsets НЕ пострадал (cleanup.policy=compact — retention по времени не работает)
- Фикс: остановили брокеров, удалили пустые .log/.index/.timeindex на hc/pc, скопировали с broker.kc файлы 00000000000000000000.{log,index,timeindex} + leader-epoch-checkpoint, поставили retention.ms=-1, запустили брокеров. После восстановления dump-log показывает records (baseOffset 0,4,6,8...), офсеты совпадают с 4.3.

**Особенности:**
- Первый даунгрейд (run 1) прошёл без грабли #18. Этот run — с граблей на 2 из 3 брокеров. Вывод: грабля #18 недетерминированная, может сработать на любом даунгрейде.
- Cruise-хост НЕ трогали (по инструкции пользователя)
- Подтверждает важность retention.ms=-1 реактивно: сразу после старта брокеров проверять `ls -la /mnt/data/log/<topic>-<N>/*.log` на всех 3 брокерах. Если хотя бы на одном 0 байт — ставить retention.ms=-1 и восстанавливать с брокера где данные целы.
- Восстановление .log с одного брокера на другие работает: Kafka при старте синхронизирует реплики, ISR становится полным (3/3) в течение ~30 сек.
