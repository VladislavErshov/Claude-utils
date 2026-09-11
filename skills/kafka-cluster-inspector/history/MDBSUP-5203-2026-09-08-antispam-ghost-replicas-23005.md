# MDBSUP-5203 — 39 URP: призрачные реплики удалённого ДЦ hc + клин нового соединений на 23005

**Кластер:** `extdbprsoc-oneme-kafka` (oneme, one-infra, ДЦ pc/kc/ec + до августа hc)
**Дата:** 2026-09-08
**Тикет:** https://jira.vk.team/browse/MDBSUP-5203
**Кластер ID:** `cb1e5643-37c4-46d8-8515-a247909a81c4` (150 брокеров: pc=22001-22050, kc=21001-21050, ec=23001-23050; cruise на pc)

## Симптом

- UI mdb-data: «39 нереплицированных партиций», хосты живые.
- Graylog: ошибки продюсеров `KafkaProducerImpl Can't send message for` (DEVOPS_MDB).

## Диагноз

1. Прод-БД: операций зависших нет (последние delete_hosts 12-14.08 done, update_user 27.08 done).
2. Jolokia-свип `UnderReplicatedPartitions` по всем 150 брокерам (параллельный xargs) — сумма 39, у каждого брокера 1-2 → не «один мёртвый брокер».
3. ⚠️ **`kafka-topics --describe --under-replicated-partitions` врал** (пусто): на этих образах нет `kafka-topics` в PATH и `/etc/kafka/kafka-console-consumer.properties`; `/opt/kafka/config/client.properties` **не содержит `security.protocol`** (listener INTERNAL = SASL_SSL) → AdminClient молча не достаёт метаданные. Рабочий конфиг собирается из секретов broker.properties:
   `security.protocol=SASL_SSL` + jaas из client.properties + `ssl.truststore.location=/opt/kafka/ssl/client.truststore.jks` + пароль truststore из broker.properties.
4. Правдивый источник по-партионного URP — **kafka-exporter :23569** `kafka_topic_partition_under_replicated_partition`.
5. Инвентаризация: 18 партиций `oneme_antispam_pr_TopAudit` с призраками 20001-20018 (реплики удалённого ДЦ hc) — ISR навсегда 2/3, unregistered брокер не входит в ISR; остальное — плавающие 1-2 URP на devops_login_* (хронический follower lag). «200xx» в devops — только мусор в старых `throttled.replicas` конфигах, не реплики.
6. Откуда призраки: delete_hosts 12-14.08 (автор a.perevalov) удалял ДЦ hc (UnregisterBrokerRecord 20048/20049/20050 в контроллер-логе), но reassign при удалении пропустил antispam → узнаваемый паттерн «удалённый брокер остался в Replicas» (см. MDBSUP-4166), только без offline — ISR 2/3.
7. Бонус-находка: `5.broker.extdbprsoc-oneme-kafka.ec.one-infra.ru` (23005) при живом процессе (BrokerState=3) **не принимал новые TCP ниоткуда, включая localhost:9092** (SYN висит, rc=124, не refused) — поэтому `kafka-reassign-partitions` падал `incrementalAlterConfigs timeout`, и это же давало часть URP-волн/ошибок продюсеров. TCP-свип с двух точек по всем 150: единственный недоступный. Пользователь отправил хост в миграцию — после неё 23005 зарегистрирован и принимает соединения.

## Фикс

- Reassign 18 партиций: призрак 200xx заменяется живым брокером из **недостающего** ДЦ (инвариант 1 реплика/ДЦ kc/pc/ec), порядок реплик сохранён (лидер не трогали), round-robin по 50 брокерам ДЦ. JSON собран локально (`/tmp/build_reassign.py` — парсер describe-вывода), залит base64.
- Грабля №1: `--execute --throttle` падал на `modifyInterBrokerThrottle` (timeout `incrementalAlterConfigs`), но throttle-конфиги успели поставиться (broker rate 100МБ/с + topic throttled.replicas с новыми целями). Решение: **повторный `--execute` без `--throttle`** → reassignment подался сразу.
- Перелив мелких партиций занял минуты; `--verify` — все completed, verify же снял throttle-конфиги (topic throttled.replicas и broker rates → 0).
- Итог: GHOSTS_LEFT=0, ISR 3/3 по всем 24 партициям, глобальный URP 39 → ~13-22.

## Остаточное (не блокер тикета)

- Плавающие URP только `oneme_devops_login_*` — replication lag под продюсерской нагрузкой (паттерн maildwh1, лечилось `num.replica.fetchers=6`) — вариант CC-ребаланса.
- Мусорные `throttled.replicas` от прошлых операций на devops_login_* (например `13:20130`) — косметика.
- Грабля №2: в Kafka 4.x TopicCommand нет `--partition`; describe-вывод строк начинается с таба (grep `^Topic:` не работает).
- Грабля №3: вывод mcc sshexec иногда глотает строки — надёжнее скриптовый файл + маркеры-echo.
