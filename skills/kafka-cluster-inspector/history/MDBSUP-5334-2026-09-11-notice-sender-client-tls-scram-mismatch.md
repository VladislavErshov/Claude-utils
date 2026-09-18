# MDBSUP-5334 — notice-sender: клиенты падают на TLS/SCRAM, кластер здоров (2026-09-11)

Кластер: dsp-notices-msk (dc0c3988-12a6-496d-8015-4be0e8d11a9e), project adtech (id 55), Kafka 4.3,
criticality A. 6 брокеров (1,2 в pc/rc/uc), 3 контроллера, CC в rc.

## Симптомы (из тикета)

«Все соединения к брокерам завершаются SSL handshake failed», продовые сервисы не могут
подключиться, вручную kafkacat тоже. Аналогичный кластер dsp-notices-spb работает штатно.

## Диагностика

1. Прод-БД: кластер и хосты; операции 08–09.09 — modify_cluster (docker 1.0.2→1.0.3),
   delete_hosts ×3, sys_update_cluster, 4× update_database — все `done`. В Temporal
   update_database = activities `updateTopic`/`updateTopicDatabase` — обновления ТОПИКОВ,
   пользователей не трогали.
2. Брокеры active, но нагрузка на rc высокая (load 30–58) — не связано с проблемой.
3. `SSL handshake failed` подтверждён в логе 1.broker.pc (33973 в текущем out.log), но также
   44 × `Authentication failed ... invalid credentials with SASL mechanism SCRAM-SHA-256`.
4. **Обратный резолв клиентских IPv6** (`dig -x` с брокера) — ключевой шаг, фейлящие клиенты:
   - TLS-фейл: dsp-beta.notice-sender.pc/rc, dsp-all-canary.notice-sender.pc (adcld.ru)
   - SCRAM-фейл (TLS проходит!): dsp-all-canary.notice-sender.nc, dsp-123-prod.notice-sender.pc
   Разные симптомы у однотипных инстансов одного сервиса = разъехавшаяся клиентская конфигурация.
5. TLS брокера проверен с нескольких точек (`openssl s_client` с самого брокера и с rc-брокера):
   TLSv1.3, full chain (leaf → one-cloud Infrastructure Vault CA → root), `Verify return code: 0`,
   `verify_hostname` OK. Серт валиден 08.09→07.12.2026. SAN = только свой FQDN (как и на spb).
6. **Таймлайн по ротированным логам** (zgrep по .gz): брокер и серты пересозданы 08.09 16:18
   (рестарт после апдейта образа) → НОЛЬ SSL-ошибок до 10.09 18:20:18, и notice-sender IP-шники
   вообще впервые появились в логах в этот же момент. Кластер создан 11.08 — клиенты мигрировали
   на него 10.09 с изначально неверной конфигурацией (не деградация).
7. Кластер живой: активный сегмент dsp-123-notices-64 вырос на 4.2 МБ за 20 сек
   (`stat` размера файла дважды с паузой) — другие клиенты пишут нормально.

## Вывод

Кластерная сторона без дефектов. У заказчика (notice-sender, adtech) при подключении к новому
кластеру msk: часть инстансов с неверным CA/truststore (TLS-фейл), часть с неверными SCRAM-
кредами (SASL invalid credentials). Ответ в тикете: сверить CA и креды из UI mdb-data.

## Что проверять в похожих кейсах (чек-лист)

- Разделить фейлы по причине: `grep 'Failed authentication' | sed ... | uniq -c` — SSL handshake
  failed vs SASL invalid credentials — это РАЗНЫЕ проблемы (TLS vs креды).
- Резолвить клиентские IPv6 через `dig -x` прямо с брокера — часто это конкретные VM заказчика.
- Проверить таймлайн по ротированным .gz (grep по .gz молча не ищет — только zgrep): «началось
  при рестарте» vs «началось с первого подключения клиента» — принципиально разный вывод.
- Подтвердить живость кластера ростом активного сегмента (два `stat` с паузой) — продакшн идёт.
- Не поверить тикету на слово: «все соединения» и «kafkacat не работает» может означать, что
  kafkacat запускали с той же битой клиентской конфигурацией/с тех же VM.
