# MDBSUP-5515 — 2026-09-17 — сброс оффсетов consumer group на огромном кластере (UI Gateway Timeout)

- **Кластер:** `kafka-tophits` (mytracker), `0516395f-bdf6-46a7-a8f1-0f70c23e2fab` — 67 брокеров × 3 ДЦ (hc/kc/pc) + 3 контроллера, KRaft 3.8
- **Запрос:** сброс оффсетов группы `dzen-visits` до latest; UI mdb при открытии consumer group отдаёт Gateway Timeout — кластер слишком большой
- **Итог:** dry-run + execute `kafka-consumer-groups.sh --reset-offsets --to-latest` с брокера при Empty-группе; 360/360 партиций; тикет закрыт

## Канон ручного сброса

1. Хосты кластера — из прод-БД `one_cloud_meta`/`host_state` (`queue=kafka-tophits-mytracker-kafka`, domain one-infra): `N.broker.kafka-tophits-mytracker-kafka.{hc,kc,pc}.one-infra.ru`.
2. Состояние группы с любого брокера:
   `/opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server <fqdn>:9092 --command-config /opt/kafka/config/client.properties --describe --group <g> --state`
   Здесь: Stable, 50 членов, coordinator `33.broker...pc` (id 22033). Reset при живых членах невозможен — запросить у пользователя остановку инстансов и дождаться `Empty, 0 members`.
3. Топики группы — из `--describe` (без `--state`): один топик `tophits`, 360 партиций.
4. Dry-run → execute:
   ```
   ... --reset-offsets --group dzen-visits --topic tophits --to-latest --dry-run
   ... --reset-offsets --group dzen-visits --topic tophits --to-latest --execute
   ```
5. Верификация: `--describe` — committed = LEO на всех партициях.

## Грабли

- **Auth-файл для CLI:** `/opt/kafka/config/client.properties` (`security.protocol=SASL_SSL`, `sasl.mechanism=PLAIN`). Файл `/etc/kafka/kafka-console-consumer.properties` на этом образе **отсутствует** (есть только `/etc/kafka/get-user-info.sh`).
- **Bootstrap — FQDN брокера :9092**, не localhost (SAN сертификата).
- **`--all-topics` на большом кластере не использовать** — создаст commits по всем партициям всех топиков кластера; scope — `--topic <имя>`.
- **Вывод на сотни партиций склеивается в одну строку** (таблица без переводов строк) — `grep -c topic` врёт; парсить после `tr '\r' '\n'`.
- **Остаточный lag сразу после reset — норма:** это приток новых сообщений между reset и проверкой (здесь ~5.4M при LEO ~21.7 млрд, продюсеры продолжали писать). Не «недосброс».
- UI self-service reset на больших кластерах таймаутит (Gateway Timeout) — отдельный баг, заведут позже (слово пользователя).
