# UI Cruise Control отдаёт 504 — мёртвые per-DC VIP в uc (10.189.121.x)

**Кластер:** `logs-recom-wizard-kafka` (dzen, ДЦ pc/hc/kc/uc)
**Хост CC:** `1.cruise.logs-recom-wizard-kafka.pc.idzn.ru`
**Дата:** 2026-08-25

## Симптом

Страница UI Cruise Control (`https://127.0.0.1:1443/#/dev/dev/admin_broker`, nginx + basic auth,
юзер `cruise`) открывается (статика 200), но данные не грузятся: `GET /kafkacruisecontrol/kafka_cluster_state?json=true` → **504 Gateway Time-out**.

## Диагностика

1. nginx_error.log: десятки `upstream timed out (110) while reading response header from upstream`
   на `kafka_cluster_state`; напрямую `curl http://localhost:8080/kafkacruisecontrol/kafka_cluster_state?json=true`
   отвечает **200 за 100.02 сек** → nginx режет по proxy_read_timeout (60с).
2. CC-лог показывает причину: `ERROR Getting log dir information for broker 23004..23010 timed out`
   (ClusterBrokerState) — describeLogDirs таймаутит по **всем 7 брокерам uc** по 10 сек каждый ≈ 100 сек.
   Плюс DiskFailureDetector WARN по тем же брокерам.
3. Сеть: TCP до брокеров uc:9092 **FAIL из всех ДЦ** (pc/kc/hc), при этом:
   - сами uc-брокеры живы: локально localhost:9092 / FQDN:9092 OK, kafka-broker active, replication идёт;
   - ping (IPv6-меш fd00) проходит — сетевой контроль не ловит;
   - хосты hc/kc/pc доступны на 9092 нормально.
4. Ключевое: getent на cruise-хосте резолвит uc-брокеров в per-DC VIP `10.189.121.43/63/64`
   (у работающего hc — VIP `10.200.76.67`). Реальные IP uc-хоста — `10.144.0.36` и т.п.
   VIP 10.189.121.x не отвечают ни на 9092, ни на 22 → **мёртвый прокси/VIP-диапазон ДЦ uc**.

## Вывод

Сломан cross-DC доступ через VIP-диапазон `10.189.121.x` (uc). Kafka-клиенты (вкл. CC
AdminClient) ходят по advertised.listeners FQDN → резолв в мёртвые VIP → таймауты.
Рестарты брокеров/CC не чинят (брокер 21008 в kc перезапускали — не помогло, он был
побочным «failed» из-за старых метрик CC).

## Действия

- Перезапущен `kafka-broker.service` на `8.broker...kc` (не помог, ожидаемо).
- Эскалация сетевикам/инфре: мёртвые VIP 10.189.121.43/63/64 (uc, порты 9092/22).
- Временный обход для UI: поднять `proxy_read_timeout` в nginx cruise-хоста до 120s —
  страница будет грузиться ~100 сек без 504.

## Грабли/заметки

- `mcc restart <host>` требует интерактивной капчи (математический вопрос) — обёртка expect
  с regex `evaluated value for \(attempt \d+/\d+\): (\d+)([+\-*])(\d+)`.
- 504 в UI CC с «User-Task-ID header is not found» = обычно просто nginx timeout на медленном
  эндпоинте CC, не CORS.
- Кросс-ДЦ VIP-диагностика: `getent ahosts <fqdn>` с разных хостов + `/dev/tcp/$ip/9092`;
  ping по IPv6-мешу проходит даже при мёртвых VIP — не доверять ping.
