# MDBSUP-5291 (2026-09-10): Kafka vkvideo-i2i — диски 100%, чистка строго по ретеншену

Кластер: vkvideo-i2i (aeb577a9-64e4-47ff-b17e-35ec4d3092f3), 9 брокеров hc/kc/pc + 3 контроллера + cruise (kc).
Симптом: /mnt/data 100% на всех брокерах, Kafka полностью недоступна (брокер не принимает подключения на 9092).

## Ключевой факт: retention топиков — из прод-БД, таблица `databases`

Когда Kafka лежит, retention.ms недоступен ни через UI (читает из мёртвой кафки), ни через
kafka-metadata-shell (снапшот метаданных пустой — delta без топиков; чтение 84М лог-сегмента
падает OOM без KAFKA_HEAP_OPTS). **Топики и их конфиги лежат в прод-БД backstage_plugin_mdb,
таблица `databases`** (это «наша база» из вики про kafka.sync):

```sql
SELECT name, settings::text FROM databases WHERE cluster_id='<uuid>';
-- settings.kafkaSettings.config.retentionMs / segmentMs / cleanupPolicy / partitions / replicationFactor
```

Для кластера vkvideo-i2i: `vkvideo-i2i-web-features-proto-base64-log-vk` — retentionMs=43200000 (12ч),
96 партиций RF=3; `...-testing` — 3600000 (1ч); служебные (__CruiseControl*, __consumer_offsets) —
config пустой → дефолт брокера 7д.

## Процедура чистки (по дежурной инструкции «В остальных случаях»)

Пер-партиция: сохранить `partition.metadata` → `find <dir> -type f -mmin +<retention_мин> -delete`
→ восстановить partition.metadata → рестарт `kafka-broker`. Ретеншен в минутах: retentionMs/60000.

- Массово — bash-скрипт циклом по `vkvideo-i2i-...-vk-*` (case по `-testing-*` для своего MIN),
  заливка base64 через sshexec, детач `setsid nohup ... & sleep 5` (см. sshexec.md).
  Лог в /tmp/clean.out (per-partition: размер до/после + pm_ok), маркер /tmp/clean.done.
- Удаление ~4ТБ с NVMe заняло ~2-3 мин на хост.
- ⚠️ Если топик без оверрайда — дефолт 7д (10080 мин); резать по 7д можно только когда
  данных старше нет/они истекли, иначе риск удаления живых данных топика с длинным ретеншеном.

## Диагностика возраста данных без кафки

`find <partition-dir> -type f -printf '%TY-%Tm-%Td %p\n' | sort | head/tail` — возраст старейших
лог-сегментов. Здесь старейшие файлы были от 2026-08-28 при retention 12ч → весь диск — истекшие
данные, удаление полностью легитимно.

## Грабли

- `kafka-configs.sh` против лежащего брокера — TimeoutException fetchMetadata, не использовать.
- mcc sshexec: фоновый запуск без `sleep 5` после `&` — процесс умирает, /tmp/clean.out не
  появляется вообще (2 повторённых фейла). Скрипт класть через `echo <b64> | base64 -d > /tmp/x.sh`.
- 1.broker.kc был в state=FINISHED outcome=PREEMPTED (миграция volumes на srvk4578) — sshexec
  молча не отвечает; такие хосты пропускать, возвращаться к ним после пересоздания.
  Итог по этому хосту: volumes дропнуты руками (данные потеряны осознанно — всё истекло по
  ретеншену), хост будет пересоздан с чистыми дисками, чистка на нём не потребовалась.
  После `start` хоста — убедиться, что брокер зарегистрировался (9/9 в CurrentObservers).
- После чистки `systemctl restart kafka-broker` обязателен: брокер active, через ~1 мин все 8
  живых брокеров видны в `kafka-metadata-quorum describe --status` (CurrentObservers), лаг 0.
