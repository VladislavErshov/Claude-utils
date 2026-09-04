# INCALL-42698: Ручное перераспределение партиций на kafka-m2b-adtech-kafka

**Дата**: 2026-07-23
**Кластер**: `kafka-m2b-adtech-kafka`
**Брокеры**: hc=20001-20006, kc=21001-21007 (21007 исключён), pc=22001-22006
**Контроллеры**: 10001 (hc), 11001 (kc), 12001 (pc)
**RF**: 3 (1 реплика на ДЦ)
**Исполнитель**: дежурный + Claude

## Контекст

На кластере `kafka-m2b-adtech-kafka` сложился дисбаланс дисковой нагрузки:
- Перегружены: 1.hc, 2.hc, 5.kc, 1.pc, 2.pc (до 87%+).
- Недогружены: 5.hc (54%), 6.kc (62%), 2.kc (64%), 6.pc (66%), 1.kc (67%), 6.hc (67%).

Два топика требовали ручного перераспределения:
1. `campaign_prices-shrd` (64 партиции) — перекос в kc-ДЦ, 2 брокера (21005, 21006) держали почти весь топик.
2. `dibezium.bannerd.target.campaign` — 5 партиций с аномальным распределением по дискам.

Cruise Control не подходил — нужна была **явная схема** размещения «жирных» (fat, партиции 32-35) и «средних» (medium, партиции 4-7) партиций на конкретных брокерах.

## Часть 1: campaign_prices-shrd

### Схема размещения (только kc-реплика)

| Брокер | Жирные (32-35) | Средние (4-7) | Остальные | Итого |
|---|---|---|---|---|
| 21001 | P32 | P4 | — | 2 |
| 21002 | P33 | P5 | — | 2 |
| 21003 | P34 | P6 | — | 2 |
| 21004 | P35 | P7 | — | 2 |
| 21005 | — | — | P0-3, P8-9, P12-21, P40-45, P48-53 | 28 |
| 21006 | — | — | P10-11, P22-31, P36-39, P46-47, P54-63 | 28 |
| 21007 | — | — | — | 0 (исключён) |

hc (200xx) и pc (220xx) реплики **не трогались** — сохранена cross-DC redundancy.

### Ход работы

1. **Сбор информации** — `kafka-topics.sh --describe --topic campaign_prices-shrd` на `1.broker.kafka-m2b-adtech-kafka.hc.one-infra.ru`. Получены текущие `Replicas:` для всех 64 партиций.
2. **Анализ метрик** — JMX `kafka_log_log_size{partition="N",topic="campaign_prices-shrd",}` на порту 8080 для определения «жирных» партиций (P32-35) и «средних» (P4-7) по размеру лога.
3. **Генерация JSON** — Python-скрипт `/tmp/gen_reassign.py`:
   - `CURRENT_REPLICAS` — словарь partition → текущий список broker IDs.
   - `NEW_KC_REPLICA` — словарь partition → новый kc-брокер.
   - Функция `build_new_replicas(partition, current, new_kc)` заменяет `210xx` в списке на `new_kc`, сохраняя порядок и preferred leader'а.
   - Результат: `/tmp/reassign.json`, 64 партиции, 12 партиций реально перемещено (P0-3, P36-39, P44-47).
4. **Выполнение** — `--execute --throttle 100MB` (104857600 байт/с):
   ```bash
   /opt/kafka/bin/kafka-reassign-partitions.sh \
     --bootstrap-server $cloud_hostname:9092 \
     --command-config /opt/kafka/config/client.properties \
     --reassignment-json-file /tmp/reassign.json \
     --execute --throttle 104857600
   ```
5. **Верификация** — `--verify` каждые ~60 сек до статуса `completed` для всех 64 партиций.
6. **Снятие throttle** — `kafka-configs.sh --alter --entity-type topics --entity-name campaign_prices-shrd --delete-config leader.replication.throttled.replicas,follower.replication.throttled.replicas`.

### Итог

Все 64 партиции завершены. Kc-реплики распределены:
- 21001-21004: по 2 партиции (1 fat + 1 medium).
- 21005, 21006: по 28 партиций (равномерно).
- 21007: 0 (выведен из топика).

## Часть 2: dibezium.bannerd.target.campaign

### Проблема

После первой перераспределки проверили дисковую нагрузку и обнаружили, что 5 партиций `dibezium.bannerd.target.campaign` (P2, P3, P7, P8, P9) создают дисбаланс — их kc-реплики сконцентрированы на перегруженных брокерах.

Дополнительно обнаружено: P4 dibezium.bannerd.target.campaign имеет реплику на **21007** (исключённый брокер) — это не попало в этот reassign, но отмечено как отдельная проблема для последующего разбора.

### Схема перемещения

Только kc-реплика (210xx), hc и pc сохранены:

| Partition | Было (kc) | Стало (kc) | Полный новый список |
|---|---|---|---|
| 2 | 21006 | 21005 | [21005, 22006, 20002] |
| 3 | 21005 | 21006 | [22003, 20003, 21006] |
| 7 | 21006 | 21003 | [20005, 21003, 22006] |
| 8 | 21005 | 21004 | [21004, 22001, 20006] |
| 9 | 21005 | 21002 | [22002, 20003, 21002] |

### Ход работы

1. **Сбор информации** — `kafka-topics.sh --describe --topic dibezium.bannerd.target.campaign`, распарсены текущие `Replicas:` для 5 проблемных партиций.
2. **Генерация JSON** — вручную составлен `/tmp/reassign_balance.json` (5 партиций, kc-реплика заменена на менее загруженный брокер).
3. **Загрузка на брокер** — через base64-через-ssh (mcc scp нестабилен):
   ```bash
   cat /tmp/reassign_balance.json | base64 | tr -d '\n' > /tmp/reassign_balance.json.b64
   # через expect: send "echo '<b64>' | base64 -d > /tmp/reassign_balance.json\r"
   ```
4. **Выполнение** — `--execute` без `--throttle` (партиции небольшие, скорость не критична).
5. **Верификация** — `--verify`:
   - Первый запрос (через ~30 сек): 3/5 completed, campaign-2 и campaign-9 in progress.
   - Второй запрос (через ~60 сек): все 5 completed.
6. **Throttle** — не был установлен (`--execute` без `--throttle`), чистить нечего.

### Итог

Все 5 партиций перемещены. Kc-реплики распределены на менее загруженные брокеры (21002, 21003, 21004, 21005, 21006).

## Нюансы и ошибки

### Expect prompt regex

Изначально пробовали `\[#?\$\]` — не сработало. Промпт на брокере выглядит как `/# `, рабочий regex: `/#\s*` (или просто строка `/# `).

### kafka-log-dirs `--topic` vs `--topic-list`

В Kafka 3.8 флаг `--topic` не распознан в `kafka-log-dirs.sh`. Нужно использовать `--topic-list`.

### mcc scp нестабилен

`mcc scp` падает с «Cannot open tunnel... SSL Handshake is not finished». Обход — base64 через ssh:
```bash
# локально
base64 < /tmp/reassign.json | tr -d '\n' > /tmp/reassign.json.b64
B64=$(cat /tmp/reassign.json.b64)
# через expect
send "echo '$B64' | base64 -d > /tmp/reassign.json\r"
```

### ANSI-коды ломают grep

`grep "completed"` подсвечивает совпадение ANSI-кодами `[01;31m[K`, что ломает парсинг. Фильтровать:
```bash
sed -E 's/\x1b\[[0-9;]*[mK]//g'
```
Просто `[m` без `K` не помогает — нужно убирать оба.

### Topic name typo

Пользователь написал `campaign_prices_shrd` (с подчёркиванием), реальное имя `campaign_prices-shrd` (дефис). Проверять реальное имя через `kafka-topics.sh --list | grep campaign_prices`.

### Сложный shell quoting

Вложенные кавычки в `send` ломают expect-парсер. Решения:
- Упрощать команды.
- Использовать heredoc на брокере: `cat > /tmp/x.sh << "EOF" ... EOF`, затем `bash /tmp/x.sh`.

## Файлы

- `/tmp/gen_reassign.py` — Python-скрипт-генератор для campaign_prices-shrd.
- `/tmp/reassign.json` — финальный reassign JSON для campaign_prices-shrd (64 партиции).
- `/tmp/reassign_balance.json` — финальный reassign JSON для dibezium.bannerd.target.campaign (5 партиций).

## Уроки

1. **Перед reassign собери полное текущее состояние** — `kafka-topics.sh --describe` для всех затрагиваемых партиций. Без этого легко сломать preferred leader или потерять реплику.
2. **Меняй только одну DC-реплику за раз** — сохраняешь cross-DC redundancy (1 реплика на ДЦ).
3. **Проверяй JMX-метрику `kafka_log_log_size`** — чтобы понять, какие партиции «жирные», и не сместить их все на один брокер.
4. **Используй throttle для больших топиков** — 100 МБ/с обычно достаточно, чтобы не положить сеть.
5. **После `--execute` без `--throttle` чистить нечего** — throttle-конфиг на топик не ставится.
6. **Verify до статуса completed** — периодичность ~60 сек для небольших партиций, ~5 мин для жирных.
