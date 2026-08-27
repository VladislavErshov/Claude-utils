# Разбор проблем Kafka

**Канон — Confluence «Дежурство MDB: Kafka», секции «Проблемы» и «Ошибки, способы
проверки и решения»** (SSOT): https://confluence.vk.team/pages/viewpage.action?pageId=1348619075

Покрывает: создание топика (расчёт партиций), не могу подключиться/читать, сэмпл сообщений,
перевод на SASL_PLAINTEXT, PLAIN-пользователь, место на брокерах (включая полный скрипт
`clean_up.sh` для `-stray` партиций) и в логах, Connection timed out, зависшая таска
создания пользователя, обновление версии, перераспределение партиций, переезд rc→hc,
STARTING RESERVED, io/network треды, ребалансировка consumer group + JoinGroup, under
min.isr, застряло удаление брокера, новый listener, брокер лежит. Вики живая — править там.

Каталог известных технических багов (Broker is dead, InvalidReplicationFactor, FencedBroker,
CruiseControlMetricsReporter) — `known_issues.md`. Базовые паттерны доступа к хостам и
грабли Tcl/SSL — скилл [`mcc-host-worker`](../../mcc-host-worker/SKILL.md).

## Наши дополнения к вики

### Очистка `-stray` партиций без clean_up.sh (mcc-перебор)

Перебрать хосты × ДЦ через [`mcc-host-worker`](../../mcc-host-worker/SKILL.md) (команда
`ssh`). Шаблон хоста: `$i.broker.<cluster>.<dc>.one-infra.ru` (`i=1..75`, `dc=hc,kc,pc`).
На каждом хосте:

```bash
cd /mnt/data/log
du -sk *-stray 2>/dev/null | awk '{s+=$1} END {print s+0}'
rm -rf *-stray
```

Сделать рестарт хостов, на которых была ошибка (иначе память может долго не обновляться).
Полный скрипт с expect-раннером и параллелизмом — в вики, секция «Кончилось место на
брокерах».

### Кончилось место в логах

Зайти на хост через [`mcc-host-worker`](../../mcc-host-worker/SKILL.md). Сначала чистим
логи, перезапускаем кафку — с забитым диском логов кафка может не подняться на новом образе.
