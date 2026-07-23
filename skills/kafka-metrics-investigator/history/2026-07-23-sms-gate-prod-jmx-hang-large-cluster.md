---
date: 2026-07-23
cluster: sms-gate-prod-vkcm-kafka
hosts:
  - 1.broker.sms-gate-prod-vkcm-kafka.hc.one-infra.ru
  - 1.broker.sms-gate-prod-vkcm-kafka.pc.one-infra.ru
  - 1.broker.sms-gate-prod-vkcm-kafka.rc.one-infra.ru
comparison_host: 1.broker.test-cruise5-mdbdev-kafka.ic.one-infra.ru
kafka_version: 3.8.0
jmx_exporter: jmx_prometheus_javaagent-0.19.0.jar
resolution: open
---

# JMX exporter виснет на большом кластере (65k partitions)

## Симптомы

3 брокера в статусе `AVAILABLE`, нагрузка штатная (CPU 11-13%, RAM 27-28%).
В Grafana пропала часть метрик — все `kafka_server_*`, `kafka_controller_*`,
`kafka_log_*`, `kafka_network_*`, `jvm_*`. Остались только `kafka_consumergroup_lag`
и `kafka_topic_partition_*` от kafka-exporter.

## Порты на хостах

| Порт | Сервис | Статус |
|---|---|---|
| 8080 JMX exporter | kafka-broker (javaagent) | ❌ висит, scrape не завершается за 180s, size=0 |
| 7777 Jolokia | kafka-broker (javaagent) | ✅ жив, 404 на /metrics — норма |
| 23569 kafka-exporter | systemd | ✅ жив, отдаёт 16.8MB за 12-18s |
| 23570 share-group-lag-exporter | — | ❌ `Unit ... could not be found` (не установлен) |

Порт 8080 при этом слушается java-процессом, javaagent загружен в `KAFKA_OPTS`:
`-javaagent:/opt/prometheus/jars/jmx_prometheus_javaagent-0.19.0.jar=8080:/opt/prometheus/kafka-broker.yml`.

## Диагностика

### Thread dump подтверждает зависание

JMX exporter застрял в `JmxScraper.doScrape → scrapeBean → processBeanValue → recordBean`
(`io.prometheus.jmx.JmxCollector.collect → HTTPServer.handle`). Обход MBean'ов не
завершается.

### GC и heap в норме

Паузы 16-20ms (G1), heap 4.5/12GB. Load average 35 — это CPU от самого JMX scraper'а,
не от Kafka. JVM не виновата.

### MBean'ы в JVM есть — их 230k

Через Jolokia `/jolokia/list` читаются корректно. Важно: `jolokia/list/kafka` возвращает
только 1 MBean (Log4jController) — нужно дёргать `/list` без пути и считать домены.
Аккуратные числа:

| Домен | MBean'ов (bad) | MBean'ов (good) |
|---|---|---|
| `kafka.cluster` | 129 324 | 0 |
| `kafka.log` | 86 232 | 240 |
| `kafka.server` | 14 614 | 133 |
| `kafka.network` | 570 | 573 |
| `kafka.coordinator.*` | 9 | 9 |
| `kafka` (Log4jController) | 1 | 1 |
| **TOTAL** | **~230 747** | **967** |

Партиций на bad-кластере ~65 000, на good-кластере 7 757. Разница ~240× по MBean'ам.

### Конфиг идентичен

`/opt/prometheus/kafka-broker.yml` на проблемном и рабочем брокерах —
одинаковый (199 строк, diff пустой). Обновление образа применено, конфиг не менялся.

В конфиге есть **закомментированные** blacklist-строки:
```yaml
# "kafka.cluster:type=*, name=*, topic=*, partition=*"
# "kafka.log:type=*,name=*, topic=*, partition=*"
```
Эти два домена дают 215k из 230k MBean'ов (129k + 86k). В `rules:` есть pattern'ы
`topic=*, partition=*` — exporter создаёт метрику для каждого partition.

## Корневая причина

`jmx_prometheus_javaagent` синхронно обходит все MBean'ы при каждом scrape.
На проде 65k partitions × несколько partition-level метрик в `kafka.log` и
`kafka.cluster` = ~215k MBean'ов. Exporter не успевает обойти 230k MBean'ов за
разумное время → scrape зависает → Prometheus получает timeout → метрик в Grafana нет.

На test-cruise5 partitions 7.7k, MBean'ов <1k — exporter справляется за 0.85s
(http=200, size=425KB).

Оценка времени scrape при линейном масштабировании: 0.85s × (230747/967) ≈ 200s.
То есть scrape потенциально завершается за ~3-4 минуты, но Prometheus к этому
моменту уже закрыл соединение по timeout.

## Что НЕ помогло

- Обновление docker-образа — конфиг `kafka-broker.yml` не изменился, проблема та же.
- Проверка GC/heap — всё в норме, JVM не виновата.

## Что НЕ работает на принимающей стороне (Prometheus)

- `metric_relabel_configs` / `sample_limit` — применяются к уже полученным метрикам.
  Если exporter не отдал — применять не к чему.
- Grafana — рисует то, что лежит в Prometheus, там ничего сделать нельзя.

## Варианты фикса

### 1. Мьютинг MBean'ов (на источнике, правильный фикс)

Раскомментировать в `/opt/prometheus/kafka-broker.yml`:
```yaml
blacklistObjectNames:
  - "kafka.cluster:type=*, name=*, topic=*, partition=*"
  - "kafka.log:type=*,name=*, topic=*, partition=*"
```
Уберёт 215k из 230k MBean'ов. Все partition-level метрики `kafka.log`/`kafka.cluster`
пропадут из Grafana, но broker-level (`kafka_server_*`, `jvm_*`, `kafka_controller_*`,
`kafka_network_*`) останутся. На dev-кластере (test-cruise5) заблэклистит 0 MBean'ов —
там `kafka.cluster` пустой, `kafka.log` = 240.

Применить в шаблоне confp в репо docker-images, чтобы переживал redeploy.

### 2. Увеличить scrape_timeout в Prometheus (костыль)

Поставить `scrape_timeout: 300s`, `scrape_interval: 5m`. Может сработать (оценка
scrape ~200s), но плохая практика — долгие соединения, устаревшие метрики.

### 3. Прокси перед JMX

vmagent или кастомный scraper, который ходит на Jolokia (7777, жив) и берёт только
нужные MBean'ы точечно. Prometheus scrape'ит уже прокси. Меняет источник, деплоится
на стороне мониторинга.

### 4. Замена exporter'а

Поставить exporter, который не делает полный sync-scrape (jolokia-exporter или
свой скрипт через Jolokia HTTP API). Это уже разработка.

## Дополнительная находка

`share-group-lag-exporter.service` отсутствует как юнит на всех 3 хостах
(`Unit ... could not be found`). Не "упал" — не установлен. Метрики
`kafka_share_group_*` в Grafana отсутствуют всегда, пока юнит не разворачивается
в OneCloud spec / docker-образе.

## Команды для повторения диагностики

### Быстрая проверка 4 портов

```bash
expect -c '
set timeout 90
spawn mcc --local ssh <host>
expect "/# "
send "for p in 8080 7777 23569 23570; do printf \"port %s: \" \$p; curl -s -o /dev/null -w \"%{http_code}\\n\" --max-time 10 localhost:\$p/metrics || echo fail; done; echo ===DONE===\r"
expect "===DONE==="
send "exit\r"
expect eof
' 2>&1 | tail -10
```

Важно: `--max-time 10` на JMX 8080 даст `000` даже если exporter живёт —
он отдаёт ~425KB, при 230k MBean'ов scrape занимает минуты. Для диагностики
ставить `--max-time 180` минимум, лучше `--max-time 600`.

### Подсчёт MBean'ов через Jolokia

Jolokia `/list` возвращает домены как top-level keys (не `domain:props`, а
`{domain: {props: ...}}`). `/list/kafka` вернёт только корневой домен `kafka`
(1 MBean — Log4jController). Нужен полный `/list` и подсчёт по доменам:

```bash
# скрипт на хосте ( через base64 чтобы обойти expect-кавычки )
curl -s --max-time 60 "localhost:7777/jolokia/list" -o /var/tmp/full.json
python3 << 'PYEOF'
import json
d = json.load(open("/var/tmp/full.json"))
v = d.get("value", {})
for domain in sorted(v.keys()):
    if domain.startswith("kafka"):
        mbeans = v[domain]
        n = len(mbeans) if isinstance(mbeans, dict) else 0
        print(f"  {n:6d}  {domain}")
PYEOF
rm -f /var/tmp/full.json
```

⚠️ Не писать в `/tmp` — на хосте tmpfs 64MB, 65MB JSON переполнит.
Писать в `/var/tmp` или `/mnt/logs`.

### Сравнение конфигов между хостами

```bash
# на локальной машине
expect -c '...' > /tmp/cfg_a.txt   # mcc ssh host_a + cat /opt/prometheus/kafka-broker.yml
expect -c '...' > /tmp/cfg_b.txt   # mcc ssh host_b + cat ...
diff /tmp/cfg_a.txt /tmp/cfg_b.txt
```

## Ключевые уроки

1. **JMX exporter линейно масштабируется по MBean'ам.** При >100k MBean'ов scrape
   начинает не укладываться в стандартные Prometheus timeout'ы. На 230k — виснет
   намертво.

2. **`kafka.cluster` и `kafka.log` partition-метрики — главные виновники.** Каждый
   partition даёт ~2-3 MBean в каждом домене. При 65k partitions × 2 домена × ~2 метрики
   = ~260k MBean'ов. Это и есть причина.

3. **Конфиг идентичен между кластерами.** Разница только в размере. Не пытаться
   чинить конфигом на одном хосте — фикс должен быть в шаблоне образа.

4. **Jolokia `/list/<domain>` возвращает только точный домен.** `kafka.server`,
   `kafka.log`, `kafka.cluster` — это **отдельные домены** (с точкой в имени), а не
   поддомены `kafka`. Для подсчёта MBean'ов нужен полный `/list` + группировка.

5. **`/tmp` на брокере — tmpfs 64MB.** Jolokia `/list` возвращает 65-94MB JSON.
   Писать в `/var/tmp` или `/mnt/logs`.

6. **Обновление образа не фиксит**, если конфиг `kafka-broker.yml` в образе не
   изменился. Проверять через `diff` с рабочим хостом, а не через факт обновления.
