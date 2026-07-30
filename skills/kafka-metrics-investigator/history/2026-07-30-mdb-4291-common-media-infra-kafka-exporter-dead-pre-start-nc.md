---
date: 2026-07-30
ticket: MDBSUP-4291
cluster: common-media-infra-kafka
hosts:
  - 1.broker.common-media-infra-kafka.hc.one-infra.ru
  - 1.broker.common-media-infra-kafka.kc.one-infra.ru
  - 1.broker.common-media-infra-kafka.pc.one-infra.ru
kafka_version: 3.8.0
resolution: resolved 2026-07-30
---

# kafka-exporter (23569) мёртв 2.5 месяца — pre-start виснет на `nc -z $cloud_hostname 9092`

## Симптомы

На broker-хосте `1.broker.common-media-infra-kafka.hc.one-infra.ru` (Kafka 3.8.0, KRaft)
порт **23569** (kafka-exporter) не отвечает. В Grafana отсутствуют `kafka_consumergroup_lag`,
`kafka_consumergroup_current_offset`, `kafka_topic_partition_current_offset`.

## Порты на хосте

| Порт | Сервис | Статус |
|---|---|---|
| 8080 JMX exporter | kafka-broker (javaagent) | ✅ 200 |
| 7777 Jolokia | kafka-broker (javaagent) | ✅ 404 на /metrics — норма |
| 23569 kafka-exporter | systemd `kafka-exporter.service` | ❌ `failed (Result: timeout)` с 2026-05-16 |
| 23570 share-group-lag-exporter | — | ⚪ `Unit ... could not be found` (Kafka 3.8, share-groups только в 4.3+ — норма) |

`systemctl is-active`:
```
kafka-broker             active
kafka-exporter           failed
share-group-lag-exporter inactive (unit not found)
```

## Корневая причина

`kafka-exporter.service` упал 2026-05-16 21:38:06 MSK по `TimeoutStartSec` и с тех пор не
поднимался (systemd не рестартит `failed` без `Restart=always`).

`/mnt/logs/dbms/kafka-exporter.out.log` заполнен строками:
```
Waiting for broker for 5 seconds...
Waiting for broker for 5 seconds...
... (последняя запись 2026-05-16)
```

Это вывод `pre-start-kafka-exporter.sh`:
```bash
while ! nc -z $cloud_hostname 9092 > /dev/null 2>&1; do
  echo "Waiting for broker for 5 seconds..."
  sleep 5
done
```

Pre-start не смог подключиться к 9092 за `TimeoutStartSec` → systemd убил юнит → `failed`.

**Важно:** это НЕ классический кейс из `check_metrics.md` (виснущий `kafka-acls.sh --list`).
Там pre-start тоже виснет, но на этапе ACL. Здесь — на более раннем шаге `nc -z 9092`.

## Почему pre-start не видел 9092

На момент проверки (2026-07-30) брокер жив: JMX 8080 и Jolokia 7777 отвечают, юнит
`kafka-broker.service` active. Значит в 2026-05-16 была временная недоступность 9092
(рестарт брокера, OOM, network blip) — pre-start попал в окно, когда 9092 не слушался,
цикл бесконечно крутился до таймаута. После восстановления брокера systemd юнит уже не
поднялся сам — `Restart=` в unit-файле нет.

## Применённый фикс (2026-07-30)

На всех трёх broker-хостах кластера выполнено:
```bash
mcc --local sshexec -n infra <host> 'systemctl restart kafka-exporter.service'
```

Результат:
| Хост | systemctl | Порт 23569 |
|---|---|---|
| `1.broker.common-media-infra-kafka.hc.one-infra.ru` | active | 200 |
| `1.broker.common-media-infra-kafka.kc.one-infra.ru` | active | 200 |
| `1.broker.common-media-infra-kafka.pc.one-infra.ru` | active | 200 (первая попытка упала на `SSL Handshake is not finished` — повтор через `sleep 3` прошёл) |

## Фикс (шаблон)

```bash
mcc --local sshexec -n infra 1.broker.common-media-infra-kafka.hc.one-infra.ru \
  'systemctl restart kafka-exporter.service'
```

Брокер сейчас жив на 9092 — pre-start должен пройти за секунды. После рестарта проверить:
```bash
curl -s -o /dev/null -w "%{http_code}\n" --max-time 10 localhost:23569/metrics  # ожидается 200
systemctl is-active kafka-exporter                                                # ожидается active
```

## Как отличить от классического кейса (kafka-acls.sh --list hang)

| Признак | Этот кейс (MDBSUP-4291) | Классический (`check_metrics.md`) |
|---|---|---|
| `out.log` последние строки | `Waiting for broker for 5 seconds...` | (нет — pre-start уже прошёл nc, висит на ACL) |
| `systemctl status` CGroup | только pre-start скрипт | виден процесс `kafka-acls.sh --list --principal User:kafka_exporter` |
| Фикс | `systemctl restart kafka-exporter` | `systemctl restart kafka-broker` затем `kafka-exporter` |

## Что НЕ покрывает

- Почему 9092 был недоступен 2026-05-16 — журнал `kafka-broker.err.log`/`.out.log` за ту
  дату не поднимался (вне scope проверки метрик). См. скилл `kafka-log-investigator`.
- Почему на юните нет `Restart=always` — это вопрос docker-образа / spec-конфига, не метрик.
