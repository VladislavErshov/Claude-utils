# 2026-09-07 — kafka-seo-seo-kafka (rc, dzen): плановая проверка cruise-control — всё ок

Кластер `kafka-seo-seo-kafka` (dzen, домен idzn.ru), cruise-хост
`1.cruise.kafka-seo-seo-kafka.rc.idzn.ru`. Триггер — запрос «проверь что с круизом всё ок».
Инцидента не было, запись для истории проверок.

## Результаты проверки (все Healthy)

- **Сервис**: `cruise-control.service` active (running), аптайм с 26.08 17:57 (~12 дней).
- **REST API**: отвечает на `:8080` (`/kafkacruisecontrol/state`). На 443 — nginx.
  ⚠️ На этом кластере CC REST **не на 9090** (порт не слушается) — проверять по `ss -tlnp`,
  а не вслепую curl-ить 9090.
- **MonitorState**: RUNNING (20% trained), 5/5 валидных окон (100%),
  545/545 партиций (100%), flawed = 0.
- **ExecutorState**: NO_TASK_IN_PROGRESS; `recentlyRemovedBrokers: [21001]` — хвост
  прошлого ресайза, задач нет.
- **AnalyzerState**: isProposalReady=true, 8 goals ready.
- **AnomalyDetectorState**: self-healing выключен (стандарт MDB); свежие
  METRIC_ANOMALY все в статусе IGNORED — типичные скачки p999
  (BROKER_CONSUMER/FOLLOWER_FETCH/PRODUCE_LOCAL_TIME_MS_999TH по брокерам
  hc/pc/ec, значения единицы-десятки мс); balancednessScore=100.0.
  Старый BROKER_FAILURE по 20001 от 28.08 — CHECK_WITH_DELAY, давно отработан.
- **Логи**: `grep -c ' ERROR '` по текущему `cruise-control.out.log` = 0.

## Памятка по проверке CC одним заходом

```bash
for i in 1 2 3 4 5; do
  OUT=$(mcc --local -n dzen sshexec 1.cruise.kafka-seo-seo-kafka.rc.idzn.ru \
    "systemctl is-active cruise-control; curl -s -m 10 http://localhost:8080/kafkacruisecontrol/state; \
     grep -c ' ERROR ' /mnt/logs/dbms/cruise-control.out.log" 2>&1) && echo "$OUT" && break
  sleep 4
done
```

- Порт REST смотреть по `ss -tlnp | grep java` (8080 здесь, не 9090).
- `Connection closed by remote host` в конце вывода sshexec — норма, не ошибка.
- Порты CC: 9000 JMX, 7777 Jolokia, 8081 prometheus-agent, 8080 REST.
