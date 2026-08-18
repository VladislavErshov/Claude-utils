# Диагностика "Broker is dead" в UI mdb-data

## Симптом

В UI mdb-data брокер отображается как `unknown` / `dead`, хотя:
- Процесс `kafka-broker.service` запущен (`systemctl status kafka-broker.service`).
- Брокер зарегистрировался в KRaft quorum (в `kafka-broker.out.log` есть `Successfully registered broker`).
- Брокер отвечает на Jolokia-запросы.

## Главные подозреваемые

1. **rscheck** (`/etc/rscheck/modules/checkkafka.py`) — падает с исключением → `return "Broker is dead"`.
2. **host_checker** (`/etc/host_checker/checks/check_kafka.py`) — падает с исключением → отправляет в Backstage `UNAVAILABLE`.

Оба ходят через Jolokia на `http://localhost:7777/jolokia/read/<mbean>`. Любой KeyError / InstanceNotFoundException
приводит к "Broker is dead".

## Пошаговый разбор

### Шаг 1: проверить, что брокер действительно жив

```bash
# На хосте 1.broker.<cluster>.<dc>.one-infra.ru:
curl -s 'http://localhost:7777/jolokia/read/kafka.server:name=BrokerState,type=KafkaServer'
```

Ожидаемый ответ (брокер жив):
```json
{"request":{...},"value":{"Value":3},"timestamp":...,"status":200}
```

`Value=3` → Running. Брокер жив.

Если MBean не существует → `InstanceNotFoundException` → брокер не стартовал, идти смотреть `kafka-broker.out.log`.

### Шаг 2: проверить, какой MBean отсутствует

Старый rscheck использовал `kafka.server:type=raft-metrics/current-state`:

```bash
curl -s 'http://localhost:7777/jolokia/read/kafka.server:type=raft-metrics/current-state'
```

Если ответ содержит `"status": 404` и `"error_type": "javax.management.InstanceNotFoundException"` —
это и есть причина "Broker is dead" в UI. MBean удалён в Kafka 4.x на broker-only хостах.

### Шаг 3: подтвердить, что rscheck падает именно на этом

Посмотреть исходник rscheck — скачать с хоста через скилл [`mcc-host-access`](../../mcc-host-access/SKILL.md)
(команда `scp`, источник `1.broker.<cluster>.<dc>.one-infra.ru:/etc/rscheck/` → `~/kafka_rscheck/`):

```bash
mkdir -p ~/kafka_rscheck
# scp "1.broker.<cluster>.<dc>.one-infra.ru:/etc/rscheck/" → ~/kafka_rscheck/
cat ~/kafka_rscheck/modules/checkkafka.py
```

Или сразу проверить ключевые MBean'ы, которые использует rscheck:

```bash
# is_broker():
curl -s 'http://localhost:7777/jolokia/read/kafka.server:type=raft-metrics/current-state'

# get_partitions_status():
curl -s 'http://localhost:7777/jolokia/read/kafka.server:type=ReplicaManager,name=UnderReplicatedPartitions'
```

Если первый возвращает 404 — rscheck упадёт в `is_broker()` на `response["value"]`.

### Шаг 4: host_checker

Та же история — `check_kafka.py` в host_checker тоже использует `kafka.server:type=raft-metrics/current-state`.
Скачать с хоста через скилл [`mcc-host-access`](../../mcc-host-access/SKILL.md) (команда `scp`,
источник `1.broker.<cluster>.<dc>.one-infra.ru:/etc/host_checker/` → `~/kafka_host_checker/`):

```bash
mkdir -p ~/kafka_host_checker
# scp "1.broker.<cluster>.<dc>.one-infra.ru:/etc/host_checker/" → ~/kafka_host_checker/
cat ~/kafka_host_checker/checks/check_kafka.py
```

Если в `get_data_from_kafka()` MBean не находится → возвращается `{}` → `get_kafka_node_info()` возвращает
`KafkaNodeInfo(False, "UNKNOWN")` → `status = "UNAVAILABLE"` → отправляется в Backstage → UI помечает
брокера как dead.

## Фикс rscheck

В `/etc/rscheck/modules/checkkafka.py` (лежит в docker-image `ubuntu20-kafka-base/rootfs/etc/rscheck/modules/checkkafka.py`):

### Было

```python
self.current_state_endpoint = "kafka.server:type=raft-metrics/current-state"

def is_broker(self):
    urlState = f"{self.base_metrics_url}{self.current_state_endpoint}"
    responseState = requests.get(urlState).json()
    return responseState["value"] == "observer"
```

### Стало

```python
self.broker_state_endpoint = "kafka.server:name=BrokerState,type=KafkaServer"

def is_broker(self):
    url = f"{self.base_metrics_url}{self.broker_state_endpoint}"
    response = requests.get(url).json()
    return "value" in response
```

Логика: на broker-хосте MBean существует (`value` в ответе) → True. На controller-хосте MBean
не существует (404) → False.

MBean `kafka.server:name=BrokerState,type=KafkaServer` существует в обеих версиях Kafka (3.x и 4.x)
на broker-хостах. Фикс универсальный.

## Фикс host_checker

Аналогично в `/etc/host_checker/checks/check_kafka.py` (лежит в `ubuntu20-kafka-base/rootfs/etc/host_checker/checks/check_kafka.py`):

### Было

```python
def get_data_from_kafka(self):
    try:
        response = urllib.request.urlopen(
            "http://localhost:7777/jolokia/read/kafka.server:type=raft-metrics/current-state",
            timeout=3
        )
        return json.load(response)
    except Exception as err:
        log.error(err)
        return {}
```

### Стало

```python
def get_data_from_kafka(self):
    try:
        response = urllib.request.urlopen(
            "http://localhost:7777/jolokia/read/kafka.server:name=BrokerState,type=KafkaServer",
            timeout=3
        )
        return json.load(response)
    except Exception as err:
        log.error(err)
        return {}
```

И в `get_kafka_node_info()` — поменять определение роли (broker/controller). Старый код проверял
`data["value"] == "observer"`. Новый — проверять наличие `value` (MBean существует → broker, иначе controller).

## После фикса

1. Пересобрать docker-образ `ubuntu20-kafka-base`.
2. Пересобрать все versioned-образы (`ubuntu20-kafka-3.8.0`, `ubuntu20-kafka-4.3.0`, ...).
3. Передеплоить хосты кластера (modify-флоу mdb-data или пересоздание).
4. После деплоя подождать 1-2 минуты (rscheck ходит с интервалом) и проверить UI mdb-data.

## Альтернативные причины "Broker is dead"

Если MBean'ы на месте, но статус всё равно dead:

1. **Jolokia не отвечает** — `curl http://localhost:7777/jolokia/` висит или connection refused.
   Причина: `kafka-broker.service` упал, либо Jolokia agent не стартовал.
   Смотреть `kafka-broker.err.log` и `systemctl status kafka-broker.service`.

2. **Брокер не зарегистрировался в KRaft quorum** — в логе нет `Successfully registered broker`.
   Причина: controller quorum не собрался, DNS не резолвит controller hostname.
   Смотреть `kafka-controller.out.log` на всех controller-хостах + проверять DNS.

3. **Fenced брокер** — брокер зарегистрировался, но controller его "заборнил" (fenced).
   На controller-хосте: `curl -s 'http://localhost:7777/jolokia/read/kafka.controller:name=FencedBrokerCount,type=KafkaController'`.
   Если `Value > 0` — есть fenced брокеры. Смотреть `kafka-controller.out.log` на предмет
   `Fencing broker` событий.

4. **Under-replicated partitions > 0** — rscheck возвращает статус "Has N under-replicated partitions",
   но не "Broker is dead". UI может показывать warning, но не dead. Если при этом ещё и min ISR
   пробит — статус "Has N partitions with min in-sync replicas" + rank=RANK_PREFAIL.
