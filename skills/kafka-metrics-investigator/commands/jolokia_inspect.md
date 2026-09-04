# Jolokia-инспекция Kafka MBean'ов

## Зачем

Jolokia — JMX over HTTP на порту 7777. Используется:
- **rscheck** (`/etc/rscheck/modules/checkkafka.py`) — для проверки состояния брокера.
- **host_checker** (`/etc/host_checker/checks/check_kafka.py`) — для отправки статуса в Backstage.
- **kafka_exporter** — отдельный процесс для consumer metrics (на порту 23569, не Jolokia).

Если rscheck / host_checker падают с KeyError → UI помечает брокер как "dead". Чтобы понять,
какой MBean отсутствует, нужно дёрнуть Jolokia напрямую.

## Доступ

Скилл [`mcc-host-worker`](../../mcc-host-worker/SKILL.md) (команда `ssh`) не принимает
аргументы с пробелами/пайпами. Поэтому команды с curl нужно выполнять **в интерактивной
сессии** на хосте:

```bash
# Подключиться к хосту через скилл mcc-host-worker (команда ssh):
#   host = 1.broker.<cluster>.<dc>.one-infra.ru
# затем на хосте:
curl -s 'http://localhost:7777/jolokia/read/<mbean>'
```

Или попросить пользователя выполнить команды и прислать вывод.

## Базовые endpoint'ы

| URL | Что делает |
|---|---|
| `http://localhost:7777/jolokia/list` | Список всех JMX доменов и MBean'ов |
| `http://localhost:7777/jolokia/read/<mbean>` | Чтение значения MBean |
| `http://localhost:7777/jolokia/read/<mbean>/<attribute>` | Чтение конкретного атрибута |
| `http://localhost:7777/jolokia/list/<domain>` | MBean'ы в конкретном домене |
| `http://localhost:7777/jolokia/list/<domain>/<mbean>` | Атрибуты конкретного MBean |

## Список всех JMX доменов

```bash
curl -s 'http://localhost:7777/jolokia/list' | python3 -c "import json,sys; d=json.load(sys.stdin); print('\n'.join(sorted(d['value'].keys())))"
```

На broker-хосте в Kafka 4.x домены: `kafka`, `kafka.cluster`, `kafka.coordinator.group`,
`kafka.coordinator.transaction`, `kafka.log`, `kafka.network`, `kafka.producer`, `kafka.server`,
`kafka.utils`, `org.apache.kafka.server`, `java.lang`, и системные.

На controller-хосте дополнительно есть `kafka.controller`.

⚠️ **На broker-хосте НЕТ домена `kafka.controller`** — это нормально, BrokerServer не запускает KafkaController.

## Поиск MBean'ов по ключевым словам

```bash
curl -s 'http://localhost:7777/jolokia/list' | python3 -c "import json,sys; d=json.load(sys.stdin); [print(f'{dom}:{k}') for dom,v in d['value'].items() for k in v.keys() if any(x in k.lower() for x in ['broker','controller','leader','fenced','active','raft','state','quorum'])]"
```

## Ключевые MBean'ы и их назначение

### На broker-хосте (process.roles=broker)

| MBean | Что показывает |
|---|---|
| `kafka.server:name=BrokerState,type=KafkaServer` | Состояние брокера: 0=NotRunning, 1=Starting, 2=Recovery, **3=Running**, 6=PendingControlledShutdown, 7=BrokerShuttingDown |
| `kafka.server:name=CurrentControllerId,type=MetadataLoader` | ID текущего активного controller'а |
| `kafka.server:name=CurrentMetadataVersion,type=MetadataLoader` | Версия metadata (KRaft) |
| `kafka.server:type=ReplicaManager,name=UnderReplicatedPartitions` | Кол-во under-replicated партиций (должно быть 0) |
| `kafka.server:type=ReplicaManager,name=UnderMinIsrPartitionCount` | Кол-во партиций с ISR < min.insync.replicas |
| `kafka.server:type=ReplicaManager,name=IsrExpandsPerSec` | Расширение ISR (репликация догоняет) |
| `kafka.server:type=ReplicaManager,name=IsrShrinksPerSec` | Сжатие ISR (реплика отстаёт) |
| `kafka.cluster:type=Partition,topic=*,name=AtMinIsr,partition=*` | Партиции на границе min ISR |

⚠️ **Грабля: не все MBean видны через Jolokia (7777)** — некоторые есть только через JMX
exporter (8080) как Prometheus-метрики. Например `kafka.server:type=ReplicaFetcherManager,name=MaxLag`
(follower lag, панель Grafana «Broker Max Lag») через Jolokia отдаёт `InstanceNotFoundException`,
но через `curl http://localhost:8080/metrics | grep replicafetchermanager_maxlag` — есть.
Если MBean «не нашёлся» через Jolokia, всегда проверяй второй порт. Подробности и список
лаг-метрик по портам — `commands/check_metrics.md` → «Follower lag» и «Дедупликация лагов».

### На controller-хосте (process.roles=controller)

| MBean | Что показывает |
|---|---|
| `kafka.controller:name=ActiveControllerCount,type=KafkaController` | 1 на активном controller'е, 0 на остальных |
| `kafka.controller:name=ActiveBrokerCount,type=KafkaController` | Кол-во активных брокеров |
| `kafka.controller:name=FencedBrokerCount,type=KafkaController` | Кол-во fenced брокеров (должно быть 0) |
| `kafka.controller:name=GlobalPartitionCount,type=KafkaController` | Всего партиций в кластере |
| `kafka.controller:name=GlobalTopicCount,type=KafkaController` | Всего топиков в кластере |
| `kafka.controller:name=OfflinePartitionsCount,type=KafkaController` | Оффлайн партиции (должно быть 0) |
| `kafka.controller:name=NewActiveControllersCount,type=KafkaController` | Сменился ли активный controller (1=недавно) |
| `kafka.server:name=ClusterId,type=ControllerServer` | Cluster ID |
| `kafka.server:type=raft-metrics` | Raft-метрики (существует на controller, **но НЕ на broker в Kafka 4.x**) |

## Различия Kafka 3.x vs 4.x

### `kafka.server:type=raft-metrics/current-state`

- **Kafka 3.x**: существует на broker-хосте, value=`"observer"` (broker-only), `"voter"` / `"leader"` / `"follower"` (controller).
- **Kafka 4.x**: на broker-хосте MBean **удалён** → `InstanceNotFoundException`. На controller-хосте — есть.

Старый rscheck использовал этот MBean в `is_broker()`:
```python
return responseState["value"] == "observer"
```

На 4.x `response["value"]` поднимает KeyError → "Broker is dead".

### Фикс

Использовать `kafka.server:name=BrokerState,type=KafkaServer`:
- На broker-хосте: MBean существует, value=3 (Running).
- На controller-хосте: MBean НЕ существует (404).

```python
def is_broker(self):
    url = f"{self.base_metrics_url}kafka.server:name=BrokerState,type=KafkaServer"
    response = requests.get(url).json()
    return "value" in response
```

Этот MBean существует и в 3.x, и в 4.x — фикс универсальный.

### `kafka.controller:*` MBean'ы

В обеих версиях существуют на controller-хосте (KRaft-only setup). На broker-хосте их нет —
это нормально (BrokerServer не запускает KafkaController).

## Проверка состояния брокера (быстрая)

```bash
# На broker-хосте:
curl -s 'http://localhost:7777/jolokia/read/kafka.server:name=BrokerState,type=KafkaServer'
# Ожидаемый ответ: {"value":{"Value":3}, "status":200, ...}
# value.Value=3 → Running, всё ок
# value.Value=2 → Recovery, ещё не готов
# 404 error → что-то не так, MBean должен быть
```

```bash
# Under-replicated partitions (должно быть 0):
curl -s 'http://localhost:7777/jolokia/read/kafka.server:type=ReplicaManager,name=UnderReplicatedPartitions'
```

## Проверка состояния controller'а

```bash
# На controller-хосте:
curl -s 'http://localhost:7777/jolokia/read/kafka.controller:name=ActiveControllerCount,type=KafkaController'
# value.Value=1 → это активный controller
# value.Value=0 → standby controller
```

```bash
# Fenced brokers (должно быть 0):
curl -s 'http://localhost:7777/jolokia/read/kafka.controller:name=FencedBrokerCount,type=KafkaController'

# Active brokers (должно быть равно числу брокеров в кластере):
curl -s 'http://localhost:7777/jolokia/read/kafka.controller:name=ActiveBrokerCount,type=KafkaController'

# Offline partitions (должно быть 0):
curl -s 'http://localhost:7777/jolokia/read/kafka.controller:name=OfflinePartitionsCount,type=KafkaController'
```

## Kafka CLI утилиты (на хосте)

Bootstrap server — `$cloud_hostname:9092` (env var на хосте, не localhost).
Все CLI требуют `--command-config /opt/kafka/config/client.properties` (там SSL/SASL креды).

```bash
# Список топиков
kafka-topics.sh --bootstrap-server $cloud_hostname:9092 --command-config /opt/kafka/config/client.properties --list

# Детали топиков (partitions, RF, ISR, leader)
kafka-topics.sh --bootstrap-server $cloud_hostname:9092 --command-config /opt/kafka/config/client.properties --describe

# Состояние KRaft quorum
kafka-metadata-quorum.sh --bootstrap-server $cloud_hostname:9092 --command-config /opt/kafka/config/client.properties describe --status

# Список controller'ов (voters)
kafka-metadata-quorum.sh --bootstrap-server $cloud_hostname:9092 --command-config /opt/kafka/config/client.properties describe --members

# Cluster ID
kafka-cluster.sh --bootstrap-server $cloud_hostname:9092 --command-config /opt/kafka/config/client.properties clusterid

# Consumer groups
kafka-consumer-groups.sh --bootstrap-server $cloud_hostname:9092 --command-config /opt/kafka/config/client.properties --list
kafka-consumer-groups.sh --bootstrap-server $cloud_hostname:9092 --command-config /opt/kafka/config/client.properties --describe --group <group>

# List brokers
kafka-broker-api-versions.sh --bootstrap-server $cloud_hostname:9092 --command-config /opt/kafka/config/client.properties | head -20
```

⚠️ Без `--command-config` команды падают с SSL/SASL ошибками.
