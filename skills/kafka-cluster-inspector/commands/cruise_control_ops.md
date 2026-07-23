# Операции с Cruise Control

Диагностика проблем CC, актуализация конфига, поднятие CC на кластере, перенос CC в другой
ДЦ. Пользовательская дока — https://one.vk.team/docs/mdb/docs/kafka/kafka-cruise-control.html.

Известные технические баги CC (Java version mismatch, CruiseControlMetricsReporter) —
`known_issues.md`. Базовые операции и кластерные проблемы — `runbook.md`,
`troubleshooting.md`.

## Важное

Круизу нужно время, чтобы подняться. При старте он делает запросы к брокерам, сверяет свой
конфиг с реальными значениями и оптимизирует цели. Обычно это занимает **около 10 минут**.

## He is dead, Jim

Чаще всего помогает просто рестарт.

Если не полечило:

1. Идём в pms, смотрим `kafka.cruisecontrol.capacity.json`. Значения `DISK` и `NW*` должны
   совпадать с реальными. Если нет — обновляем конфиг.
2. Идём в pms, смотрим `kafka.cruisecontrol.properties`. Проверить, что указан корректный
   список брокеров для подключения и валидный `security.protocol`.
3. Снова делаем рестарт.
4. Если не помогло — смотреть логи (скилл `kafka-log-investigator`).

## Cruise RUNNING UNAVAILABLE

В логах ошибки вида:
```
This may happen due to any of the following reasons: (1) Authentication failed due to invalid
credentials with brokers older than 1.0.0, (2) Firewall blocking Kafka TLS traffic (eg it may
only allow HTTPS traffic), (3) Transient network issue. (org.apache.kafka.clients.NetworkClient)
```

Был случай: в pms в `kafka.cruisecontrol.properties` стоял `PLAIN` вместо `SSL`, при этом в
кафке включён SSL (`kafka.ssl.enabled`). Помогло поправить строки возле:
```
security.protocol=
```

## Не идут метрики от брокеров — proposal are not ready

1. По логам круиза (скормить в llm) найти, кто не шлёт.
2. Сделать рестарт этих брокеров.

## Актуализация конфига Cruise

### `kafka.cruisecontrol.sysconfig`

В pms (Application — `mdb`, host — `<название кластера>.clouds`) поправить из шаблона:
https://gitlab.corp.mail.ru/mdb/backstage/-/blob/main/plugins/mdb-backend/src/task/manifest/templates/kafka-cruise-sysconfig

Выставить JVM 4 Гб.

### `kafka.cruisecontrol.properties`

Из шаблона:
https://gitlab.corp.mail.ru/mdb/backstage/-/blob/main/plugins/mdb-backend/src/task/manifest/templates/kafka-cruise-control-config

Узнать у заказчиков про ребаланс (первая строка `true`, если есть хотя бы один ребаланс,
вторая про брокеров, третья про хард goals):
```properties
# Enable self healing for all anomaly detectors, unless the particular anomaly detector is explicitly disabled
self.healing.enabled=true

# Enable self healing for broker failure detector
self.healing.broker.failure.enabled=false

# Enable self healing for goal violation detector
self.healing.goal.violation.enabled=true
```

Узнать у заказчиков тротлинг репликации и указать его (в байтах), либо вообще не писать
эту строку:
```properties
# Default replica movement throttle. If not specified, movements unthrottled by default.
default.replication.throttle=5242880
```

Оставить как было (шаблонная версия):
```properties
# The Kafka cluster to control.
bootstrap.servers=$BOOTSTRAP_SERVERS
{% if $SSL_ENABLED %}
security.protocol=SASL_SSL
{% else %}
security.protocol=SASL_PLAINTEXT
{% endif %}
sasl.mechanism=PLAIN
```

### Manifest

- Выставить 6 Гб RAM.
- Выставить:
  ```yaml
  network:
    lan: v4,v6
  ```

## Поднять круиз для кластера

Сейчас делаем по API (актуальную версию кластера можно посмотреть в таблице
`db_cluster_version`):

- `kafkaParams.cruiseControl.cruiseControlDc` — из формы
- `kafkaParams.cruiseControl.cruiseUserPassword` — из формы
- поле `type='create_additional_service'`

Swagger: https://mdb.kaizen.idzn.ru/swagger-ui/api/docs/#/Cluster/post_api_mdb_cluster__clusterId__version_

### 1. Серты в pms (app=ok-pyvault)

Пример для infra ns, host: `cruise.<clustername>.clouds`:

```yaml
pki-role: hostname.one-infra.ru
dir: /etc/security/ssl
cert-name: tls.crt
key-name: tls.key
ca-name: tls_ca.crt
ttl: 4320h
user: www-data
group: www-data
mode: 400
reload-cmd: systemctl reload-or-try-restart nginx
alt-names: '{{ env(cloud_hostname) }},{{ env(cloud_hostname_wan) }}'
```

### 2. `kafka.cruisecontrol.capacity.json` (app=mdb)

Шаблон:
```json
{
  "brokerCapacities":[
    {
      "brokerId": "-1",
      "capacity": {
        "DISK": "$DISK",
        "CPU": "100",
        "NW_IN": "$NW_IN",
        "NW_OUT": "$NW_OUT"
      },
      "doc": "This is the default capacity. Capacity unit used for disk is in MB, cpu is in percentage, network throughput is in KB."
    }
  ]
}
```
`DISK` — в Мб, `NW_IN`/`NW_OUT` — в Кб.

### 3. `kafka.cruisecontrol.jaas.conf` (app=mdb, одинаковый для всех)

```
{% import "/etc/misc/utils.j2" as utils -%}

//Enter appropriate KafkaClient entry if using the SASL protocol, remove if not
KafkaClient {
  org.apache.kafka.common.security.plain.PlainLoginModule required
  username="super"
  password="{{ vault(utils.calculate_path_to_cluster_secret('super', 'kafka', 'password')) }}";
};
```

### 4. `kafka.cruisecontrol.log4j.properties` (app=mdb, одинаковый для всех)

```
# Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.

rootLogger.level=INFO
appenders=console

property.filename=./logs

appender.console.type=Console
appender.console.name=STDOUT
appender.console.layout.type=PatternLayout
appender.console.layout.pattern=[%d] %p %m (%c)%n

# Loggers
logger.cruisecontrol.name=com.linkedin.kafka.cruisecontrol
logger.cruisecontrol.level=info
logger.cruisecontrol.appenderRef.kafkaCruiseControlAppender.ref=kafkaCruiseControlFile

logger.detector.name=com.linkedin.kafka.cruisecontrol.detector
logger.detector.level=info
logger.detector.appenderRef.kafkaCruiseControlAppender.ref=kafkaCruiseControlFile

logger.operationLogger.name=operationLogger
logger.operationLogger.level=info
logger.operationLogger.appenderRef.operationAppender.ref=operationFile

logger.CruiseControlPublicAccessLogger.name=CruiseControlPublicAccessLogger
logger.CruiseControlPublicAccessLogger.level=info
logger.CruiseControlPublicAccessLogger.appenderRef.requestAppender.ref=requestFile

rootLogger.appenderRefs=console, kafkaCruiseControlAppender
rootLogger.appenderRef.console.ref=STDOUT
rootLogger.appenderRef.kafkaCruiseControlAppender.ref=kafkaCruiseControlFile
```

### 5. `kafka.cruisecontrol.properties` (app=mdb)

Взять по примеру у других кластеров. Заменить `bootstrap.servers` на минимум 3 брокера из
разных ДЦ для текущего кластера.

### 6. Vault

Создать секрет `cruise`. Пароль сгенерировать.

### 7. Конфиг брокеров в pms

Добавить/изменить в конец конфига:
```
{% if true %}
metric.reporters=com.linkedin.kafka.cruisecontrol.metricsreporter.CruiseControlMetricsReporter
cruise.control.metrics.reporter.bootstrap.servers={{ env('cloud_hostname') }}:9092
cruise.control.metrics.reporter.security.protocol=SASL_SSL
cruise.control.metrics.reporter.sasl.mechanism=PLAIN
cruise.control.metrics.reporter.sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username='super' password='{{ vault(utils.calculate_path_to_cluster_secret('super', 'kafka', 'password')) }}';
cruise.control.metrics.topic.auto.create=true
cruise.control.metrics.topic.num.partitions=9
cruise.control.metrics.topic.replication.factor=3
{% endif %}
```

В начало конфига добавить / проверить, что есть импорт:
```
{% import "/etc/misc/utils.j2" as utils -%}
```

### 8. Рестарт брокеров поочерёдно

Сверить версию образа. Минимально поддерживаемая для круиза — `1.1.5`. Если меньше —
обновляем на актуальную.

Рестарт:
```bash
confp --oneshot
systemctl restart kafka-broker
```

### 9. Сабмит манифеста круиза

Шаблон:
```yaml
type: service
namespace: ${NAMESPACE}
name: cruise
queue: ${QUEUE_NAME}
comment: null
availability:
  governor: reported
alloc:
  vcores: '2'
  mem: 2G
  lan_out: 10M
  lan_in: 10M
env:
  - ONECLOUD_PROJECT=${PROJECT}
  - ROOT_QUEUE=${ROOT_QUEUE}
  - MDB_CLUSTER_ID=${CLUSTER_ID}
  - KAFKA_CLUSTER_ID=${CLUSTER_ID}
  - DB_TYPE=cruisecontrol
  - prometheus_enabled=true
  - prometheus_metrics_cfg=/metrics:8081
  - prometheus_use_ip=lan4
  - prometheus_labels=project=mdb;mdb_project=${PROJECT};mdb_kafka_cluster=cruise-control.${CLUSTER_QUEUE_NAME}
image:
  registry: dzen-external-registry.odkl.ru
  name: ubuntu20-mdb-cruisecontrol
  version: 1.0.5
  login: mdb
mounts:
  logs: /mnt/logs
volumes:
  logs:
    size: 10g
    durability: persist
    type: nvme
timeouts:
  deploy: 10m
  start: 10m
  stop: 10m
network:
  lan: v4,v6
ports:
  '443': lan,tcp,started
  '8081': lan,tcp
  '9000': lan,tcp
```

### 10. host_state

Добавить хост в таблицу `host_state` в pg.

## Перенести круиз в другой ДЦ

1. Сохранить параметры из UI и пароль из Vault.
2. Остановить инстанс.
3. Удалить сервис и MOUNT.
4. Создать через UI в новом ДЦ, со старыми параметрами и паролем.
