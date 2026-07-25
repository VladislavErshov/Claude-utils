# Дежурный ранбук Kafka

Базовые инструкции дежурного. Источник — внутренний ранбук «Разборки Kafka».
Конкретные проблемы — `troubleshooting.md`, операции с Cruise Control — `cruise_control_ops.md`,
рутинное администрирование (топики/ACL/users) — `administration.md`.

## Важное

- Все скрипты взаимодействия с кластером — в `/opt/kafka/bin`. На каждую команду есть `--help`.
- Все конфиги — в `/opt/kafka/config`: `broker.properties`, `controller.properties`,
  `producer.properties`, `consumer.properties` (последние два — для локальных тестов).
- Серты — `/opt/kafka/ssl`.
- Логи — `/mnt/logs/dbms`.

## Доступность кластера

Кластер состоит из нод 2 типов: **контроллеры** и **брокеры**. Среди контроллеров 1 лидер.
Количество контроллеров `2n+1`, где `n` — сколько может упасть и кластер выживет.

Пример: 5 контроллеров → кластер жив, пока живы минимум 3 (2 недоступны).

Кластер **недоступен**, если упало больше `n` контроллеров. Обычно помогает рестарт.

```bash
systemctl restart kafka-controller  # контроллеры
systemctl restart kafka-broker      # брокеры
```

## Логи

Лежат в `/mnt/logs/dbms`:

| Файл | Что внутри |
| --- | --- |
| `kafka-controller.err.log` | ошибки контроллера |
| `kafka-controller.log` | лог контроллера |
| `kafka-broker.err.log` | ошибки брокера |
| `kafka-broker.log` | лог брокера |
| `kafka-exporter.err.log` | ошибки kafka-exporter (метрики консьюмеров) |
| `kafka-exporter.out.log` | лог kafka-exporter |

Анализ логов через скилл [`mcc-host-access`](../../mcc-host-access/SKILL.md) (`mcc ssh`/`mcc scp`) — скилл `kafka-log-investigator`.

## Порты

- Брокеры: `9092` (INTERNAL) и `9093` (WAN).
- Контроллеры: `9093`.

## Про топики, пользователей и ACL

Для всех кластеров по умолчанию включён `kafka.sync` на операторе. Таска синхронизирует
в среднем раз в 30 минут реальное состояние топиков/пользователей с нашей базой.

→ Если пользователи жалуются на «параметры, которые они не задавали» — скорее всего кафка
  прислала нам дефолтные значения, и `kafka.sync` их перезаписал.

Расположение лидеров/партиций по брокерам удобно смотреть в Cruise Control — пригодится
при разборе инцидентов.

## Kafkactl

Утилита администрирования кластером: https://github.com/deviceinsight/kafkactl

Возможности: чтение/запись топика, управление топиками (describe/create/alter),
управление consumer groups, управление ACL, изменение параметров брокера,
перераспределение партиций **без генерации json**.

### Установка на хост брокера

Через скилл [`mcc-host-access`](../../mcc-host-access/SKILL.md) (команды `scp`, `ssh`):

```bash
# scp kafkactl_5.18.0_linux_amd64.tar.gz <broker>:/opt/kafka/config
# ssh <broker>
cd /opt/kafka/config
tar -xvf kafkactl_5.18.0_linux_amd64.tar.gz
vim kctl.yaml
```

### Конфиг `kctl.yaml`

```yaml
contexts:
  default:
    brokers:
      - 1.broker.<cluster>.<dc>.one-infra.ru:9092
  remote-cluster:
    brokers:
      - 1.broker.<cluster>.hc.one-infra.ru:9092
      - 1.broker.<cluster>.kc.one-infra.ru:9092
      - 1.broker.<cluster>.pc.one-infra.ru:9092
      - 2.broker.<cluster>.hc.one-infra.ru:9092
      - 2.broker.<cluster>.kc.one-infra.ru:9092
      - 2.broker.<cluster>.pc.one-infra.ru:9092
    tls:
      enabled: true
      ca: /opt/kafka/ssl/tls_ca.crt
      insecure: true
    sasl:
      enabled: true
      username: super
      password: password
      mechanism: plaintext
```

Пароль `super` взять из `/opt/kafka/config/jaas.conf`:
```bash
cat /opt/kafka/config/jaas.conf
```

### Примеры

```bash
# Изменить min.insync.replicas
./kafkactl alter topic <topic> --config-file kctl.yaml --context remote-cluster \
  --config min.insync.replicas=1

# Изменить replication-factor без генерации json
./kafkactl alter topic <topic> --config-file kctl.yaml --context remote-cluster \
  --replication-factor 2
```

## Встреча по кафке

- Тайминги и разбор — https://vkvideo.ru/video-227738875_456240804?list=d8390036ac515df8f7
