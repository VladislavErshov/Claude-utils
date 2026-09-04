# bilmigrated-datatransfer-kafka: CC в crash-loop — PMS заполнен, но нет импорта utils.j2

**Дата**: 2026-08-21
**Кластер**: `bilmigrated-datatransfer-kafka` (4 брокера: hc/pc/uc/kc по 1, cruise в kc)
**Хост CC**: `1.cruise.bilmigrated-datatransfer-kafka.kc.one-infra.ru`
**Симптом (жалоба)**: «хост cruise лежит» (`mcc ssh ... --container main`).

## Симптом

`cruise-control.service` в crash-loop: старт → ~68 сек → exit 1 → systemd-рестарт каждые
~90 сек (NRestarts=9, Result=exit-code). err.log пуст, причина в `cruise-control.out.log`:

```
java.lang.IllegalStateException: Cruise Control cannot find the metrics reporter topic
that matches [__CruiseControlMetrics] in the Kafka cluster.
```

## Диагностика — трёхуровневая сверка

| Уровень | Проверка | Результат |
|---|---|---|
| Кластер | `kafka-topics --list \| grep -i cruise` на брокере | топика `__CruiseControlMetrics` нет |
| Брокер-хост | `grep metric.reporters /opt/kafka/config/broker.properties` | 0 вхождений; файл от 2026-08-18 18:00 (до create_additional_service) |
| Образ | `ls /opt/kafka/libs/ \| grep -i cruise` | JAR `cruise-control-metrics-reporter-2.5.141.jar` есть — образ не блокер |
| PMS | `pms-read.sh bilmigrated-datatransfer-kafka.clouds kafka.broker.properties infra mdb` | блок `metric.reporters` + `cruise.control.metrics.*` ЕСТЬ, `topic.auto.create=true` |

Вывод: create_additional_service записал блок репортера в PMS, но рендер на брокеры
не дошёл (файлы старше PMS-изменения, брокеры не рестартовались с 18.08).

## Попытка фикса и новый блокер

`confp --oneshot` на брокере падает (рестарт из-за `&&` не выполнился — безопасно):

```
confp.backends INFO Key 'KAFKA_BROKER_RACK' not found in backend 'env'. Falling back to default value.
confp.confp ERROR Exception while evaluating template for dest /opt/kafka/config/broker.properties:
jinja2.exceptions.UndefinedError: 'utils' is undefined'
```

**Корень-2**: в PMS-переменной `kafka.broker.properties` блок репортера использует
`{{ vault(utils.calculate_path_to_cluster_secret('super', 'kafka', 'password')) }}`,
но **импорт `{% import "/etc/misc/utils.j2" as utils -%}` в начале файла отсутствует** —
забыт шаг 7 процедуры «Поднять круиз» (cruise_control_ops.md: «В начало конфига добавить /
проверить, что есть импорт»).

## Фикс (руками через web PMS)

1. PMS: `https://pms.cloud.vk.team/client/#/props-search?ns=infra&a=mdb&h=bilmigrated-datatransfer-kafka.clouds`
   → `kafka.broker.properties` → первой строкой добавить:
   ```
   {% import "/etc/misc/utils.j2" as utils -%}
   ```
2. На каждом брокере поочерёдно (строго по одному, дожидаться регистрации):
   ```bash
   mcc --local sshexec -n infra 1.broker.bilmigrated-datatransfer-kafka.<dc>.one-infra.ru \
     "confp --oneshot && systemctl restart kafka-broker"
   # проверка на этом же хосте:
   grep -E 'Successfully registered broker|Kafka Server started' /mnt/logs/dbms/kafka-broker.out.log | tail -2
   ```
   Порядок: hc → pc → uc → kc.
3. Репортер создаст `__CruiseControlMetrics` при старте первого брокера
   (`cruise.control.metrics.topic.auto.create=true`). Маркер: `Starting Cruise Control metrics
   reporter` в `kafka-broker.out.log`.
4. CC поднимется сам со следующего systemd-рестарта (он в restart-loop, отдельный рестарт
   не нужен). После — подождать ~30 мин на накопление окон.

## Грабли / уроки

1. **«Хост cruise лежит» ≠ хост лежит** — сначала `systemctl show cruise-control -p
   NRestarts -p Result`: crash-loop с Result=exit-code выглядит как «лежит», но systemd
   tirelessly крутит сервис.
2. **`IllegalStateException: cannot find the metrics reporter topic` = репортер не
   настроен на брокерах**, не «сломан CC». Трёхуровневая сверка: топик в кластере →
   `metric.reporters` в файле на брокере → блок в PMS.
3. **PMS заполнен ≠ конфиг применён**: сверять дату `broker.properties` на хосте с
   датой create_additional_service. Рендер может не дойти (в этом кейсе — не дошёл вовсе).
4. **Блок репортера в PMS без `{% import ... as utils %}` роняет ВСЬ рендер
   broker.properties** (не только блок репортера). При добавлении блока по шагу 7 всегда
   проверять импорт в первой строке. Хорошая новость: упавший confp не портит файл,
   `&&` не пускает рестарт — состояние безопасное.
5. Строки репортера с `SASL_SSL` и `SASL_PLAINTEXT` в PMS-переменной одновременно — это
   jinja2-условие по `kafka.ssl.enabled` (две ветки), не дубликат-ошибка.
