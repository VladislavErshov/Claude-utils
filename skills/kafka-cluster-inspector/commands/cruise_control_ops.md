# Операции с Cruise Control

**Канон — Confluence «Дежурство MDB: Kafka», секция «Cruise-control»** (SSOT):
https://confluence.vk.team/pages/viewpage.action?pageId=1348619075

Покрывает: важное (прогрев ~10 мин), «He is dead, Jim», Cruise RUNNING UNAVAILABLE,
актуализацию конфига Cruise (`kafka.cruisecontrol.sysconfig/properties` из gitlab-шаблонов,
Manifest), «Поднять круиз для кластера» (шаги 1–10: серты в pms, capacity.json, jaas.conf,
log4j, properties, vault, конфиг брокеров, поочерёдный рестарт, манифест, host_state),
перенос в другой ДЦ. Вики живая — править там.

Пользовательская дока про круиз — https://one.vk.team/docs/mdb/docs/kafka/kafka-cruise-control.html

Известные технические баги CC (Java version mismatch, CruiseControlMetricsReporter) —
`known_issues.md`. Базовые операции и кластерные проблемы — `runbook.md`, `troubleshooting.md`.

## Наши дополнения к вики

### He is dead, Jim — продолжение

Если рестарт и проверка `kafka.cruisecontrol.capacity.json` / `kafka.cruisecontrol.properties`
не помогли — смотреть логи (скилл `kafka-log-investigator`).

### Не идут метрики от брокеров — proposal are not ready

В вики по этой проблеме 2 строки; полный разбор:

**Симптомы**: в mcc `availability_details: NO_TASK_IN_PROGRESS; Proposals are not ready`,
в `/state` CC `isProposalReady=false`, `trained=false`, `monitoredWindows` во всех окнах
малые значения (например 0.1 = 10% партиций), `monitoringCoveragePct` формально ОК (≥95%),
часть goals `notReady` с причиной `minMonitoredPartitionsPercentage=0.95` + `requiredNumSnapshots=1`.

**Причина**: `CruiseControlMetricsReporter` на части брокеров падает при подключении к
собственному bootstrap и больше не поднимается (`Connection to node -1 could not be established`
+ `App info ... for CruiseControlMetricsReporter unregistered` в `kafka-broker.out.log`).
Метрики в `__CruiseControlMetrics` не идут, CC копит снапшоты только для части партиций.

**Фикс — не выискивать конкретных виновников, а просто пройти по всем broker-хостам кластера
и поочерёдно выполнить `confp --oneshot && systemctl restart kafka-broker`**. Если coverage
сильно ниже 100%, проблема массовая, и точечный рестарт не поможет — CC всё равно не досчитает.

⚠️ **Рестарт строго поочерёдный**: один broker — `confp --oneshot && systemctl restart kafka-broker`
— **дождаться регистрации в quorum и `Kafka Server started`** на этом хосте — только потом
следующий. Параллельно рестартить **нельзя** — потеря кворума/under-replicated partitions.

```bash
mcc --local -n infra sshexec -n infra <broker-fqdn> "confp --oneshot && systemctl restart kafka-broker"
# проверка на этом же хосте:
mcc --local -n infra sshexec -n infra <broker-fqdn> \
  "grep -E 'Successfully registered broker|Kafka Server started' /mnt/logs/dbms/kafka-broker.out.log | tail -2"
# убедились, что брокер поднялся → переходим к следующему хосту
```

Маркер успеха после рестарта (на broker-хосте):
```bash
grep "Starting Cruise Control metrics reporter" /mnt/logs/dbms/kafka-broker.out.log | tail -1
# 2026-... INFO [CruiseControlMetricsReporterRunner] CruiseControlMetricsReporter - Starting Cruise Control metrics reporter with reporting interval of 60000 ms.
```

После рестарта всех брокеров — подождать ~30 минут (CC накапливает снапшоты в 5 окнах по 5 мин),
затем проверить `/state`: `isProposalReady` должен стать `true`, `trained=true`, `monitoredWindows`
должны расти до 1.0.

**Если после рестарта всех брокеров проблема осталась** — смотреть:
- `cruise-control.err.log` на OOM (см. вики «He is dead, Jim» / `known_issues.md` про CC).
- `kafka.cruisecontrol.properties` — корректный `bootstrap.servers`, `security.protocol`,
  `metric.reporters` на брокерах (см. вики «Поднять круиз для кластера», п.7).

Список broker-хостов кластера уточнить через `mcc --local -n infra -c <dc> instances
'*broker.<cluster>*'` — в некоторых ДЦ может быть меньше брокеров, чем в других.
