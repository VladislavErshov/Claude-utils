# MDBDEV-2969: Аудит пустых `kafka.sysconfig` на controller-ключах PMS

- **Дата**: 2026-08-20
- **Скоуп**: 785 Kafka-кластеров в prod БД `pg_backstage_plugin_mdb` (все `type='kafka'`, `deleted=false`)
- **Цель**: найти кластеры, где на controller-ключе в PMS значение `kafka.sysconfig` отсутствует или пустое.

## Метод

Для каждого кластера из `one_cloud_meta` извлекается `queue` (из JSON-колонки `params`), формируется PMS-ключ `controller.<queue>.clouds` (см. граблю в `SKILL.md` «PMS-ключи для controller-хостов разбиты на два»). Через `pms-read.sh` (curl+mTLS к `https://pms.cloud.vk.team/api/conf/values.do?application=mdb&property=kafka.sysconfig`) читается значение. Скрипт `/tmp/check_kafka_sysconfig_full.sh`.

### Namespace по `domain`

| `params.domain` | PMS namespace |
|---|---|
| `one-infra` | `infra` |
| `idzn` | `dzen` |
| `vkcl` | `vkontakte` (⚠️ не `vkcl` — PMS использует `vkontakte` для vkcl-кластеров; проверено через web-интерфейс `https://pms.cloud.vk.team/client/#/props-search?ns=vkontakte`) |

### SQL для удалённой БД (одним JSON)

```sql
SELECT jsonb_agg(jsonb_build_object(
  'cluster_id', c.id,
  'name', c.name,
  'queue', m.params->>'queue',
  'domain', m.params->>'domain',
  'deleted', c.deleted,
  'environment', c.environment
) ORDER BY c.name)
FROM db_cluster c
JOIN one_cloud_meta m ON m.cluster_id = c.id
WHERE c.type = 'kafka'
  AND m.params_type = 'db-service'
  AND c.deleted = false;
```

Результат — в `out.json` (785 записей).

## Результат

| Категория | Кол-во | Файл |
|---|---|---|
| PMS вернул `<NOT_SET>` (переменной нет на `controller.<queue>.clouds`) | **117** | `MDBDEV-2969-kafka-sysconfig-notset-clusters.txt` |
| PMS вернул значение, но только whitespace/пустые строки | **0** | — |
| PMS вернул полноценный конфиг (JMX_PORT, KAFKA_HEAP_OPTS, KAFKA_OPTS, …) | **668** | `MDBDEV-2969-kafka-sysconfig-all-clusters.tsv` |
| Итого | 785 | |

**Главный вывод:** кейса «PMS что-то отдал, но файл пустой» среди 785 Kafka-кластеров **нет**. PMS-API ведёт себя бинарно — либо переменная есть с полным конфигом (минимум 9 значимых строк, ~1000+ байт), либо её нет вообще (`<NOT_SET>`). Промежуточного состояния «пустое значение» не существует.

## Разбивка 117 NOT_SET кластеров

### По namespace

| namespace | кол-во |
|---|---|
| `infra` | 93 |
| `vkontakte` | 13 |
| `dzen` | 12 |

### По environment

| environment | кол-во |
|---|---|
| `production` | 113 |
| `dev` | 4 |

### Подмножество пустых production-кластеров по namespace

#### infra (93)
Примеры: `ads-comp`, `aigen`, `auction-realtime`, `bizon-edr-kafka`, `callbackd-billing`, `debezium`, `debezium-test`, `dzen-com1-attr`…`dzen-com4-attr`, `kafka-dev`, `kafka-m2b-staging`, `vkcluster-kafka`, `vkmyvkteamprod`, `ya-yt-channel` и др.

#### vkontakte (13)
`apps-staging`, `debezium0`, `eventbus-sr-prod`, `eventbus-sr-stage`, `g3-prod`, `kafka-dbaas-3`, `logs-nc`, `mvp-eventbus-kafka`, `rugc-backend-s`, `sandbox-testing`, `vk-alert-staging`, `vk-health-production`, `vk-health-staging`.

#### dzen (12)
`blogger-import`, `comments`, `dzen-common-kafka`, `kafka-content2`, `ml-platform-prod`, `ml-platform-test`, `odklcluster-kafka`, `one-flow`, `tg-bot-updates`, `ucp`, `ucp-e2e`, `ucp-test`.

## Файлы

- `MDBDEV-2969-kafka-sysconfig-notset-clusters.txt` — TSV со 117 NOT_SET кластерами (cluster_id, name, queue, namespace, environment).
- `MDBDEV-2969-kafka-sysconfig-all-clusters.tsv` — полный лог по 785 кластерам: cluster_id, name, queue, namespace, sig_lines, total_bytes, value_short(100 chars).

## Скрипт

`/tmp/check_kafka_sysconfig_full.sh` — bash+curl+jq, ~10 минут на 785 кластеров. Ключевые шаги:
1. Читает `out.json` через `jq`
2. Для каждого: curl PMS-API → `jq` фильтрует по ключу `controller.<queue>.clouds`
3. Сравнение с `<NOT_SET>`/`null`/пусто → NOT_SET-категория
4. Иначе: `grep -cvE '^[[:space:]]*$'` для подсчёта значимых строк → если 0, то whitespace_only
5. Иначе: нормальный кластер

## Замечание

В первый прогон скрипт показал 118 NOT_SET, во второй — 117. Расхождение — кластер `pulse` (pulse-reco-kafka). Причина: network jitter / таймаут PMS. Не системная аномалия — просто перезапуск скрипта даёт ±1.

## Применимость

Аудит можно переиспользовать для других PMS-переменных (`kafka.broker.properties`, `kafka.controller.properties`, `kafka.cruisecontrol.properties`, и т.д.) — просто заменить `property=kafka.sysconfig` в curl. Скрипт тривиально адаптируется.
