# 2026-08-21 — Прод-БД: cruise-записи one_cloud_meta + db_cluster_version (даунгрейд 4.3→3.8)

Прод-инстанс `backstage_plugin_mdb` через `mcc tp-port-forward` (см. секцию «Продовая БД» в SKILL.md).
Все операции согласованы с пользователем в чате перед выполнением.

## one_cloud_meta

1. **Вставка** fd812e93 (dzen-comments4): cruise-control-service, fake_id 17561, params от db-service
   записи кластера (queue dzen-comments4-datatransfer-kafka, serviceName=cruise).
2. **Массовая вставка** 106 кластерам без cruise-записи (INSERT…SELECT, см.
   kafka-config-inspector/history/2026-08-21_cruise-serviceName-migration-all-clusters.md).
3. **Переименование** serviceName cruise-control → cruise: 16 живых + 59 soft-deleted
   (59 сверх плана из-за отсутствия фильтра deleted=false — оставлены по решению пользователя).

Итог: `SELECT count(*) FROM one_cloud_meta WHERE params->>'serviceName'='cruise-control'` → 0.

## db_cluster_version

fd812e93, строка 224001 (status=draft, type=update) — db_version заменён на версию downgrade7
(образец: кластер 23f108ac, строка 196248):

```json
{"id": "9", "dockers": [
  {"dockerTag": "1.0.2", "dockerName": "ubuntu20-mdb-cruisecontrol-2.5.147", "dockerType": "cruise-control"},
  {"dockerTag": "2.4.3", "dockerName": "ubuntu20-kafka-3.8.0", "dockerType": "service"}
], "sharded": false, "versionName": "3.8"}
```

## PMS-часть той же миграции

См. `~/.claude/skills/kafka-config-inspector/history/2026-08-21_cruise-serviceName-migration-all-clusters.md`
(копии vault-pki.certs на cruise.* ключи, ns infra/dzen/vkontakte).

## 2026-08-27 — дозачистка: переименование cruise-хоста в host_state

Кластер d74193bc (ml-platform-test-mops-kafka, kc.idzn.ru): host
`1.cruise-control.ml-platform-test-mops-kafka.kc.idzn.ru` → `1.cruise.ml-platform-test-mops-kafka.kc.idzn.ru`
вместе с replace старого имени в grafana_dashboard_link и onecloud_ui_link
(one_cloud_meta serviceName=cruise уже был корректен). SQL — через docker cp + psql -f,
BEGIN/COMMIT, верификация после. Осталась запись старого формата у кластера
e1bdd84e (`...kc.wan.idzn.ru`) — не переименовывалась (вне ТЗ).
Общее правило: при rename FQDN в host_state менять и вложенные ссылки
(var-instance в grafana, service/<name> в onecloud_ui_link), иначе ссылки битые.

### wan-конвенция host_state (выяснена 2026-08-27 на кейсe «нет метрик круиза»)

Для кластеров с isWan=true в host_state: `host` = **wan-FQDN**
(`1.cruise.<queue>.<dc>.wan.idzn.ru`, как у брокеров `1.broker...wan...`),
а grafana var-instance и prometheus instance-label — **без wan**
(`1.cruise.<queue>.<dc>.idzn.ru`). Эталонные примеры: events, kafka-seo.
ml-platform-test (d74193bc) доведён до конвенции: host → wan-форма,
ссылки остались без wan. ml-platform-prod (79ab1d8c) НЕ переименован:
в облаке хост ещё реально `1.cruise-control...` (миграция имени не доезжала),
one_cloud_meta serviceName=cruise при этом уже переименован — рассинхрон.

### Зачистка «нет метрик круиза» (2026-08-27, ml-platform-test / d74193bc)

Цепочка: host_state имел старый non-wan FQDN → доведён до wan-конвенции.
Метрики не шли из-за network-пулов манифеста в облаке: cruise-хост был
lan-only, у брокеров wan+wlan (пользователь починил манифест сам через
админку облака; после этого mcc instances стал показывать wan+w wlan).
Диагностика: JMX-экспортер CC живёт на порту **8081** (конфиг
/opt/prometheus/cruise-control-*.yml), 8080 — REST API CC (Jetty,
/metrics → 404 — это норма, не ошибка). Реальный instance-label CC = FQDN
без .wan (DNS: non-wan имя → wlan IP, wan имя → wan IP).

Свип по всем таблицам с cluster_id на «cruise-control» для кластера:
- легитимно (не трогать): db_cluster_version (dockerType "cruise-control",
  образ ubuntu20-mdb-cruisecontrol), one_cloud_meta params_type
  'cruise-control-service', operations/tasks завершённого create_cluster
  (история со старыми fqdn/serviceName — не правим);
- реальный остаток: **cluster_to_template** — cruise-шаблон манифеста
  содержит `prometheus_labels=...;mdb_kafka_cluster=cruise-control.${CLUSTER_QUEUE_NAME}`
  (у broker/controller-шаблонов — `${CLUSTER_QUEUE_NAME}` без префикса).
  При регенерации манифеста лейбл снова уедет в cruise-control.<queue>.
  Решение (БД vs код mdb-processing и нужное значение лейбла) — не принято.
- чисто: host_state, kafka_config, serviceName в one_cloud_meta.
