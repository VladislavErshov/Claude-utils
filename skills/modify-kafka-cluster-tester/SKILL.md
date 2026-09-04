---
name: modify-kafka-cluster-tester
description: Локальное тестирование modify-операции Kafka-кластера (PATCH /api/v2/mdb/kafka/clusters/{id}/modify) через mdb-data → mdb-processing. Первый сценарий — ресайз брокеров со сменой типа диска (MDBDEV-3231): запрет валидатора снят в mdb-data, waitShardAllocMatches в processing теперь ждёт совпадения типа. Тестовый кластер 9fc47c1b-011d-4aaa-b411-de5345a0204e (test-modify3, project 160). Используй когда нужно локально прогнать modify/resize-флоу Kafka и проверить сходимость после падений.
allowed-tools: [bash, read_file, edit_file, write_file, grep, glob]
---

# Скилл тестирования modify Kafka-кластера

Общие разделы (инфраструктура, seed, прод-Temporal, верификация) — ниже.
Специфика сценариев — в секциях.

# Секция: Смена типа диска при modify (MDBDEV-3231)

## Что тестируем

- **mdb-data** (branch `ershov/MDBDEV-3231-allow-disk-type-change-on-kafka-modify`):
  снят запрет смены `params.diskType` в `KafkaClusterModificationValidator`
  («Нельзя изменить тип диска при модификации кластера» — удалён вместе с параметром
  `currentDiskType` из сигнатуры валидатора). Diff-детектор (`hasBrokerResourcesDiff`)
  и маппер volumes (`toBrokerResources`/`toControllerResources`) уже поддерживали тип.
- **mdb-processing** (branch `ershov/MDBDEV-3231-check-disk-type-on-resize`):
  `BaseInstanceResizeWorkflowImpl.waitShardAllocMatches` теперь сравнивает
  `volume.type() == disk.type()` (раньше — только размер: resize с тем же размером,
  но другим типом завершался без фактической смены). Сверка манифеста шеорда
  (`StorageManifestWrapper.hasEqualsSize`) тип уже учитывала.
- Смена типа реализуется облаком через `migrationRequired` → `migrateShard`
  (grow не подходит — тип ≠ размер).

Ключевые свойства: валидация пропускает смену типа; volumes с новым типом доезжают
до temporal input; resize ждёт фактического применения типа (не тихий false-success);
идемпотентность повторного запуска.

## Seed-кластер: test-modify3 — общая для всех операций

- cluster_id: `9fc47c1b-011d-4aaa-b411-de5345a0204e`, project 160 (mdbdev), kafka, namespace INFRA.
- Хосты: brokers dc/hc/kc, controllers dc/hc/kc (по 1), cruise в ic.
- В draft-версиях `cluster_params` обязательно пустые
  `kafkaParams.brokerConfig.config={}` и `kafkaParams.controller.controllerConfig.config={}`
  (иначе NPE в `KafkaClusterDiffDetector`):

```sql
UPDATE db_cluster_version
SET cluster_params = jsonb_set(jsonb_set(cluster_params, '{kafkaParams,brokerConfig,config}', '{}'), '{kafkaParams,controller,controllerConfig,config}', '{}')
WHERE cluster_id='9fc47c1b-011d-4aaa-b411-de5345a0204e' AND status='draft';
```

Seed SQL (выгрузка из прода через read-only туннель localhost:53480 одним
`jsonb_build_object`-запросом, вставка через `docker cp` + `psql -f`), грабли
`one_cloud_meta.fake_id`, синхронизация БД/облака/PMS/operations — полностью
как в скилле **`scale-kafka-hosts-tester`**, секция «Seed-кластер: test-modify3».
Готовый снапшот: `/tmp/test-modify3.json` (если жив).

## Инфраструктура (порты) — общая для всех операций

| Сервис | Порт | Запуск |
|---|---|---|
| mdb-data | 8081 | `bootRun --args='--spring.profiles.active=local --server.port=8081'` (лог `/tmp/mdb-data.log`) |
| mdb-processing | 8080 | `bootRun --args='--spring.profiles.active=local'` (лог `/tmp/mdb-processing.log`) |
| temporal (local) | 8233 | docker-compose в `mdb-processing/localrun/` (`/setup-local-temporal`) |
| postgres (mdb-data) | 6434 | контейнер `pg_backstage_plugin_mdb`, БД `backstage_plugin_mdb`, юзер `dev` |
| wiremock | 8088 | docker-compose mdb-processing |

Auth в mdb-data local-профиле отключён — curl без токена.
⚠️ local-профиль mdb-processing пишет в РЕАЛЬНЫЙ `pms.cloud.vk.team` и ходит в
реальный one-cloud (wiremock их НЕ перехватывает). Только dev-кластеры (project 160).
Снапшот PMS до/после обязателен (чтение/запись PMS — скилл [`pms-worker`](../pms-worker/SKILL.md)).

## Эндпоинт и структура запроса

`PATCH http://localhost:8081/api/v2/mdb/kafka/clusters/9fc47c1b-011d-4aaa-b411-de5345a0204e/modify`

Тело — обёртка `ModifyKafkaClusterRequest` (НЕ плоские params). Все неизменяемые
поля = baseline из `db_cluster_version` (последняя версия), меняем только нужное:

```json
{
  "params": {
    "acl": {}, "name": "<db_cluster.name>", "isWan": false,
    "lanIn": <baseline>, "lanOut": <baseline>,
    "diskGb": <baseline diskGb>, "diskType": "<НОВЫЙ ТИП: network-hdd|network-ssd|nvme>",
    "rootQueue": "<baseline>",
    "needLanIpv6": false, "needWanIpv4": false, "needWanIpv6": false,
    "kafkaParams": {
      "controller": {"controllerDcs": <baseline>, "controllerConfig": {"config": {}},
        "controllerDiskGb": <baseline>, "controllerDiskType": "<baseline>",
        "controllerVcores": <baseline>, "controllerMemGb": <baseline>,
        "controllerLanIn": <baseline>, "controllerLanOut": <baseline>},
      "brokerConfig": {"config": {}},
      "jvmHeapSizeMb": <baseline>,
      "cruiseControl": {"cruiseControlDc": "<baseline>"}
    }
  },
  "hardwarePresetId": <baseline hardware_preset_id>,
  "isNeedShards": false,
  "hosts": [{"dc": "dc"}, {"dc": "hc"}, {"dc": "kc"}],
  "type": "update_instances",
  "attempts": 3
}
```

Baseline-поля брать из БД (иначе валидатор lan/heap зарежет или DiffDetector
посчитает лишние изменения):

```bash
docker exec pg_backstage_plugin_mdb psql -U dev -d backstage_plugin_mdb -c \
  "SELECT cluster_params->>'diskType', cluster_params->>'diskGb', cluster_params->>'lanIn',
          cluster_params->>'lanOut', cluster_params->'kafkaParams'->'jvmHeapSizeMb',
          hardware_preset_id
   FROM db_cluster_version WHERE cluster_id='9fc47c1b-011d-4aaa-b411-de5345a0204e'
   ORDER BY create_ts DESC LIMIT 1;"
```

## ⚠️ Образ docker (грабля MDBDEV-3231)

Resize сабмитит инстанс с образом ИЗ ТАБЛИЦЫ (`db_cluster_version.db_version`,
jsonb `dockers`) — НЕ из манифеста облака. Поэтому:

1. **Не класть в локальную БД неправильную версию.** Сеять `db_cluster_version`
   (и `db_versions`/`db_version_dockers`) актуальным снапшотом из прод-БД — там лежат
   верные версии. Не хардкодить и не тащить старые снапшоты.
2. Перед запуском сверять `db_version.dockers[service]` кластера с образом, который
   реально крутится на хостах (манифест инстанса в облаке / `kafka-host-inspector`).
   Рассинхрон (кластер переключали образом через облако, а БД отстала) → resize уедет
   со старым образом.
3. Симптом рассинхрона: инстанс после submit не стартует, one-cloud 400
   `ServiceValidationException "cannot start by either reason"` (наблюдалось на
   test-modify3: БД числила 3.8/ubuntu20-kafka-3.8.0:2.4.0, кластер реально на
   4.3.0:1.0.2).
4. Для тестов выбирать кластеры, у которых версия в прод-БД совпадает с реальным
   образом: test-downgrade6 (69204f9d) — 4.3, `ubuntu20-kafka-4.3.0:1.0.2`;
   test-downgrade5/7 — 3.8, `ubuntu20-kafka-3.8.0:2.4.3`.
5. Тот же 400 может быть и при незавершённой миграции дисков — облако доиграет её
   само, retry-цикл `startInstance` сойдётся; различать по состоянию шеорда
   (`cloud_shardInfo`: `MIGRATING` + `migrationRequired=true`).

## ⚠️ Lost timer в локальном temporal (грабля окружения)

У долгоживущего docker-temporal (localrun, аптайм недели) таймер `Workflow.sleep`
может «теряться»: TIMER_STARTED есть, TIMER_FIRED не приходит часами. Симптом:
workflow RUNNING, последние события — TIMER_STARTED десятки минут назад, при этом
другие workflow живые. Диагноз через query `__stack_trace` (POST
`…/workflows/{wid}/query/__stack_trace` + csrf-cookie): стек показывает BLOCKED на
`Workflow.sleep`. `docker restart mdb-processing-temporal` НЕ лечит (mutable state
в postgres уже битый). Лечение: terminate workflow + перезапуск операции
(draft откатить, иначе «No changes found» 400).

Наблюдалось 26.08: M2/M3 зависли в `update-broker-config` (executeReloadCycle,
15-сек пауза), M4/M5 — живые. Рестарт не помог, terminate+rerun — помог.
Вечером 26.08 — ещё 4 случая (M2, M3×2, reverse) за один сетевой блэкаут-день.
Нюанс: query `__stack_trace` может «пнуть» зависший workflow (M2 после запроса
проснулся и дошёл сам); terminate иногда возвращает 404 — workflow уже закрыт.

## ⚠️ PREFAIL-брокеры блокируют resize-родителя (by design)

`takeNext` (KafkaIteratePolicy) не берёт PREFAIL-инстансы → родитель бесконечно
ретраит NO_AVAILABLE_HOSTS, пока облако не снимет PREFAIL. Если PREFAIL «застрял»
(например, UnderReplicatedPartitions>0 после блэкаута) — операция упрётся в
runTimeout 1ч и упадёт TIMED_OUT. Диагностика: `curl http://<host>:81/getstatus`
на брокере (true = ок; текст = причина), Jolokia
`kafka.server:type=ReplicaManager,name=UnderReplicatedPartitions` (источник:
docker-images `ubuntu20-kafka-base/rootfs/etc/rscheck/modules/checkkafka.py`).
⚠️ plaintext kafka-CLI (9092/9093 SASL_SSL) таймаутится всегда — не диагностика.

## ⚠️ Порядок фаз modify: UPDATE_THEN_RESIZE у брокеров

`ModifyKafkaBrokerWorkflowImpl`: при `order=UPDATE_THEN_RESIZE` сначала
update-config (SSH-рестарт всех брокеров), и ТОЛЬКО потом resize. Если видишь,
что resize-ребёнка нет, а update-config уже пошёл — это не баг, resize в очереди.
Terminate update-config = resize не стартует никогда (весь modify-родитель гаснет).
У controller-ветки порядок resize-first — M4 прошёл resize при живом update.

## Декодирование temporal input (local)

```bash
curl -s "http://localhost:8233/api/v1/namespaces/default/workflows/$WID/history" | \
  jq -r '.history.events[0].workflowExecutionStartedEventAttributes.input.payloads[0].data' | base64 -d | jq
```

Для сценария смены типа диска проверяем в input `modifyKafkaCluster`:
`brokerResources.volumes.disks[0]` = `{"name":"data","type":"<новый>","sizeGb":<N>}`;
`controllerResources` — `volumes: null` (controllerDiskType не менялся). Это норма.

## План тестирования (смена типа диска)

### M1. Happy path: broker diskType network-ssd → network-hdd (или наоборот)
1. Seed test-modify3, baseline-снимок: `diskType`/`diskGb`/hosts, PMS-снапшот,
   состояние volumes в облаке (`cloud_getServiceInfo`).
2. PATCH /modify с новым `params.diskType`, остальное = baseline.
3. Ожидание 202 — валидация прошла (нет ошибки `params.diskType` — запрет снят).
4. temporal: `modifyKafkaCluster` → фазы controller (пропуск, volumes null) →
   `resizeKafkaBroker` → инстансные child: `submitStorageAlloc` с новым типом →
   `migrationRequired` → `migrateShard` → ожидание совпадения size+type.
5. После COMPLETED: baseline `db_cluster_version` обновлён (новый `diskType`),
   `operations` = done, тип volume в облаке реально изменён.

### M2. Регресс: только diskGb (классический grow)
Тот же запрос, `diskType` = baseline, `diskGb` больше. Обычный grow без миграции —
проверяет, что фикс типа не сломал resize по размеру.

### M3. Смена типа без смены размера (главный кейс фикса)
`diskType` новый, `diskGb` тот же. Раньше `waitShardAllocMatches` сравнивал только
размер → тихий false-success. Теперь workflow обязан дождаться реальной смены типа
(или упасть по retry `MigrationWait`, если облако не может — диагностируемо, не тихо).

### M4. Контроллерный диск
`kafkaParams.controller.controllerDiskType` = новый тип. Зеркально M1, но фаза
controller: `resizeKafkaController` с volumes в input, brokers пропускаются.

### M5. Идемпотентность / рестарт после падения
1. Уронить workflow terminate'ом посреди broker-фазы (POST
   `…/workflows/{wid}/terminate`, csrf-cookie — как в scale-kafka-hosts-tester),
   либо `kill` mdb-processing во время ожидания миграции.
2. Перезапустить операцию (та же operationId → тот же workflowId, фазы с
   `ALLOW_DUPLICATE_FAILED_ONLY` пропускают завершённое).
3. Ожидание: сходимость к M1, тип не «задваивается», миграция не запускается повторно
   если уже завершена.

### M6. Негатив: валидация остальных полей не сломана
Запрос со сменой diskType + невалидное lan (prod-минимум) / heap → старые ошибки
валидатора возвращаются, кроме `params.diskType`.

| Сценарий | Статус | История |
|---|---|---|
| M1 Happy path смена типа брокеров | PASS (nvme↔ssd; полный 3-ДЦ nvme→ssd BLOCKED квотой SSD продукта 7514) | history/2026-08-26-M1-broker-disk-type-change.md |
| M2 Регресс grow | PASS (op da58a0ce; lost timer + PREFAIL по пути — сам дошёл) | history/2026-08-26-evening-M2-M6-results.md |
| M3 Тип без размера | PASS (входит в M1; отдельный прогон: modify3 nvme 2g→hdd 2g, op 463315a1 — все фазы, ~40 мин; попытка на downgrade5 ⏸ отложена из-за pc PREFAIL) | history/2026-08-26-evening-M2-M6-results.md |
| M4 Контроллерный диск | PASS (716bbd02 после DNS-фейла 046c10a2; идемпотентные скипы) | history/2026-08-26-evening-M2-M6-results.md |
| M5 Идемпотентность/рестарт | PASS (6b588732: облако само перезапустило миграцию, скипы совпавших) | history/2026-08-26-evening-M2-M6-results.md |
| M6 Негатив валидации | PASS частично: 2n+1-валидация ок (400); lanIn-минимум НЕ воспроизводится на dev (prod-блок выключен) | history/2026-08-26-evening-M2-M6-results.md |
| Reverse hdd→nvme + lanIn modify3 | PASS (70672ddf после 2 упавших попыток) | history/2026-08-26-evening-M2-M6-results.md |

Кластеры-доноры (seed из прода 26.08, project 160, broker/controller ДЦ hc/kc/pc):
- test-downgrade5 `184ac05d-64e7-4276-ad59-017475bf4f4a` (3.8, nvme 8)
- test-downgrade6 `69204f9d-723c-4ad7-848c-efcd1b2389bd` (4.3, nvme 8)
- test-downgrade7 `23f108ac-1907-434e-a67b-dda01df316f4` (3.8, nvme 25)
Снапшот+seed: `/tmp/test-downgrades.json`, `/tmp/seed_downgrades.sql` (включают
db_versions/db_version_dockers из прода).

⚠️ С 26.08 в тестовых модифях — diskGb=2 (экономия квот: миграция держит старый+новый
volume; SSD продукта 30G). Целевой тип для смен — hdd (квота HDD почти свободна).

⚠️ Грабли M1: (1) чётные controllerDcs (4 ДЦ после upscale в ic) режутся валидатором —
передавать 3 из 4; (2) draft с новым diskType пишется при старте → ретрай даёт
«No changes found», откатывать draft; (3) смена типа = миграция = двойной спрос квоты.

## Ключевые точки кода (для диагностики)

| Что | Где |
|---|---|
| Валидация diskType (запрет снят) | mdb-data `KafkaClusterModificationValidator` |
| Diff-детектор (resourcesDiff при смене diskType) | mdb-data `KafkaClusterDiffDetector.hasBrokerResourcesDiff` |
| Сборка volumes в DTO | mdb-data `KafkaClusterModificationMapper.toBrokerResources` / `toControllerResources` |
| Ожидание size+type volume | mdb-processing `BaseInstanceResizeWorkflowImpl.waitShardAllocMatches` |
| Сравнение манифеста шеорда (уже с type) | proxylib `StorageManifestWrapper.hasEqualsSize` |
| Миграция шеорда при смене типа | mdb-processing `BaseInstanceResizeWorkflowImpl.migrateOrGrowIfRequired` |

## Верификация кластера ДО и ПОСЛЕ каждого сценария

Как в `scale-kafka-hosts-tester`: baseline-снимок до (host_state, volumes облака,
PMS), тот же после + diff. Дополнительно для дисков:
- фактический тип/размер volume в облаке (`cloud_getServiceInfo` / UI one-cloud) —
  до и после: тип должен измениться ровно у data-дисков брокеров целевых ДЦ;
- после рестартов инстансов — живость кластера через `/kafka-cluster-inspector`
  (KRaft quorum, брокеры зарегистрированы, ISR не сломаны).

## Откат состояния между сценариями

- После M1/M3/M4 вернуть `diskType` в исходный симметричным modify-запросом
  (обратная смена типа — тот же механизм). НЕ править БД/облако руками.
- 409 при новом запуске: `UPDATE operations SET status='done'` (как в scale-скилле).
- Terminate workflow — только через UI API с csrf-cookie.

## Правило запуска операций

Только через mdb-data API (`PATCH …/modify`), не напрямую через temporal:
прямой запуск рассинхронизирует БД с облаком (save не выполняется).

## Смежные скиллы

- `scale-kafka-hosts-tester` — seed test-modify3, выгрузка из прода, прод-Temporal,
  terminate/откат, верификация кластера (эталон для общих разделов)
- `mdb-local-tester` — структура modify-запроса, mapping request → temporal input
- `kafka-cluster-inspector` / `kafka-config-inspector` — живость и сверка конфигов

## Сохранение результатов

Каждый прогнанный сценарий — в `history/` этого скилла: baseline SQL, modify JSON,
workflowId, декодированный input, результат, найденные баги. Прежде чем писать новый
сценарий — `ls history/`. Готовые шаблоны запросов — `configs/`.
