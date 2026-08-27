# T1 Happy path — upscale контроллера {dc:2} через mdb-data (2026-08-24)

**Результат: PASS** (замечания по mdb-data см. в конце)

## Запуск

- Эндпоинт mdb-data (порт 8081): `POST /api/v2/mdb/kafka/clusters/9fc47c1b-011d-4aaa-b411-de5345a0204e/hosts/controllers?dc=dc` → 202.
- mdb-data сам строит `controllersPerDc` = текущие контроллеры по ДЦ +1 в target DC (KafkaHostsServiceImpl.upscaleKafkaController).
- operationId = workflowId: `f2020cd2-0a25-471c-a2d2-1a76b5f7a842`, parent + child `<opId>_<dc>`.
- Local temporal UI: workflow `f2020cd2-0a25-471c-a2d2-1a76b5f7a842`.

## Baseline (до)

- Кластер жив: контроллеры dc/hc/kc active; ACC: dc=0, hc=1 (лидер), kc=0. Кворум 3 voters.
- PMS `kafka.controller.quorum` (broker-ключ `test-modify3-mdbdev-kafka.clouds`): `10001@1…dc:9093, 11001@1…hc:9093, 12001@1…kc:9093`.
- ⚠️ quorum/layout живут на broker-ключе, на `controller.<queue>.clouds` — NOT_SET.

## Temporal input parent (декодирован)

`controllersPerDc {kc:1, hc:1, dc:2}`, `brokerDcs [dc,hc,kc]`, queueInfo:
`queueName=test-modify3-mdbdev-kafka.mdbdev.db.production.mdb.prod`, `queueShortName=test-modify3-mdbdev-kafka`,
`pmsHost=test-modify3-mdbdev-kafka.clouds`, productId 7514, namespace infra; hw alloc 2c/2g/10g NVME; TTL 3ч. — контракт T7 OK.

## Ход workflow (92 события)

1. `cloud_getInfosForServices` (discovery);
2. `upsertKafkaLayout` + `upsertControllerQuorum` ×3 (по одному на ДЦ — идемпотентные upsert);
3. 3 child `upscaleKafkaControllerInDc`:
   - **hc, kc** — мгновенный COMPLETED (skip: цель 1==1 уже достигнута, только discovery-activity);
   - **dc** — `submitQueueIfNeeded` → `isServiceExists` → `getServiceInfo` → `rescaleService` 1→2 → `getServiceInfo` (wait running);
4. reload: child `updateConfigKafkaBroker` + `updateConfigKafkaController` (parameters=null);
5. `saveUpscaledKafkaControllersInfo`.
Итог: parent COMPLETED (~9 мин, основное время — ожидание деплоя хоста в one-cloud).

## После (post-проверки)

- **PMS quorum** = baseline + `10002@2.controller…dc:9093` — union-merge без дублей. PASS
- **Хост 2.controller…dc**: поднят, сервис active, Jolokia отвечает, ACC=0 (follower). PASS
- **controller.properties на новом хосте** физически содержит 4 voters (reload применил). PASS
- **operations mdb-data**: одна операция add_hosts, status done. PASS

## ⚠️ Замечание (не блокер, проверить отдельно)

`host_state` в mdb-data **локальном** НЕ пополнилась `2.controller…dc` — в локальном seeding-режиме
saveUpscaledControllersInfo, судя по всему, пишет не в локальную БД (или фильтруется). На проде этот
шаг обновляет host_state. Перед T2/T3 проверить, куда реально пишет activity `saveUpscaledKafkaControllersInfo`
и как mdb-data обновляет host_state после операции. Для T2 (повторный запуск) это важно: mdb-data строит
controllersPerDc из host_state — без записи нового хоста повторный запрос даст {dc:2} снова (идемпотентно ок),
но после «честного» T5-downscale могут быть сюрпризы.

## Грабли инструментария (запомнить)

- wait_workflow.py падает (`temporal` CLI нет в PATH) — поллить через curl UI API: `GET /api/v1/namespaces/default/workflows?pageSize=5` + jq по workflowId.
- Jolokia GET: `:` и `,` в MBean-имени обязательно URL-энкодить (`%3D` нельзя для `=`? — нет: `name%3D…,type%3D…` работает), `kafka.controller:type=raft-metrics,name=current-state` на контроллере 3.x/4.x отсутствует — искать через `/jolokia/search/kafka.controller:*`.
- В expect `[a-z]` внутри send — Tcl-грабля (интерпретируется как команда). Для grep-классов на удалённом хосте экранировать или упрощать паттерн.
- pms-read.sh читает только broker-ключ для quorum/layout — controller-ключ всегда NOT_SET, это норма.
