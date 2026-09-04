# MDBDEV-2969 / WSINCID-2880: руткоз недоступности кафки ГП (vkwsgp-mail-kafka)

- **Дата**: 2026-08-21
- **Кластер**: `vkwsgp-mail-kafka` (id `7bd18711-9fc0-4e86-9595-f40d05ec2308`, infra, production)
- **Инцидент**: WSINCID-2880 — 5 августа кафка групповых политик недоступна ~15:24–17:10 МСК.
- **Вердикт**: **не ресайз**. Пустое `kafka.sysconfig` на controller-ключе создал баг в
  `KafkaPmsActivityImpl.upsertSysconfig` (mdb-processing) во время modify-«фикса» (op2).

## Хронология 2026-08-05 (Temporal + PMS-аудит, всё сходится до секунд)

| Время МСК | Событие | Источник |
|---|---|---|
| 14:36 | op1 (82ac359b, done): resize + update. Broker: 4cpu/8G/48G, heap 1024; **controller: только resize** (`updateConfigData: null`) | БД operations + Temporal input |
| 15:20:59 | op1 `upsertSysconfig` broker-ключ → heap 1024 (был 1024 — не менялся) | Temporal event 13 + PMS audit (api-update, n.dergunov) |
| 15:21–15:23 | op1: reload-цикл рестартов 3 брокеров | Temporal |
| 15:24 | Кафка ГП деградировала (рестарт всех брокеров + параллельный resize инстансов) | таймлайн WSINCID-2880 |
| 15:41 | op1 завершена | БД |
| 15:49:16 | op2 (f20288b1, done): «фикс» — modify update-config `heapSizeMB=2048` (broker и controller) | БД + Temporal input |
| **15:49:20.492** | op2 `upsertSysconfig(controller.vkwsgp-mail-kafka.clouds, 2048)` → на ключе **не было** `kafka.sysconfig` → `loadCurrentPropertiesRaw`="" → `patchHeapOpts` skip → `createOrUpdateVariable` записал **ПУСТУЮ строку**. PMS-аудит: `create, propertyValue="", user=n.dergunov` | PMS audit + Temporal (completed 12:49:20.5Z — совпадение до мс) |
| 15:49:29–15:50:29 | op2: SSH-рестарты 3 контроллеров → поднялись с пустым `/etc/sysconfig/kafka` (без jaas/heap/javaagent) → **KRaft-кворум лёг → полная недоступность** | Temporal `kafka_host_restartControllerInstanceSsh` ×3 |
| 16:51 | op3 (face77b8, canceled): попытка отката heap 1024; успела записать broker-ключ 1024, отменена до рестартов | Temporal + PMS audit (13:51:15Z api-update 1024) |
| 17:03:53 | **e.selivanov вручную**: broker-ключ → полный конфиг heap 2048 | PMS audit |
| 17:04:08 | **e.selivanov вручную**: controller-ключ → полный конфиг heap 1024m (текущее значение) | PMS audit (api-update, 666 байт) |
| ~17:10+ | Кафка ГП восстановлена | таймлайн инцидента |

PMS-аккаунт `n.dergunov` — под ним mdb-processing делает api-update в PMS (массовые
create/api-update от этого пользователя на других controller-ключах — миграции/modify-флоу).

## Баг (mdb-processing)

`KafkaPmsActivityImpl.upsertSysconfig` (строки ~313-352):

```java
var updated = loadCurrentPropertiesRaw(namespace, pmsHostName, SYSCONFIG.value()); // "" если переменной нет
final var patched = patchHeapOpts(updated, heapSizeMB).orElse(null); // Optional.empty: KAFKA_HEAP_OPTS не найден в ""
if (patched == null) { log.warn("...skipping heap patch"); }        // ← только warn
else { updated = patched; }
pmsService.createOrUpdateVariable(namespace, pmsHostName, SYSCONFIG.value(), updated, null); // ← пишет ""
```

При отсутствии `kafka.sysconfig` на ключе патч skip'ается, но пустая строка всё равно
сабмитится (`create`). Затем рестарт контроллера с пустым sysconfig → процесс без
jaas/heap/JMX → KRaft-нода не работает. В отличие от этого, backstage-генератор
(`TemplateBuilder.buildKafkaControllerSysconfig`) рендерит полный шаблон — но в
modify-флоу mdb-processing им не пользуется.

## Системный риск

117 кластеров (см. `MDBDEV-2969-kafka-sysconfig-notset-clusters.txt`) не имеют
`kafka.sysconfig` на `controller.<queue>.clouds`. Любой modify с controller
heap/tosAgent у них создаст пустой sysconfig и уложит кластер при рестарте контроллеров.

## Фикс (2026-08-21, mdb-processing)

`KafkaPmsActivityImpl.upsertSysconfig`: при `isBlank(current)` — **пропуск записи** (warn + return),
пустое значение в PMS не создаётся вообще. Первая версия фикса (бутстрап из шаблона
`kafka-sysconfig.template`) отклонена пользователем — принято решение «не создавать файл, если
параметра нет». Тесты: 2 unit-кейса в `KafkaPmsActivityImplTest`
(`shouldSkipWhenMissingInPms` / `shouldSkipWhenBlankInPms`, проверяют `never()` на
`createOrUpdateVariable`). `./gradlew check` зелёный.

Грабля: `verify(...).createOrUpdateVariable(any(), any(), ...)` даёт ambiguous (в PmsService два
перегруза — Namespace и ExtendedNamespace) → первый мэтчер надо типизировать `any(Namespace.class)`.
Вторая грабля: строки text block в тестах тоже лимитируются checkstyle 120 символами.

## Фикс 2 (2026-08-21, mdb-processing): jinja-import utils при создании cruise-control

`KafkaPmsActivityImpl.upsertBrokerCruiseMetrics`: cruise-блок (kafka-broker-cruise-metrics.template)
использует `utils.calculate_path_to_cluster_secret(...)` — jinja-объект `utils` импортируется первой
строкой broker.properties (`{% import "/etc/misc/utils.j2" as utils -%}`). На старых кластерах без этой
строки confp-рендер на хосте падал. Фикс: перед аппендом блока `ensureJinjaUtilsImport` — regexp-проверка
`\{%-?\s*import\s+["']/etc/misc/utils\.j2["']` по всему файлу; если нет — импорт добавляется первой
строкой, если есть (обычно первая строка) — не дублируется. Тесты: 3 unit-кейса (добавление /
не-дублирование / пустые properties).

## Как расследовали (переиспользуемые приёмы)

1. **Temporal history без UI**: `https://temporal.common.mdb.one-infra.ru/api/v1/namespaces/default/workflows/<id>/history?maximumPageSize=500` — доступен без авторизации. Payload'ы base64 в `.input.payloads[].data` / `.result`. Вложенность: modifyController → updateControllerConfig → activity `upsertSysconfig` (input: namespace, pmsHostName, heapSizeMB, tosAgentEnabled). У `eventId` тип строка — jq: `select((.eventId|tonumber)==N)`.
2. **PMS-аудит без UI**: `POST https://pms.cloud.vk.team/internal/api/properties/audit.do` (фильтры application/propertyName/host — но даты не работают, отдаёт последние N) и правильный `POST /internal/api/audit.do` с телом `{"applicationId":"mdb","propertyName":"kafka.sysconfig","hostname":"controller.<queue>.clouds","fromDate":"YYYY-MM-DD","toDate":"YYYY-MM-DD","pageIndex":0,"pageSize":50}` (mTLS + `x-namespace`). Отдаёт items[] с action/username/date/propertyValue/oldValueDiff. Названия полей выведены из main-бандла UI (класс ZW).
3. **Операции кластера**: `SELECT ... FROM operations WHERE cluster_id=... ` (колонки created_ts/started_ts/finished_ts); версии: `db_cluster_version.cluster_params->'kafkaParams'->...` (186292: broker 1024/ctrl null; 186420: оба 2048; 186533: 1024/null).

## Файлы

- `/tmp` (opencode tmp): `hist_*.json` — истории Temporal, `audit_ok.json` / `audit_broker.json` — PMS-аудит controller/broker ключей.
