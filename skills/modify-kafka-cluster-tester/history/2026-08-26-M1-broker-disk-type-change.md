# M1: Ресайз брокеров со сменой типа диска (MDBDEV-3231)

Дата: 2026-08-26. Кластер: test-modify3 `9fc47c1b-011d-4aaa-b411-de5345a0204e` (project 160).
Ветки: mdb-data `ershov/MDBDEV-3231-allow-disk-type-change-on-kafka-modify`,
mdb-processing `ershov/MDBDEV-3231-check-disk-type-on-resize`.

## Baseline

- `cluster_params`: diskType=**nvme**, diskGb=8, lanIn/lanOut=10/10, preset 14,
  controller: nvme/10gb, dcs dc,hc,kc,ic; jvmHeap 1024.
- Перед тестом: 6 draft-версий без `kafkaParams.brokerConfig` → проставил `{"config":{}}`
  (иначе NPE в DiffDetector).
- ⚠️ **controllerDcs=[dc,hc,kc,ic] (4 ДЦ) не проходит валидатор modify** («2n+1»).
  Передавал [dc,hc,kc] — ic-контроллер не трогается, ресурсного diff по диску нет
  (но resourcesDiff=true из-за dcs → controller resize как no-op, volumes=baseline NVME).

## Прогон 1: nvme → ssd (opId 1c53bb44)

- PATCH /modify с diskType=ssd → **202** (запрет снят; 400 был только на чётные controllerDcs).
- Temporal input `modifyKafkaCluster`: `resizeBrokerRequest.volumes.disks[0] = {name:data, size:8g, type:SSD}`,
  `resizeControllerRequest.volumes.disks[0].type = NVME` (baseline). Контракт ОК.
- Фазы: controller resize no-op (matches target ×3) → broker resize.
- **dc_1**: migrateShard → `Volumes of shard .../broker/1 matches target` — тип реально сменился.
- **hc_1**: migrate → matches target — ОК.
- **kc_1**: FAILED `submitStorageAlloc`: облако 400
  `demand is over your quota for product 7514: SSD=38G > SSD=30G. Unsatisfied: SSD=8G`.
  Миграция держит старый+новый диск — квоты SSD продукта не хватает на 3-й брокер.

Ретрай (opId a8e16911): dc/hc мгновенно matches (уже ssd), kc снова quota → FAILED.
**Вывод: это ограничение квоты продукта в облаке (Resource Manager), не код.**
Для полного nvme→ssd на 3 брокеров нужно поднять SSD-квоту продукта 7514 до ≥38G.

## Прогон 2 (rollback): ssd → nvme (opId 607ed3f8)

- Draft в БД после прогона 1 = ssd → diff есть, 202.
- Все 3 инстансных resize COMPLETED (dc, hc, kc), миграция ssd→nvme по каждому,
  `modifyKafkaCluster` **COMPLETED**.
- После: `db_cluster_version.draft.diskType = nvme`, `operations = done|modify_cluster`.
- Итог: кластер возвращён в исходное состояние (все брокеры nvme).

## Результаты

| Проверка | Результат |
|---|---|
| 202 при смене diskType (запрет снят) | PASS |
| volumes с новым типом в temporal input | PASS (SSD/NVME корректно) |
| migrateShard при смене типа | PASS (nvme→ssd и ssd→nvme) |
| waitShardAllocMatches ждёт size+type | PASS (migrate → matches target по факту) |
| Идемпотентность ретрая (dc/hc skip) | PASS (instant matches target) |
| Полный 3-ДЦ nvme→ssd | BLOCKED квотой продукта (SSD 30G < 38G) — нужен RM |
| ssd→nvme полный 3-ДЦ | PASS, COMPLETED |

## Находки/грабли

1. Чётные controllerDcs в modify-валидаторе — кластер с 4 ДЦ контроллеров (после
   upscale в ic) нельзя модифицировать с фактическим составом ДЦ. Обход: передавать
   3 из 4 ДЦ (инстансы в 4-м не трогаются).
2. mdb-data пишет draft-версию с новым diskType сразу при старте операции → ретрай
   той же операции даёт «No changes found» 400. Для симуляции ретрая откатывать
   draft diskType руками.
3. Смена типа диска = миграция = двойной спрос по квоте на время миграции
   (старый+новый). Планировать квоту продукта перед операцией.
4. **startInstance retry-loop — норма после миграции**: облако отвечает 400
   `ServiceValidationException "cannot start by either reason. Once resolved, it
   will start automatically"`, пока миграция дисков не доиграна. Workflow крутит
   getInfoForInstance→startInstance и сходится сам (dc в 607ed3f8: ~5 мин).
   НЕ terminate'ить в этом состоянии.
5. **Повторный migrateShard в ретрае — by design**: `migrationRequired=true`,
   пока миграция в полёте; облако идемпотентно (dc/hc завершились успешно).
6. Ретрай mdb-data после failed = НОВАЯ операция (607ed3f8), не ретрай той же;
   error_message «Resize failed due to one of instances. Analyze child WFs».

## Дополнение (вечер 26.08): полный флоу успешного dc_1 из 607ed3f8

`getInfoForInstance → shardInfo → submitStorageAlloc → getStorageManifest →
submitInstance → getInstanceManifest → shardInfo → migrateShard →
wait-цикл (shardInfo/getInfoForInstance/startInstance ×N) →
kafka_host_isBrokerServiceFailed → kafka_host_pingBrokerActive → COMPLETED`.
Манифест инстанса: образ `ubuntu20-kafka-4.3.0:1.0.2` (правильный, из облака).

## Артефакты

- Запрос: /tmp/m1-modify.json (baseline + diskType)
- Операции: 1c53bb44 (nvme→ssd, failed по квоте kc), a8e16911 (ретрай, failed),
  607ed3f8 (ssd→nvme rollback, done)
- Логи: /tmp/mdb-processing.log (Migrate shard requested ×5 broker, matches target)
