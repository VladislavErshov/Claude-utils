# Итоги вечерней сессии 26.08 (продолжение STATE-SNAPSHOT от ~20:40)

Контекст: во время сессии был сетевой блэкаут (DNS не резолвил master.{pc,hc}.odkl.ru) —
он сломал часть операций и поломал брокеры. Далее — что прошло, что отложено.

## Статусы сценариев

| Сценарий | Операции (хронология) | Итог |
|---|---|---|
| M2 grow 8→10 nvme (downgrade6) | `771a149d` (lost timer, терминирован ранее) → `da58a0ce` | ✅ **PASS** — op done, draft nvme 10; hc/kc/pc все COMPLETED |
| M3 тип без размера (downgrade5) | `27680ea7` → `08f92153` (terminate lost timer) → `8926e32c` (lost timer) → `a5247db0` | ⏸ **ОТЛОЖЕН** — TIMED_OUT (runTimeout). hc/kc мигрированы на hdd, pc остался nvme (PREFAIL в облаке). Draft = hdd 8 |
| **M3-v2 тип без размера (modify3, nvme 2g→hdd 2g)** | `463315a1` (27.08 утро) | ✅ **PASS** — done за ~40 мин, все дети COMPLETED, draft hdd 2; input `HDD 2g`, миграция шла, update-config затем resize |
| M4 ctrl nvme→hdd (downgrade7) | `046c10a2` (DNS-fail в update-broker-config) → ретрай `716bbd02` | ✅ **PASS** — done за ~2 мин, идемпотентные скипы |
| M5 modify3 2g hdd | `d3678bf5` (terminate ранее) → `7bdc0c69` (TIMED_OUT: dc-миграция в облаке умерла ATTEMPTS_LIMIT) → `6b588732` | ✅ **PASS** — done; облако само перезапустило миграцию после восстановления сети, hc/kc/controller мгновенно скипнулись |
| M6a lanIn=1 + diskType nvme | `4390b373` | ⚠️ **НЕ воспроизводится на dev**: prod-минимумы lan выключены для non-PROD / EXCLUDED_PROJECTS (`KafkaClusterModificationValidator`: `environment != PRODUCTION → return ok`). 202 прошёл и случайно запустил reverse-сценарий |
| M6b controllerDcs=[hc,kc] | — | ✅ **PASS** — 400 «Количество контроллеров Kafka должно рассчитываться как 2n+1, где n > 0». Валидация не сломана |
| Reverse modify3 hdd 2g→nvme + lanIn 1 | `4390b373` (kc failed: transient «Could not execute request») → `b325b73b` (lost timer) → `70672ddf` | ✅ **PASS** — done, draft modify3 = nvme 2g lanIn 1 |

Итоговая таблица в SKILL.md обновлена.

## Новые грабли (добавить к известным)

1. **Сетевой блэкаут на ноуте → UnknownHostException в cloud-activity**
   (`master.pc.odkl.ru: nodename nor servname provided`), RETRY_STATE_MAXIMUM_ATTEMPTS_REACHED
   у update-broker-config / resize-детей. Лечение: новая операция (draft откат + PATCH).
   Transient-вариант «Could not execute request» лечится так же.
2. **Lost timer — 4 случая за вечер** (M2, M3×2, reverse b325b73b). Симптом прежний:
   TIMER_STARTED без TIMER_FIRED >5 мин при живом воркере и свежих других workflow.
   Нюанс: M2 «проснулся» после query `__stack_trace` + terminate-цикла — query может
   пнуть workflow; terminate иногда возвращает 404 (уже закрыт).
3. **Полуживые брокеры после блэкаута**: JVM живая, порт слушает, Jolokia 7777 = 200,
   но plaintext METADATA-клиенты таймаутятся (9092/9093 SASL_SSL — plaintext CLI вообще
   не показатель). Облако держит PREFAIL → `takeNext` кидает NO_AVAILABLE_HOSTS и
   resize-родитель бесконечно ждёт (by design). Рестарт kafka-broker помог d6-hc,
   но d5-pc остался PREFAIL: чекер `rscheck@kafka` (порт 81 `/getstatus`) отвечает
   «Has 1 partitions with min in-sync replicas» = UnderReplicatedPartitions=1
   (checkkafka.py: UR>0 + minISR>0 → RANK_PREFAIL).
4. **runTimeout 1ч у modify-родителя**: если облако доигрывает миграцию дольше часа
   (наш случай после блэкаута) — операция падает TIMED_OUT. Не баг кода.

## Диагностика PREFAIL (как проверять)

- Прямой статус чекера: `curl http://localhost:81/getstatus` на брокере (true = ок).
- Jolokia: `localhost:7777/jolokia/read/kafka.server:type=ReplicaManager,name=UnderReplicatedPartitions`
  (источник чекера — docker-images `ubuntu20-kafka-base/rootfs/etc/rscheck/modules/checkkafka.py`).
- availability из temporal: последний `cloud_getInfosForServices` output в history
  resize-родителя.

## Состояние кластеров на конец сессии

| Кластер | Состояние |
|---|---|
| test-modify3 (9fc47c1b) | draft: nvme 2g, lanIn 1 (reverse прошёл) |
| test-downgrade5 (184ac05d) | draft: hdd 8; hc/kc реально на hdd, pc на nvme + PREFAIL (UR=1); операция a5247db0 failed |
| test-downgrade6 (69204f9d) | draft: nvme 10 (M2 done) |
| test-downgrade7 (23f108ac) | draft: ctrl hdd 10 (M4 done) |

## Как продолжить M3 (когда вернёмся)

1. Разобраться с pc-брокером d5 (UR=1): найти/починить партицию (это уже к
   /kafka-cluster-inspector, не modify-тест) — или ждать, пока облако само снимет PREFAIL.
2. `UPDATE operations SET status='done', in_processing=false WHERE id='a5247db0-…';`
3. Откатить свежий draft hdd → nvme (или оставить hdd и ждать «No changes» → откат).
4. PATCH `/tmp/m3-rerun.json` — hc/kc скипнутся, останется pc.
