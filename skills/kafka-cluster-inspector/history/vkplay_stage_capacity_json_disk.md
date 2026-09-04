# vkplay-stage CC: «цели не готовы» (isProposalReady=false) — capacity.json DISK=8G при дисках 50G

**Дата**: 2026-08-21
**Кластер**: `vkplay-stage-vkplaystore-kafka` (KRaft, 3×1 брокера в hc/kc/pc + 3 controller, cruise в kc)
**Хост**: `1.cruise.vkplay-stage-vkplaystore-kafka.kc.one-infra.ru`
**Жалоба**: «CC не готовит цели» (goals not ready), rebalance через CC недоступен.

## Симптом

`GET /state`:
- `MonitorState: RUNNING(20% trained), NumValidWindows: 5/5 (100%), NumValidPartitions: 1166/1166 (100%)` — метрики в норме
- `AnalyzerState: isProposalReady: false` при том, что readyGoals перечислены
- `recentGoalViolations`: каждые 5 мин `unfixableViolatedGoals=[DiskCapacityGoal]`, status=IGNORED
- balancednessScore ~82.9, ongoingAnomalyDuration неделями

## Диагностика

| Что | Команда | Результат |
|---|---|---|
| /state | `curl -s localhost:8080/kafkacruisecontrol/state` | окна/покрытие 100%, но isProposalReady=false + вечный unfixable DiskCapacityGoal |
| capacity.json | `cat /opt/cruise-control/config/capacity.json` | `DISK: 8192` (MB) — дефолт-сток из шаблона |
| Реальный диск брокеров | `df -h /mnt/data` на всех брокерах | 50G, занято ~7.9–8.0G (16%) |

**Маркер**: занятый объём данных (~8G) совпал с заявленным лимитом DISK=8192 MB → у CC
«брокеры на 97–100% полны», DiskCapacityGoal математически неудовлетворим (некуда
перекладывать) → violation всегда unfixable, proposals не вырабатываются.

Отсюда же ложный след: «данные упёрлись в диск» — по df занято всего 16%, упирается
не диск, а заявленная capacity.

## Корень

`kafka.cruisecontrol.capacity.json` в pms остался дефолтным (DISK=8192, NW_IN=10240,
NW_OUT=20480 KB) — не актуализировали под реальные аллокации брокеров (50G диск).
Класс проблемы как в `cc_manifest_outdated_servlet_capacity.md` (устаревший
конфиг круиза), но другой симптом-манифестация.

## Фикс

1. В pms (app=mdb, host `cruise.<cluster>.clouds`) обновить `kafka.cruisecontrol.capacity.json`:
   `DISK` → реальные MB (здесь 51200). Заодно сверить `NW_IN`/`NW_OUT` (KB) с
   lan_in/lan_out аллокаций брокеров (`mcc instances` → alloc).
2. На cruise-хосте:
   ```bash
   mcc --local sshexec -n infra 1.cruise.<cluster>.<dc>.one-infra.ru \
     "confp --oneshot && systemctl restart cruise-control"
   ```
   (первый confp-прогон на cruise-хосте может упасть на vault-pki — повторить, см. MDBSUP-4739)
3. Подождать ~30 мин (накопление 5 окон), проверить `/state`: `isProposalReady: true`,
   DiskCapacityGoal violations прекратятся.

## Грабли / уроки

1. **`isProposalReady: false` при 100% windows/partitions — не проблема метрик**. Смотреть
   `recentGoalViolations` → какой goal unfixable. Unfixable capacity-goal почти всегда =
   неактуальный `capacity.json`.
2. **capacity.json DISK в MB, NW в KB** (см. шаблон в cruise_control_ops.md) — при
   актуализации не перепутать единицы.
3. `readyGoals` в /state может перечислять цели, даже когда proposal не готов —
   ориентироваться на `isProposalReady` + violations, а не на список readyGoals.
4. Metric anomalies (BROKER_PRODUCE_LOCAL_TIME_MS и т.п., status=IGNORED) и
   broker failures из-за сети — шум, к проблеме не относятся.
5. Дефолт-значения capacity.json (DISK=8192 / NW_IN=10240 / NW_OUT=20480) — маркер, что
   конфиг ни разу не актуализировали под железо кластера.
