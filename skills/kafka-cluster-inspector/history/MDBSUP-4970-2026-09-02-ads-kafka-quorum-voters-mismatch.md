# MDBSUP-4970: ads-kafka — 14 часов NOT_LEADER_OR_FOLLOWER из-за рассинхрона controller.quorum.voters при миграции hc→ec

**Дата:** 2026-09-02
**Кластер:** `ads-kafka` (RuStore, 6f4133c3-1cfa-4ed5-be46-a60532a29952; 6 брокеров hc/kc/pc ×2;
контроллеры были hc=10001, kc=11001, pc=12001, ec=13001; namespace infra)
**Тикет:** MDBSUP-4970 (+ предыстория MDBSUP-4856/4752 — upscale ec-контроллера)

## Инцидент (29-30.08)

- 29.08 17:01:49 остановлен kc-контроллер (11001, лидер кворума) → перевыборы →
  лидером избран 10001 (hc).
- Брокеры не смогли работать с 10001: их `controller.quorum.voters` был уже
  `[11001,12001,13001]` (рендер по новому PMS), без 10001 → `Unable to send a heartbeat`
  непрерывно 14.5 часов (17:01 → 07:32) → брокеры фенсились → лидерства отозваны →
  NOT_LEADER_OR_FOLLOWER у всех клиентов (в т.ч. у 4 сервисов RuStore).
- 30.08 07:32 kc-контроллер временно подняли → снова лидер → всё восстановилось.
- С 01.09 19:50 лидер снова 10001 (kc опять был остановлен) → деградация повторилась
  и длилась до починки 02.09.

## Корень

Незавершённая миграция «hc → ec» с рассинхроном voters:
- PMS `kafka.controller.quorum` = `11001@kc,12001@pc,13001@ec` (целевой, kc — легитимный voter!).
- Брокеры отрендерены по новому листу; контроллеры hc/pc — по старому `[11001,10001,12001]`,
  ec — переходный 4-voter. Это недоведённый config reload упавшей `delete_hosts`
  (39099247, failed с 26.08 на транзиентной ошибке cloud-proxy — паттерн MDBSUP-4752).
- Остановка kc (11001) 27.08/29.08 была ошибочной (он voter по PMS); удалялся hc.
- Доп. фактор: `node.id` рендерится по позиции ДЦ в `kafka.layout=hc,kc,pc,ec`
  (механика I48592) — поэтому voters нельзя править без учёта layout.

## Починка (02.09, последовательность)

1. **Подъём kc**: `mcc start` — завис в STARTING (старый диск) → lifecycle-флоу:
   `stop` → `delete volumes` (uuid из `tool_status --type storage`, уравнение pexpect) →
   `start` → RUNNING. Чистый старт отрендерил правильный voters.
2. Кворум сам пересобрался: 11001 лидер (epoch 5792), hc «Renouncing leadership».
3. `confp --oneshot && systemctl restart kafka-controller` на **pc** (упал в момент
   перевыборов — поднялся с новым voters) и **ec** (убран 4-й voter).
4. **hc**: после рендера контроллер падает
   `node id 10001 must be included in the set of voters Set(11001,12001,13001)` —
   ожидаемо для выводимого (known-issue INCALL-42685-типа).
5. Перезапуск операции `delete_hosts` 39099247 (юзер): новый run COMPLETED 13:19,
   hc-контроллер удалён из облака и host_state.
6. **pc «Broker is dead» в облаке**: rscheck@kafka спамил «Не удалось подключиться к
   10001@hc» — держал старый voter-лист в памяти. Лечится `systemctl restart rscheck@kafka`
   (конфиг rscheck уже не содержит 10001). Availability вышла из dead → RESERVED.
   pc при этом легитимный лидер кворума (epoch 5793) — дроп диска не потребовался.
7. Финал: кворум kc+pc+ec (лидер 12001), URP=0, UNAVAIL=0, брокеры-observers lag=0.

## Грабли

- Рассинхрон voters — бомба: пока лидер тот, кто есть в листах всех, кластер «работает»;
  любой failover на отсутствующего в чьём-то листе → мгновенный полный фенсинг брокеров
  (NOT_LEADER_OR_FOLLOWER у всех клиентов), без падений самих брокеров.
- Диагностика такой деградации: `grep 'Unable to send a heartbeat' kafka-broker.out.log`
  по часам (точные границы) + `grep 'the leader is' kafka-controller.out.log | uniq`
  (таймлайн кворум-лидеров) + сверка voters в конфигах брокеров/контроллеров с PMS.
- rscheck@kafka кэширует voter-лист: после смены кворума рестартовать rscheck на всех
  хостах роли, иначе облако помечает живой хост «Broker is dead».
- `mcc start` контроллера с битым/несогласованным диском может висеть в STARTING —
  лечится stop → delete volumes → start (мета подтянется с лидера кворума).
- PMS-кворум уже мог быть правильным — проверять до правок PMS
  (`kafka.controller.quorum`, `kafka.layout`).

## Заглушка тикета

`jira-mdbsup-solver/history/MDBSUP-4970-2026-09-02.md`
