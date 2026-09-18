# MDBSUP-5270: adb-users — вечный шторм выборов KRaft из-за замороженного metadata-лога kc-контроллера (чинился стопом + вайпом volumes)

**Дата:** 2026-09-09
**Кластер:** `adb-users` (e97f7382-3889-4968-9b95-97e10e2092c1, datatransfer, ns infra)
**Кворум:** контроллеры hc=10001, kc=11001, pc=12001; брокеры hc=20001, kc=21001, pc=22001
**Тикет:** MDBSUP-5270 (создан 18:07, при epoch ~58741)

## Симптом

Кластер Unavailable, клиенты NOT_LEADER_OR_FOLLOWER. Ни один контроллер не удерживает
лидерство: каждый лидер низлагается за 2–3с («Renouncing the leadership due to a metadata
log event ... in the new epoch X, the leader is (none)»), epoch гонится 58741 → 62000+
(~1000/час), все узлы Unattached/Candidate, кандидату kc все отвечают REJECTED. Брокеры
живы (mdb-health AVAILABLE), но фенсились без контроллера.

⚠️ Конфиги были **чистые**: `controller.quorum.voters` на всех трёх == PMS
(`kafka.controller.quorum` = 11001@kc,10001@hc,12001@pc; layout `hc,kc,pc`), hc/kc даже
перерендерены+рестартнуты утром того же дня. Это был **НЕ** кейс voters-drift (5044/wave2).

## Корень

**kc (11001) с замороженным metadata-логом**:

- `/mnt/data/metadata` = **272K** (против 78M/88M на hc/pc), один сегмент
  `00000000000021327797.log`, последний write **08.09 23:25**; снапшот на 21330319;
  `meta.properties` переписан **08.09 23:12** + recovery-`.checkpoint` (нечистое событие
  вчера вечером; в operations кластера ничего нет — не через mdb-data). Лог-end kc
  21330319@epoch178 против 21442k+ у живых — отставание ~111k записей.
- Как фолловер kc **никогда не догонял**: в voterStates лидера
  `11001=... lastCaughtUpTimestamp=-1`, endOffset статичен.
- Механика шторма: fetch kc доходит до лидера (lastFetchTimestamp свежий), лидер шлёт
  данные (tcpdump: басты по 11KB, **ядро kc ACK-ит**), но процесс не аппендит — ни одной
  ошибки в логе; через fetchTimeout=2000мс kc поднимает выборы → лидер видит больший
  epoch и сразу renounce → по кругу. REJECTED кандидату kc — корректный отказ по
  отстающему логу (log superiority check).
- Load на kc-хосте 49 — шторм жрёт CPU.

Диагностика-тупики (проверено, НЕ причины): рассинхрон voters (все чисто), сеть
(/dev/tcp v4/v6 OK), TLS (openssl s_client OK, verify 0), диск (2% used).

## Починка (09.09, последовательность)

1. **`systemctl stop kafka-controller` на kc** — hc+pc дают 2/3 majority → через ~20с
   лидер 10001 на epoch 62124, стабильный (новых выборов нет). Брокеры перерегистрировались
   (`CurrentObservers: [20001,22001,21001]`), кластер вернулся в работу. Кластер метился
   как лечащийся на 2/3 voter'ах до завершения этапа 2.
2. **Вайп volumes kc**: `mcc -c kc stop controller.<queue>` → instance FINISHED →
   delete volumes по UUID (pexpect-уравнение) → `start`.
3. **Грабля стартa**: после delete всплыл **старый комплект volumes от 05.09**
   (`2ac9b182/2ac9b183-4ac5-11f1`) на минионе srvk7162 в состоянии **CORRUPT**
   (`chcon failed: ... Input/output error`) → инстанс «not ready to mount», start висит.
   UUID'ы видны в `tool_status --type storage -f json` (поле `details`); удалили и их →
   storage EMPTY → start → новые volumes на srvk7902 → RUNNING.
4. kc чисто стартовал: увидел лидера 10001 **на том же epoch 62124** (выборов не было),
   догнал снапшот и лог. Финал: `describe --status` — LeaderId 10001, epoch 62124,
   **MaxFollowerLag=0**, voters/observers 3+3; URP=0 на брокерах, BrokerState=3.

Триггер события 23:12 08.09 не установлен (утром 08.15 инстанс кто-то рециклил через
облако + перерендер — до нашего вмешательства). Если повторится — смотреть события
storage/минионов kc (пара CORRUPT-дисков от 05.09 на srvk7162 намекает на недавние
перекладки).

## Грабли / приёмы

- **Замороженный metadata-лог одного voter'а даёт ту же картину, что voters-drift**
  (REJECTED, растущий epoch, «No controller appears to be active»), но конфиги чистые.
  Быстрый дифференциатор: `du -sh /mnt/data/metadata` + mtime файлов по всем
  контроллерам (2-3 команды) — замороженный узел виден сразу.
- `lastCaughtUpTimestamp=-1` у voter'а в voterStates лидера = фолловер не догонял никогда.
- tcpdump — решающий аргумент «данные доходят до ядра, но не аппендятся процессом»:
  значит битое in-memory/локальное состояние JVM, лечится только чистым стартом.
- **Стоп битого voter'а — первый шаг**: мгновенно восстанавливает кворум 2/3 и работу
  кластера ещё до пересоздания дисков; вайп делать уже в спокойном состоянии.
- `kafka-metadata-quorum.sh` в 3.8: `--bootstrap-server` и `--command-config` ДО
  подкоманды `describe`.
- После delete volumes проверять `tool_status` на предмет старых комплектов дисков
  (могут остаться с прошлых миграций и быть CORRUPT) — иначе start навсегда висит в
  «not ready to mount» без внятных ошибок в главном логе.
- `echo ===X` в zsh ломается (`=word` expansion) — только в кавычках.

## Связанные разборы

- MDBSUP-4970 (ads-kafka) — то же лечение (stop → delete volumes → start kc), но корень
  там был voters-drift; здесь конфиги чистые, корень — локальное состояние диска/процесса.
- 2026-09-08-mass-quorum-voters-drift-wave2.md — adb-users числился в хвостах
  `voters_dead`, но механика оказалась другой (не drift, а заморозка лога).
