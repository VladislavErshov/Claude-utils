# 2026-09-04: массовая чистка фантомных voter'ов (49+2 кластера, mismatch)

Массовая починка `kafka_controller_quorum_voters_mismatch` по всем продовым Kafka-кластерам
mdb-data. Процедура — MDBSUP-5044, применена 51 раз за один день без единого сбоя.
У первоисточника см. [MDBSUP-5044](MDBSUP-5044-2026-09-03-quorum-phantom-hc-voter.md).

## Паттерн

Все кластеры одинаковы: контроллер выводили (обычно hc при миграции layout hc,kc,pc,uc →
kc,pc,uc), PMS `kafka.controller.quorum` обновили, но у 2 из 3 живых контроллеров остался
старый отрендеренный `controller.quorum.voters` с фантомом → живой кворум держит 4 voter'а
при 3 контроллерах (MaxFollowerLag = весь лог у фантома). Сценарий-бомба MDBSUP-4970:
failover на узел с рассинхроном → фенсинг брокеров.

## Процедура (скрипт `~/Documents/utils/fix_phantom.sh` + обёртка `fix_by_pms.sh`)

1. PMS сверить, НЕ править (`kafka.controller.quorum` правильный; `kafka.layout` не трогать
   — сдвиг node.id, I48592).
2. `grep controller.quorum.voters` на каждом `1.controller.<queue>.<dc>` → confp расходившимся.
3. Лидер через `kafka-metadata-quorum describe --status` с брокера (FQDN брокера + 
   `/opt/kafka/config/client.properties`; localhost не в SAN, `kafka-metadata-quorum.sh` — 
   полный путь `/opt/kafka/bin/`).
4. Рестарт follower'а → verify voters=PMS, lag=0 → рестарт лидера → verify. rscheck@kafka
   рестартовать на тронутых хостах.

## Результат

- 51 кластер (49 из списка + пропущенные billing-logs, logs): mismatch 0, `_dead` фантомных
  тоже погасли (sms-gate-prod, sms-history-prod, oneme-aqa-kafka, vk-ucp-video-match).
- mdbdev (test-modify3/4, test-downgrade7) не трогали по решению владельца.
- Верификация после циклов чека mdb-health: в `warnings.cluster_warnings` mismatch вне
  mdbdev — 0 строк; по выборке починенных кластеров quorum-варнингов нет вообще.

## Скрипты

- `scripts/fix_phantom.sh <queue> <dc...>` — основной цикл (confp → follower → verify →
  leader → verify → rscheck).
- `scripts/fix_by_pms.sh <queue>` — обёртка: ДЦ живых контроллеров из PMS-кворума.
- Рабочие копии: `~/Documents/utils/`. Для MDBSUP: брать из скилла
  `kafka-cluster-inspector/scripts/`.

## Грабли, добавленные к 5044

- **Скрипт считает 1 контроллер на ДЦ.** `logs-adtech-kafka` (proj 55) имел 2 контроллера
  в hc (10001+10002, PMS-лист из 5 voter'ов) и фантома 11002 без хоста. Скрипт не увидел
  `2.controller.*`: «файл уже верный» у 1.controller.hc не значит, что процесс лидера
  (2.controller.hc) не держит старый набор в памяти. Лечится вручную: confp лидеру,
  рестарт НЕрестартованных follower'ов (файл чистый ≠ процесс чистый), затем лидер.
- ДЦ живых контроллеров надёжнее выводить из PMS-кворума, а не из host_state
  (там зомби-записи: у ads-kafka-hdhc hc-контроллера в облаке нет, а host_state есть).
- После вывода контроллера хост может остаться в host_state (зомби) — UI показывает
  мёртвый хост; уборка отдельным действием (ads-kafka-hdd, возможно другие).
- `echo ====` в zsh под sshexec-циклом интерпретируется как глоб — квотить.
