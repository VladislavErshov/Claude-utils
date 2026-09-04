# MDBSUP-5093: leadads — kc-контроллер удалён, но остался voter'ом KRaft-кворума (NOT_LEADER_OR_FOLLOWER на учениях)

**Дата:** 2026-09-03
**Кластер:** `leadads` (adtech, f8bc73f0-667b-4f3d-8e08-c91dede5fa86; Kafka 3.8, KRaft, статические voters;
брокеры hc/pc/uc, контроллеры hc=10001/pc=12001/uc=13001, layout=hc,kc,pc,uc → kc=11001; namespace infra)
**Тикет:** MDBSUP-5093 (аналог: MDBSUP-4970, okfeed MDBSUP-5063 — третий случай одного паттерна)

## Суть

31.08 операция `delete_hosts` (`642354b4`, done 20:50) удалила kc-контроллер 11001 из облака
и host_state, но конфиги на pc/uc не перерендерились — они остались на старом 4-voter листе.
hc (10001) — единственный с правильным 3-voter листом.

## Симптомы

- WARN `Error connecting to node ... 11001` в kafka-controller.out.log на pc (1209/день) и uc (2580/день), на hc — 0.
- Учения 03.09 (изоляция ДЦ) → NOT_LEADER_OR_FOLLOWER: pc+uc ждут мажорити 3 из 4, hc — 2 из 3, консенсуса нет.
- `kafka-metadata-quorum describe --status` через брокер: `CurrentVoters: [10001,12001,11001,13001]`,
  лидер 13001 (uc — как раз со старым конфигом, кворум наследует лист лидера).

## Диагностика (без правок)

1. Прод-БД: операция `done`, kc отсутствует в host_state, RUNNING-операций нет → править БД не надо.
2. PMS `kafka.controller.quorum` уже правильный (`10001@hc,12001@pc,13001@uc`) — **сначала PMS, потом хосты**.
3. Конфиги на хостах: сверить `controller.quorum.voters` всех контроллеров + `broker.properties` всех брокеров.
   Здесь брокеры — все правильные, рассинхрон только pc/uc.

## Починка (03.09 14:39–14:45)

1. pc (12001): `confp --oneshot` (rc=0, конфиг стал 3-voter) → `systemctl restart kafka-controller` →
   Kafka Server started, без `must be included in the set of voters` (12001 есть в новом листе).
2. uc (13001, лидер): то же → рестарт вызвал перевыборы, новым лидером стал 10001 (hc), epoch 265772→265773.
3. rscheck@kafka на pc держал старый voter-лист в памяти (спам «Не удалось подключиться к 11001»
   при уже правильном /etc/rscheck/kafka.conf) → `systemctl restart rscheck@kafka` на pc и uc.
4. Верификация: `CurrentVoters: [10001,12001,13001]`, URP=0, CurrentObservers все 3 брокера,
   свежих WARN 11001 / heartbeat-ошибок нет.

## Грабли

- Паттерн повторяется третий раз → см. [MDBSUP-4970](MDBSUP-4970-2026-09-02-ads-kafka-quorum-voters-mismatch.md):
  delete_hosts/миграция недоведена до конца на всех хостах роли; PMS может быть уже правильным.
- Лидер кворума со старым конфигом протаскивает старый voter-лист в metadata log
  (`CurrentVoters` в quorum describe = лист лидера) — сверять конфиги, а не только метадату.
- rscheck@kafka кэширует voter-лист в памяти: после confp на хосте рестартовать rscheck,
  иначе ложный «Не удалось подключиться» / «Broker is dead».
- `kafka-metadata-quorum.sh` на контроллере: `localhost:9093` падает на SSL (нет SAN=localhost) —
  использовать `$(hostname -f):9093` + `--command-config /opt/kafka/config/client.properties`;
  надёжнее ходить через брокера `:9092`.

## Заглушка тикета

`jira-mdbsup-solver/history/MDBSUP-5093-2026-09-03.md`
