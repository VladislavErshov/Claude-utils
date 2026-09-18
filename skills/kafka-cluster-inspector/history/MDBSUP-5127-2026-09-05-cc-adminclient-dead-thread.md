# MDBSUP-5127 (2026-09-05) — preprod-vk-support-kafka: тикет «CC unavailable»; CC уже восстановлен рестартом; AdminClient-поток умер разово после июльских OOM. ec-часть = MDBSUP-5137

Кластер `preprod-vk-support-kafka` (`e67f499f-6b72-4234-bf33-aef81c7b8476`, ns vk-support,
домен one-infra), cruise-хост `1.cruise.preprod-vk-support-kafka.kc.one-infra.ru`.
Тикет: https://jira.vk.team/browse/MDBSUP-5127 (04.09 13:46, заявитель n.tolstov).

## CC-часть (суть тикета) — уже неактуальна на момент разбора

- Тикет 04.09 13:46: CC UNAVAILABLE, Proposal not ready, циклический
  «TimeoutException: The AdminClient thread has exited», память инстанса 89%.
- **CC перезапущен 04.09 13:50:16** (через 4 мин после тикета, руками в треде поддержки) —
  с тех пор healthy: `/state` → isProposalReady=true, все goals ready,
  monitoringCoveragePct=100%, monitoredWindows по 1.0, host_checker получает 200 каждые 5 с.
- Конфиг из тикета валиден: bootstrap.servers = все 3 брокера (hc/kc/pc),
  security.protocol=SASL_SSL — гипотеза про PMS не подтвердилась.
- «Memory 89%» — RSS JVM (Xmx1024m) на инстансе alloc 2048MB, не проблема.

## Паттерн: циклический «AdminClient thread has exited» в cruise-control.out.log

- Источник — BrokerFailureDetector (`KafkaBrokerFailureDetector.aliveBrokers`):
  после смерти AdminClient-потока CC **не пересоздаёт его** → WARN + stacktrace каждые
  5 минут бесконечно. У нас: ~05.08 → 04.09 (~8.7k строк стектрейсов).
- Разовый триггер смерти потока — вероятно июльские OOM: `cruise-control.err.log`
  полон `OutOfMemoryError` (HTTP-Dispatcher, GoalOptimizerExecutor, SampleStoreProducer),
  err.log замолкает с 21.07.
- **Лечение: только рестарт cruise-control.** Само не проходит, конфиг не виноват.
- Грабля подсчёта: timestamped-строк с «TimeoutException» почти нет — продолжения
  стектрейса пишутся без timestamp. Считать по WARN
  «Broker failure detector received exception».

## ec-часть (failed add_hosts) — не дублирую

Операция `5b4f76bd-2f02-47ec-a3b5-e74bf8c27ce2` (та же, что в тикете) и корневая причина —
v4-изоляция нового ec-хоста: полный разбор в
[MDBSUP-5137](MDBSUP-5137-2026-09-04-new-ec-host-ipv4-isolation.md) (тот же кластер,
тот же хост, та же операция) и паттерн по M100 —
[MDBSUP-5141](MDBSUP-5141-2026-09-05-chathub-create-cluster-ec-ipv4-isolation.md).

Дополнения 05.09:
- v4 по-прежнему закрыт; onecloud зациклил редеплой ec-инстанса
  (DEPLOYING → STARTING → kafka-broker fail через ~65 с), v6 ec→контроллеры OK —
  подтверждает 5137.
- Хвост: у операции `in_processing=t`, `finished_ts=NULL`, attempts_left=0 —
  заблокирует новые операции. SQL-фикс (in_processing=false + finished_ts)
  предложен, **не применён** — автор тикета забрал это из комментария в тикет
  (внутренняя кухня), решение отложено.
