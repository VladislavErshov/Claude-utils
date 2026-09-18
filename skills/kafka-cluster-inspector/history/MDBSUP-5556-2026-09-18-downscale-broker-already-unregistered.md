# MDBSUP-5556 — frontlogs2-adtech-kafka: downscale-broker завис на unregisterBroker уже deregisterнутого брокера (18.09.2026)

Повтор паттерна [MDBSUP-5092](MDBSUP-5092-2026-09-03-downscale-broker-isfresh-precondition.md)
(operator-флоу, шаг `get_kafka_downscale_broker_result`) — читать канон там. Здесь только
отличия этого кейса.

**Кластер:** frontlogs2-adtech-kafka (`a12b488e-3584-4a0e-bdf5-983be4e29ed0`),
25 брокеров hc/kc/pc/rc/uc × 5, fullQueue `frontlogs2-adtech-kafka.adtech.db.production.mdb.prod`,
cruise в rc. Контекст: 16.09 юзеры прогоняли remove_broker 21001–21005 через CC
([MDBSUP-5485](MDBSUP-5485-2026-09-16-frontlogs2-cc-cpu-util-migrate.md)); в этот раз —
mdb-операция delete_hosts на 21005 (5.kc).

## Отличия от 5092

- **Вариант зависания другой:** precondition'ы healthy (`Cluster is AVAILABLE`,
  `Operator is fresh / ready for actions`), задача стояла на экшене
  `kafka.downscale-broker[unregisterBroker]` (последний вызов за 2.5 часа до разбора,
  дальше не ретраила). `BrokerIdNotRegisteredException` был **корректен**: брокер 21005
  уже отсутствовал в метаданных (24 брокера), инстанс 5.broker...kc уже удалён из облака
  (mcc instances с `-c kc`), storage-спека брокера уже на шарды 1-4, host_state-строки
  в прод-БД нет. KRaft deregisterнул брокер сам после удаления инстанса — как в ручном
  флоу MDBSUP-5335 («unregister после stop+дрейна — норма»).
- **Withdraw не понадобился вовсе** — вся облачная часть уже была сделана до стака.
  Осталось только снять задачу и закрыть операцию.
- **Самосхождения не случилось, в отличие от 5092.** После op_stop get_result отработал
  и перевёл операцию `in_progress → failed` (error «Operator task ... in progress»),
  attempts_left=0 — припаркована. Закрытие SQL:
  `UPDATE operations SET status='done', in_processing=false, finished_ts=now(),
  error_message=NULL WHERE id='…' AND status='failed';`
  Вывод: при attempts_left=0 считать «операция сойдётся сама после op_stop» нельзя —
  проверять статус через ~30 сек и закрывать SQL.

## Диагностика (сжатый чек-лист этого варианта)

1. Прод-БД: `operations` (status/attempts_left/in_processing) + отсутствие host_state-строки.
2. `mcc --local -n infra -c kc ops "queue://<fullQueue>" -f json` →
   `tasks["downscale-broker"]`: args, последний `invoked`, `serviceWithdrawn/storageWithdrawn`.
3. Список брокеров: `kafka-broker-api-versions.sh … | grep -oE 'id: [0-9]+'` — есть ли
   целевой brokerId в метаданных.
4. Drain: цикл `kafka-topics.sh --describe --topic <t>` по всем топикам, grep brokerId
   (101 топик → 0 партиций). Плюс `kafka-reassign-partitions.sh --list` — активных движений нет.
5. Облако: `mcc -c <dc> instances '*<cluster>*'` — инстанс удалён?

## Грабли

- **`mcc sshexec` печатает `*** Connection closed by remote host ***` ПОСЛЕ успешного
  вывода** — строка-шум в хвосте; не считать признаком неудачи команды (здесь на всех
  брокерах этого кластера), выходной текст валиден. Сломал первый цикл ретраев.
- `mcc instances`/`sshexec` «No instances found» без `-c <dc>` — повтор грабли 5335:
  проверять инстансы только с явным `-c kc`.
- Kafka CLI: полный путь `/opt/kafka/bin/*.sh`, `--command-config /opt/kafka/config/client.properties`,
  FQDN-bootstrap (канон 5485, этот же кластер).

## Хвост

- **Баг оператора (кандидат в MDBDEV):** `DownscaleKafkaBrokerTask.unregisterBroker`
  не считает BrokerIdNotRegistered успехом — задача паркуется навсегда, операция блокируется.
  Это уже 4-й кейс семейства downscale-broker: 4895/4899 (ghost-хосты), 5092 (isFresh), 5556.
- **URP=18 в топике `api`** (18 из 41 партиций; из ISR выпадают реплики на разных брокерах
  pc/rc/hc/uc, лидеры uc/hc) — стабильно в течение наблюдения, к 21005 отношения не имеет
  (на нём 0 партиций), reassignments нет. Причина не копалась — отдельный разбор при
  следующем обращении по кластеру.
