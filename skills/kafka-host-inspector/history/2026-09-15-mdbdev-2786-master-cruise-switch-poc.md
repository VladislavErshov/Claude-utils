# MDBDEV-2786: свитч Cruise Control на чужой кластер (мастер-круиз PoC) — механика на хостах

2026-09-15. Полный отчёт и замеры: `~/.paiw/projects/personal/mdb-mdbdev-2786-kafka-balancers/.learnings/2026-09-15-master-cruise-switch-poc.md`.
Здесь — хостовая механика.

## Переключение CC cruise-хоста на другой кластер

1. `systemctl stop cruise-control` на cruise-хосте цели (эксклюзив: два CC на кластер нельзя).
2. PMS `kafka.cruisecontrol.properties` на ключе `<свой>.clouds`: меняется ТОЛЬКО `bootstrap.servers`
   (diff конфигов кластеров = одна строка). update.do → verify байт-в-байт.
3. На мастер-хосте `confp --oneshot` — НО он отрендерит jaas/capacity от СВОЕГО кластера:
   - jaas: пароль из vault по контексту хоста → SASL fail у чужих брокеров;
   - capacity.json: per-cluster (DISK/NW различаются).
   Оба файла скопировать с cruise-хоста цели (отрендеренные): `cat /tmp/x > /opt/cruise-control/config/...`
   (cat поверх сохраняет owner/perms).
4. `systemctl restart cruise-control`.

Замер разогрева: detached-скрипт (setsid nohup + sleep 5), лог /tmp/warmup*.log,
poll `/state?substates=monitor,analyzer` до `state: RUNNING` + `isProposalReady: true`.
Результат (4 брокера/76 партиций, sample store цели прогрет): **45–50 с до proposal**;
на горячем CC proposals/dry-run = 77–126 мс.

## Грабли

- `mcc scp` на cruise-хосты (пример: test-modify3, ic) молча НЕ заливает файлы — заливать
  base64-чанком через sshexec (см. mcc-host-worker scp.md). Повтор грабли из kbbalance (2026-09-14).
- `pkill -f warmup2.sh` в sshexec убивает САМУ sshexec-сессию (exit 143) — паттерн в собственной
  cmdline. Ломать само-матч: `pkill -f "warmup[2].sh"`.
- Демо-дисбаланс для теста executor'а: kafka-reassign не даёт плана (TopicReplicaDistribution
  threshold 3×avg) — использовать `POST /demote_broker?brokerid=X&dryrun=false` (реальные
  лидер-мувы), затем `POST /rebalance?dryrun=false&exclude_recently_demoted_brokers=false`.
- Долгие замер-скрипты — только детачем (setsid nohup ... & sleep 5), иначе умирают с сессией.
- После CC executions проверить троттлы: `kafka-configs --describe --entity-type brokers --entity-name N`
  + topic; после корректного завершения CC чистит сам (проверено — чисто).
- Sample store кластера (`__KafkaCruiseControlPartitionMetricSamples`/`ModelTrainingSamples`) живёт
  В кластере: новый CC-хост/свитч подхватывает его мгновенно. «Холодный» тест = кластер без
  CC-истории, а не новый cruise-хост.
