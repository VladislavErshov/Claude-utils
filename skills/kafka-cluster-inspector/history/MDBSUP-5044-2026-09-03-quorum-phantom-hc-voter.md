# MDBSUP-5044 (2026-09-03) — фантомный hc-voter в KRaft кворуме extdbpu

## Симптом

- `kafka-metadata-quorum describe --status`: `CurrentVoters: [10001, 12001, 11001, 13001]` — **4 voter'а**
- `10001` = `1.controller.extdbpu-oneme-kafka.hc.one-infra.ru` — **хост не существует** (кластер в ec/kc/pc),
  в логах контроллеров ~26k `UnknownHostException` (спам RaftManager)
- Фактически кластер работал на 3/4 voters — отказ любого живого контроллера ронял бы кворум
  (majority из 4 = 3, а живых было 3; рестарт одного → 2/4 → freeze metadata-операций)

## Корень

Кластер изначально размечался на 4 ДЦ: `kafka.layout = hc,kc,pc,ec` (отсюда и ID: hc=10xxx, kc=11xxx,
pc=12xxx, ec=13xxx). Контроллер в hc не создавался, но старые конфиги контроллеров ec и pc содержали
`controller.quorum.voters` из 4 записей с фантомным hc. PMS `kafka.controller.quorum` уже был исправлен
на 3 voters, но confp+рестарт в августе прогнали только на kc (1.controller.kc) — ec и pc остались со
старыми конфигами (датировка файлов на хосте сразу выдаёт рассинхрон: Aug 17/18 vs Aug 18 23:26).

## Фикс (проверено на проде extdbpu)

1. **PMS не трогать** — `kafka.controller.quorum` уже правильный (3 voters, ec/kc/pc).
   ⚠️ `kafka.layout = hc,kc,pc,ec` НЕ исправлять: node.id рендерится по позиции ДЦ в layout
   (`10000 + dc_pos*1000 + instance_id`) — удаление hc сдвинет позиции и ID всех контроллеров
   при следующем confp (катастрофа, см. I48592).
2. `confp --oneshot` на ec и pc → проверить `controller.quorum.voters` в controller.properties
   (стало 3, без hc). Первый прогон confp может упасть на vault-pki — повторить.
3. Рестарт **по одному**: сначала follower (pc), проверка кворума, затем бывший лидер (ec).
   Во время рестарта pc у лидера ec voter set был ещё «4» → 2/4 живых → краткосрочный freeze
   metadata-операций (~1-2 мин, produce на живых партициях не прерывается). После рестарта pc
   кворум сменил voter set на 3 (лидер стал kc, epoch 279→280) — рестарт ec уже безопасен.
4. Верификация: `CurrentVoters: [12001,11001,13001]`, `MaxFollowerLag: 0`, HW растёт,
   `Kafka Server started` на обоих, новых `UnknownHostException` — 0.

## Грабли

- `systemctl is-active kafka-controller` на контроллерах **врал** (failed при active running) —
  проверять через `systemctl status` / логи (совпадает с общим правилом: `mcc status` → sshexec).
- `describe --status` показывает CurrentVoters от кворума — с брокера, не с контроллера
  (на контроллере CLI до localhost не подключится: SAN без localhost; ходить по FQDN брокера,
  command-config = `/opt/kafka/config/client.properties`, `broker.properties` не годится — там
  CruiseControlMetricsReporter в metric.reporters и другой JAAS).
- `kafka-get-offsets.sh` пишет INFO-логи в **stdout** (AdminClientConfig с `retries = 2147483647`)
  — фильтровать `grep '^<topic>:'`, иначе awk-суммы мусорные; `printf "%d"` в awk клампит int32
  (сумма оффсетов ~2.6 млрд → ровно 2147483647).
- mcc `instances "<fqdn>"` для этого кластера даёт EntityNotFoundException — искать через
  `instances "*oneme*" --output_format json` + фильтр по queue в python/jq.
- Часть брокеров (follower'ы других ДЦ) может не иметь лидерства по нужному топику — логи
  авторизации смотреть на лидере конкретной партиции (id из `kafka-topics --describe`;
  21xxx=ec, 22xxx=kc, 23xxx=pc).
