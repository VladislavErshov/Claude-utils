# ml-platform-test-mops-kafka — хронический URP из-за удалённого брокера 22001 (dzen)

**Дата**: 2026-08-27
**Кластер**: `ml-platform-test-mops-kafka` (namespace **dzen**, idzn.ru) — брокеры kc/pc/rc,
контроллеры kc/pc/uc, CC в kc. UI mdb-data показывает брокеров как `observer` — норма KRaft.
**Тикета нет**, разбор по запросу в чате.

## Симптом

На всех 3 брокерах висят under-replicated partitions (kc=6, pc=5, rc=3), «зависли» намертво:
`IsrShrinksPerSec Count=0`, `ReassigningPartitions=0`, quorum здоров (describe --replication:
Leader 10001, фолловеры 11001/12001, lag=0).

## Диагноз (паттерн MDBSUP-4166, см. kafka-cluster-inspector/known_issues.md)

`kafka-topics --describe --under-replicated-partitions`: 14 партиций топиков `training.*`, у всех
в `Replicas` есть брокер **22001, которого нет среди зарегистрированных** (registered: 20001=kc,
21001=pc, 23001=rc; voters: 10001=kc, 11001=pc, 12001=uc). 22001 — бывший uc-брокер (нумерация по
позиции ДЦ в kafka.layout: uc → 22001 для брокеров, 12001 для контроллеров). Брокер из uc удалён
из кластера (остался только uc-контроллер), а reassign его партиций не прошёл/не запускался.
Незарегистрированный брокер никогда не вернётся в ISR сам → URP перманентный.

## Фикс

Генератор на живых данных (`kafka-topics --under-replicated-partitions` → replace 22001 на
недостающий из {20001,21001,23001}, порядок/preferred leader сохранены, итог — 1 реплика на ДЦ):
`--execute` **без throttle** → все 14 `completed` → URP=0 на всех брокерах, `22001` в Replicas
больше нигде нет. Rollback-assignment (старое состояние с 22001) — в выводе execute.

## Специфика dzen-namespace (idzn.ru)

- mcc: `-n dzen`, облако указывать обязательно: `mcc --local -n dzen -c <dc> instances ...`
  (ДЦ = kc/pc/rc/uc; перечисление без `-c` ищет только в default cloud).
- Имена инстансов в mcc — **без `.wan`**: `1.broker.<cluster>.kc.idzn.ru`, хотя UI/пользователь
  даёт `...kc.wan.idzn.ru`. Для bootstrap-server Kafka тоже работает FQDN без `.wan`.
- Заливка скрипта через `sshexec` одним base64-чанком (~2KB) — ок; лимит ~8KB (414 URI Too Long).
- `sudo -u kafka` для kafka-topics/reassign работает без source /etc/sysconfig/kafka.
- `--describe --under-replicated-partitions` на dzen-хостах отдаёт строки с ведущей табуляцией —
  в regex парсера заложен `^\s*Topic:`.

## Грабли

- `mcc sshexec` на cruise-хост упал `Container main ... is not found` — CC-диагностика через
  `/state` в этот раз недоступна; для кейса не понадобилась (Reassigning=0 и так видно).
