# MDBSUP-4170: Рассинхрон controller.quorum.voters → падение kc controller + ec broker

**Дата**: 2026-07-24
**Кластер**: `sms-gate-dev-vkcm-kafka` (Kafka 4.x, KRaft, 3 controller + 3 broker: hc/kc/pc/ec)
**Референс**: INCALL-42685 (аналогичный кейс на `kafka-spd-adtech-kafka`)

## Симптом

В UI mdb-data два хоста в статусе `unknown`:
```
1.controller.sms-gate-dev-vkcm-kafka.kc.one-infra.ru    unknown
1.controller.sms-gate-dev-vkcm-kafka.pc.one-infra.ru    AVAILABLE  voted
1.controller.sms-gate-dev-vkcm-kafka.ec.one-infra.ru    AVAILABLE  candidate
1.broker.sms-gate-dev-vkcm-kafka.kc.one-infra.ru        AVAILABLE  unattached
1.broker.sms-gate-dev-vkcm-kafka.pc.one-infra.ru        AVAILABLE  unattached
1.broker.sms-gate-dev-vkcm-kafka.ec.one-infra.ru        unknown
```

Broker-хосты `unattached` — **норма** для broker-роли в KRaft (не voters, регистрируются в quorum).

`host_checker` на проблемных хостах шлёт `{"status":"UNAVAILABLE","role":"UNKNOWN"}` с ошибкой:
```
ERROR checks.check_kafka <urlopen error [Errno 111] Connection refused>
```

## Причина

Рассинхрон `controller.quorum.voters` при выводе controller-хоста `hc` (node.id=10001) из кластера.

**PMS на broker-ключе `sms-gate-dev-vkcm-kafka.clouds`** (читается через `pms-read.sh <broker-host> kafka.controller.quorum`):
```
13001@1.controller...ec:9093,11001@1.controller...kc:9093,12001@1.controller...pc:9093
```
3 voters, без 10001 — корректно.

**PMS на controller-ключе `controller.sms-gate-dev-vkcm-kafka.clouds`** — `kafka.controller.quorum=<NOT_SET>`, `kafka.layout=<NOT_SET>`. То есть voters на controllers рендерится не из этих PMS-переменных, а подтягивается с broker-ключа во время modify-флоу.

**Файлы `controller.properties` на хостах** — рассинхрон:

| Хост | `controller.quorum.voters` |
|---|---|
| `ctrl-kc` (11001) | `13001@ec, 11001@kc, 12001@pc` (3 voters, без 10001) ✓ |
| `ctrl-pc` (12001) | `10001@hc, 11001@kc, 12001@pc, 13001@ec` (4 voters, **со старым 10001@hc**) ✗ |
| `ctrl-ec` (13001) | `10001@hc, 11001@kc, 12001@pc, 13001@ec` (4 voters, **со старым 10001@hc**) ✗ |
| `broker-kc/pc/ec` | `13001@ec, 11001@kc, 12001@pc` (3 voters) ✓ |

На brokers и kc controller конфиги перерендерились, на pc/ec controllers — нет.

## Цепочка падений

1. `kc controller` (11001) — `kafka-controller.service` циклически падает при старте:
   ```
   java.lang.IllegalStateException: Leader 10001 must be in the voter set
   VoterSet(voters={11001=kc, 12001=pc, 13001=ec})
     at org.apache.kafka.raft.QuorumState.initialize
   ```
   В **локальном meta-log kc** (`/mnt/data/metadata/__cluster_metadata-0/`) остался recorded leader = 10001, которого нет в текущем voter set kc (3 voters).
   ⚠️ meta-log лежит в `/mnt/data/metadata/`, **не** в `/mnt/data/log/` (хотя `log.dirs=/mnt/data/log`).

2. `ec broker` (23001) — падает с `CancellationException while waiting for the controller to acknowledge that we are caught up`. В логе:
   ```
   Connection to node 11001 (...kc:9093) could not be established. Node may not be available.
   ```
   Брокер ломится к мёртвому kc controller → падает. **Следствие**, не самостоятельная проблема.

3. pc и ec controllers работали (voted/candidate) — кворум 2/3, кластер жив. У них 10001 в voters, но `10001@hc` не резолвится DNS-ом (`UnknownHostException: 1.controller...hc.one-infra.ru`) → 10001 никогда не становился лидером → meta-log pc/ec не содержит записи "leader=10001".

## Фикс

**Этап 1** — `confp --oneshot` на pc и ec controllers (перерендерить `controller.properties` из PMS):
```bash
# на pc и ec:
cp /opt/kafka/config/controller.properties /tmp/controller.properties.before
confp --oneshot
grep controller.quorum.voters /opt/kafka/config/controller.properties
# → 13001@ec,11001@kc,12001@pc (3 voters, без 10001)
```

**Этап 2** — `systemctl restart kafka-controller` на pc, затем ec. Они переизбирают лидера (становится pc=12001), обновляют meta-log без 10001.

**Этап 3** — на kc: `systemctl stop kafka-controller` + удалить локальный meta-log + `systemctl start`:
```bash
systemctl stop kafka-controller.service
mv /mnt/data/metadata/__cluster_metadata-0 /mnt/data/metadata/__cluster_metadata-0.bak
systemctl start kafka-controller.service
# kc скачивает snapshot с лидера (pc=12001) → стартует как follower
```

**Этап 4** — `confp --oneshot` + `systemctl restart kafka-broker` на ec broker. Подключается к кворуму, регистрируется.

## Грабли

1. `mcc ssh` **не принимает command как аргумент** — только через `expect -c '...spawn mcc --local ssh <host>; expect "/# "; send "..."; expect "===DONE==="; send "exit\r"'`.
2. `mcc ssh` периодически падает с `SSL Handshake is not finished` / `Too early` — повторить через 5 сек.
3. Хост kc **временно потерял сеть** (ping 100% loss, порты 22/9093 timeout, `mcc ssh` → `Container not found` / `not scheduling on a minion`). Восстановился сам через ~5 минут. Не KRaft-проблема, а инфраструктурная.
4. tcl/expect **не разворачивает `$(date +%s)`** в `send` — нужно использовать фиксированный суффикс или `expect`-экранирование.
5. `meta-log` на controller-хостах лежит в **`/mnt/data/metadata/__cluster_metadata-0/`**, **не** в `log.dirs=/mnt/data/log`. В `/mnt/data/log/` только `bootstrap.checkpoint` и `meta.properties`.
6. PMS на controller-ключе `controller.<cluster>.clouds` почти пустой — `kafka.controller.quorum=<NOT_SET>`, `kafka.layout=<NOT_SET>`. Voters на controllers рендерится из broker-ключа `<cluster>.clouds` во время modify-флоу. Проверять через `pms-read.sh <broker-host> kafka.controller.quorum`.
7. `kafka-metadata-quorum.sh` падал с `OutOfMemoryError: Java heap space` (heap утилиты маловат). Не использовать для диагностики на этих хостах — смотреть логи напрямую.
8. **`host_checker` отправляет `UNAVAILABLE/UNKNOWN`** в mdb-health при `Connection refused` к JMX/Jolokia-порту, даже если Kafka-сервис `active` в systemd. То есть `unknown` в UI ≠ упавший сервис. Проверять через `tail /mnt/logs/system/host-checker.log` и `systemctl status kafka-*.service`.
9. **`systemctl is-active` может вернуть `active` когда сервис уже падает** — проверять `systemctl status` (visible `Active: failed`) и `ps -ef | grep kafka`.

## Backup

На kc оставлен `/mnt/data/metadata/__cluster_metadata-0.bak` — удалить после пары дней стабильной работы.
