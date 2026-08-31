# URP на всём кластере = один брокер: в uc снова не поднимаются НОВЫЕ соединения

**Кластер:** `logs-recom-wizard-kafka` (dzen, `idzn.ru`, ДЦ hc/kc/pc/uc)
**Дата:** обнаружено 2026-08-31 (проблема с 2026-08-29 13:00 МСК)
**Предыстория:** повтор инцидента 2026-08-25 —
[`kafka-host-inspector/history/ui-cc-504-dead-vip-uc-logs-recom-wizard-kafka.md`](../../kafka-host-inspector/history/ui-cc-504-dead-vip-uc-logs-recom-wizard-kafka.md)

## Симптом

- `UnderReplicatedPartitions = 9` (и `UnderMinIsrPartitionCount = 3`) на брокере
  `1.broker.logs-recom-wizard-kafka.uc.idzn.ru` = **23001**.
- Все 9 URP-партиций кластера ведёт 23001, у каждой `Isr: 23001` (схлопнут до лидера):
  `ok-games-features-proto-base64-log` P0/P3/P5, `vk-games-user-item-events-log` P2/P14,
  `ok-games-user-item-events-log` P9, `__consumer_offsets` P1/P3, `__CruiseControlMetrics` P3.
- Влияние: acks=all в 3 партиции ниже min ISR → `NotEnoughReplicas` (вкл. коммиты оффсетов
  групп на `__consumer_offsets` P1/P3).

## Broker ID → хост (dzen-неймспейс, отличаются от one-infra таблицы!)

hc=20001-20012, kc=21001-21012, pc=22001-22012, uc=23001-23010 (46 брокеров).
Проверять всегда через `kafka-broker-api-versions.sh`, не верить таблице из SKILL.md.

## Диагноз

1. **URP-метрика leader-side** — `curl localhost:7777/jolokia/read/kafka.server:type=ReplicaManager,name=UnderReplicatedPartitions`
   на каждом брокере: 9 только у 23001, у 23002-23010 нули → проблема локализована в одном брокере.
2. Лог kc-брокера (`ReplicaFetcherThread-0-23001`): `Disconnecting from node 23001 due to
   socket connection setup timeout` — **с 2026-08-29 13:00:44**, непрерывно до сих пор.
   Фолловеры из kc/pc/hc не могут установить соединение к 23001:9092 → не входят в ISR.
3. Сетевые пробы (kc→uc и pc→uc, `/dev/tcp` по FQDN): **FAIL для ВСЕХ опрошенных uc-брокеров**
   (1/2/5/10), причём по обоим адресам — мёртвый VIP `10.189.121.x` и mesh `fd00::`.
   Исходящие из uc (uc→kc/pc/hc:9092) — OK. 23001 локально здоров (LeaderCount=9,
   PartitionCount=23, сам фолловер в ISR у лидера 21012).
4. Ключевая развилка «почему URP только у 23001, если новые коннекты в uc мертвы для всех»:
   брокеры 23002-23010 живут на **давно установленных** соединениях (репликация идёт, URP=0);
   у 23001 фолловер-коннекты оборвались 29.08 13:00 и заново не поднимаются. Сеть uc
   «половинчато мёртвая»: старые флоу пропускает, новые (handshake) — нет.
5. kafka-broker на 23001 рестартовал 2026-08-29 04:13 МСК — ДО обрыва (04:13→13:00 репликация
   работала), т.е. рестарт не триггер.

## Вывод

Сетевая проблема инфраструктуры uc (VIP `10.189.121.x` + mesh, новые входящие на 9092),
повтор инцидента 25.08 (тогда мёртвые `10.189.121.43/63/64`; `.43` = 23001 — снова он).
Эскалация сетевикам (Денис Селюцкий, 31.08, текст: диагноз + тайминг + VIP-диапазон).

## Что делать / чего НЕ делать

- **НЕ рестартовать** kafka-broker на uc-брокерах: 23002+ держатся на установленных
  соединениях, рестарт их убьёт и заново не поднимет → массовый URP/offline.
- После починки сети ISR восстанавливается сам (фетчеры догоняют, лидер расширяет ISR).
- Побочка: лидерства стекли с uc в другие ДЦ (9 из 360 партиций led by uc) — после
  починки прогнать preferred leader election / rebalance CC.

## Грабли диагностики

- `mcc sshexec -n dzen` рвёт соединение на длинных командах (kafka-topics --describe) —
  писать скрипт в файл через expect+heredoc (`cat > /tmp/x.sh << "EOF"`) и запускать.
- Tcl/expect: `[a-z0-9]` в grep-шаблоне = `invalid command name "a-z0-9"` — избегать
  квадратных скобок (заменять на `...`/cut/awk без классов).
- `kafka-topics.sh --describe` сыплет ~40 строк AdminClient-конфига — фильтровать
  `grep Partition:`, иначе head/tail съедает результат.
- URP-картину снимать per-broker через Jolokia (быстро), полноту проверять
  `--describe --under-replicated-partitions` (может терять темы при таймаутах AdminClient).
- bash `/dev/tcp $FQDN/9092` использует ПЕРВЫЙ адрес из getent — для проверки конкретного
  пути резолвить все IP и тестировать каждый отдельно.
