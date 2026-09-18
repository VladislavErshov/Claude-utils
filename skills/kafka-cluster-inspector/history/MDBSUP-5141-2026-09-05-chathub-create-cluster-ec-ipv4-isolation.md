# MDBSUP-5141 (2026-09-05, КЕЙС ОТКРЫТ — ждём фикса сети) — chathub: create_cluster упала, ec-хост изолирован по IPv4

> **Дополнение 2026-09-07:** изоляция не снята (проверено с chathub-ec — v4 до kc всё ещё FAIL).
> Третий случай того же инцидента — MDBSUP-5154: ec-контроллер `kafka-news-snapshots` (13001,
> добавлен 04.09, тот же минион srve4515) в candidate после delete_hosts hc-контроллера,
> конфиг voters корректный. Эскалация в поддержку облака продублирована в тикете 5154.
> Эскалация переведена в тикет **ONESUP-972** (в работе, исполнитель Пантелеев); 07.09
> предоставлены детали: v4 TCP на любой порт ec↔kc/pc в обе стороны, v6 OK, ICMP непоказателен,
> пропало 04.09 с размещением на legacy-минионе srve4515. Свежий тест chathub-ec (10.210.189.6):
> v4 kc/pc FAIL, v6 kc/pc OK.

Кластер `chathub` (`fbc3e4b4-91f9-4f31-aad4-cca10d387093`, проект mdb10263, Kafka 3.8, ns infra,
fullQueue `chathub-mdb10263-kafka.mdb10263.db.production.mdb.prod`). Кластер создаётся — ни одной
успешной операции ещё не было. create_cluster `0f88a303-e690-4a1b-b11e-109692dfb61a` упал:
`Error while get_submit_kafka_broker_manifest: service is in STARTING UNAVAILABLE`.
Тикет: https://jira.vk.team/browse/MDBSUP-5141

Топология: контроллеры-вайтеры `10001@ec, 11001@kc, 12001@pc` (из controller.properties),
брокер ec = 20001. ec-брокер и ec-контроллер на одном минионе srve4515 (10.4.52.33, ДЦ M100).
Все брокеры перезапущены утром 05.09 (~10:44-10:49 МСК).

## Симптомы

- UI/облако: ec-контроллер RUNNING, но `candidate` («There are not enough controller count»);
  ec-брокер STARTING/UNAVAILABLE «Broker is dead»; kc/pc-контроллеры PREFAIL «minimum
  controller count»; cruise на pc — «He's dead, Jim» (отдельная проблема, к ec-v4 не относится).
- ec-брокер crash-loop с периодом ~65 с (systemd: 14:31:32 → 14:33:36 → 14:35:32 → …):
  `initial.broker.registration.timeout.ms=60` + Restart. В логе:
  `Disconnecting from node 11001 due to socket connection setup timeout`,
  `Unable to register the broker because the RPC got timed out before it could be sent`.
  Load average на минионе ~23 — следствие рестарт-лупа, не причина.
- ec-контроллер висит в `candidate`: не может достучаться до лидера (pc) и follower'а (kc).
- Кворум 2/3 держат pc (leader) + kc (follower) — кластер без резерва: потеря ещё одного
  контроллера = полный отказ.

## Корневая причина: сетевая изоляция ec-хостов по IPv4

- **v4 между ec ↔ kc/pc не ходит в обе стороны** (TCP на любой порт — дроп), **v6 работает**.
  kc ↔ pc — v4 и v6 ок. Loopback v4 на ec — ок (self:9093 отвечает).
- ICMP между хостами заблокирован политикой во всех ДЦ (100% loss даже на живых парах) —
  пинг не показателен.
- DNS отдаёт и A, и AAAA; Java по умолчанию предпочитает IPv4 (`preferIPv6Addresses` в
  /etc/sysconfig/kafka не выставлен) → все соединения KRaft с ec уходят в мёртвый v4.
  Отсюда candidate у контроллера и registration-timeout у брокера.
- Прецедент — **MDBSUP-5137 (накануне, 2026-09-04)**: новый ec-хост `preprod-vk-support-kafka`
  с той же v4-изоляцией. Похоже на один сетевой инцидент M100 с новыми ec-размещениями.

## Диагностика (воспроизведение за 5 минут)

1. `mcc instances "%<cluster>%" -f yaml -c <dc>` по всем ДЦ — state/minion/IP/availability.
2. Порты: `grep -E '^(listeners|controller.listener.names|controller.quorum.voters|node.id)'
   /opt/kafka/config/controller.properties` (конфиги в /opt/kafka/config, env в /etc/sysconfig/kafka).
3. Связность **раздельно по семействам**: `nc -z -w3 -4 <fqdn> 9093` и `nc -z -w3 -6 <fqdn> 9093`
   с контроллера каждого ДЦ. `/dev/tcp` и `getent` идут по v6 первым — без `-4` получишь
   ложное «связность есть» (тот же грабёж, что в MDBSUP-5137).
4. Лог брокера: `journalctl -u kafka-broker -n 20` (период фейлов) +
   `tail /mnt/logs/dbms/kafka-broker.out.log` (registration-timeout, socket setup timeout).

## Что делать дальше

1. Эскалация в поддержку облака: проверить v4-маршрутизацию/микросегментацию между M100 (ec)
   и MSK-ДЦ для новых ec-хостов. Со стороны Kafka не лечится (комментарий в тикете оставлен 05.09).
2. После появления v4: ec-брокер поднимется сам на ближайшем рестарте, ec-контроллер выйдет
   из candidate, кворум станет 3/3; далее перезапустить create_cluster через UI/API mdb.
3. Cruise «He's dead, Jim» разбирать отдельно — не связан с v4-изоляцией ec.

## Грабли

- Падение create_cluster с «service is in STARTING UNAVAILABLE» + «Broker is dead» в облаке
  не означает проблему Kafka: под этим симптомом может быть сетевая изоляция хоста.
- Быстрый (60-65 с) crash-loop «unable to register with the controller quorum» при живом
  кворуме = не проблема кворума, а недоступность пути broker→контроллеры (паттерн MDBSUP-5137).
- Пинг в этих ДЦ бесполезен (ICMP дропается всеми) — только TCP-пробы с указанием семейства.
- candidate у контроллера в UI = не может связаться с кворумом по v4: смотреть связность до
  leader/follower, а не перезапускать его.
