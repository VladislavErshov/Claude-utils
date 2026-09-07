# 2026-09-07: Сетевая доступность events-front-kafka × comments-social-kafka (PC/UC/HC)

## Задача

Проверить сетевую доступность между двумя кластерами Kafka в комбинациях
PC→UC, PC→HC, UC→PC, UC→HC (telnet 9092 v4/v6, ping v4/v6, traceroute TCP 9092).

Ссылки UI:
- https://one.vk.team/mdb/?path=front/kafka/6e9d061a-e0db-42ae-89a6-3b163ed88fef → кластер **events**, алиас `events-front-kafka`
- https://one.vk.team/mdb/?path=social/kafka/a8ac8c98-c018-4d24-b3dd-3c01b2ebdf4e → кластер **comments**, алиас `comments-social-kafka`

Оба в mcc-неймспейсе **dzen** (домен `idzn.ru`), хосты:
- `events-front-kafka`: брокеры 1–8 pc, 1–8 uc, 1–12 hc; контроллеры pc/uc/hc
- `comments-social-kafka`: по 1 брокеру в pc/uc/hc/kc/dc; контроллеры pc/uc/hc

Проверки: `mcc --local sshexec -n dzen <src> "nc -zv -w5 [-6] <ip> 9092"`, `ping/-c3`,
`ping6 -c3`, резолв через `getent ahostsv4/ahostsv6`.

## Результат: межкластерная матрица (events → comments)

| Направление | TCP 9092 v4 | v6 | ICMP v4/v6 |
|---|---|---|---|
| PC→UC | ❌ timeout | ❌ timeout | ✅ ~5.4мс |
| PC→HC | ✅ | ✅ | ✅ ~1.5мс |
| UC→PC | ✅ | ✅ | ✅ ~4.9мс |
| UC→HC | ✅ | ✅ | ✅ ~5.9мс |

## Результат: внутрикластерное (то, что реально нужно)

- **events-front-kafka**: ✅ полностью — все пары pc↔uc↔hc, 9092 v4+v6 OK, контроллеры 9093 OK.
- **comments-social-kafka**: ✅ работает, но брокер **UC не принимает 9092 по IPv4** от
  pc и hc (даже от своего кластера; проверены оба v4: 10.103.17.217, 10.103.222.59 —
  оба фильтруются). По v6 (fd00:b4c0:7c1::aff:0) — OK. DNS отдаёт AAAA первым, поэтому
  репликация живёт поверх v6. v4-only клиенты будут таймаутиться на UC-брокере.
  Контроллеры 9093 — OK.

**Аномалия**: правило фильтрации именно на стороне UC и только v4. Объясняет и
межкластерные фейлы «в сторону UC». Чинить firewall/NSG на
`1.broker.comments-social-kafka.uc.idzn.ru` (и сверить с events, где v4 OK).

## Грабли / приёмы

1. **traceroute на Kafka-хостах отсутствует** (контейнеры: нет traceroute/tracepath/mtr/busybox) —
   трассировку снять нечем; TCP-проверка через `nc -zv` (OpenBSD netcat есть везде).
   bash `/dev/tcp` не умеет v6 — не использовать для telnet -6.
2. **UUID из ссылки UI → алиас кластера**: `db_cluster` даёт короткое имя (`events`),
   реальный FQDN-алиас — только через прод-БД `host_state`:
   `SELECT host FROM host_state WHERE cluster_id='<uuid>'` (туннель localhost:53480,
   креды в db-seed). Угадывание по подстроке через `mcc instances "%front%"` даёт мусор
   (нашлись чужие adtech-кластеры).
3. Хосты `*.idzn.ru` → `mcc -n dzen` (не infra), и `-c <dc>` обязателен.
4. Локальная самопроверка порта на брокере: `timeout 4 bash -c '</dev/tcp/<свой-ip>/9092'` —
   `ss` на хостах тоже отсутствует.

## Отправка диагностики (INCALL-51367)

Инцидентный чат [P3] I51367 «сетевая доступность между брокерами из любого дц в UC»:
`173452223713281@chat.agent` (ссылка `u.internal.myteam.mail.ru/profile/Aqe4j-G4oAFOkLGdjx2fYQ`
резолвится через `vkws_resolve` — id в URL это stamp чата). Координатор — Даниил Шакуров
(d.shakurov@vk.team), проблема сформулирована как «нет сетевого доступа IPv4 между брокерами
из любого дц в UC».

В чат отправлена выжимка без рекомендаций (пользователь вырезал строку про firewall/NSG):
UC-брокер comments-social-kafka не принимает 9092 по v4 (оба ip), v6 OK, репликация поверх v6
через AAAA; events-front-kafka полностью OK; межкластерно падает только PC→UC (v4+v6).
