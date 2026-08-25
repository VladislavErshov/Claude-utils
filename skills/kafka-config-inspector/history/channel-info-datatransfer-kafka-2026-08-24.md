# channel-info-datatransfer-kafka — 2026-08-24

Кластер: `channel-info-datatransfer-kafka` (ns=infra, app=mdb, PMS-ключ `channel-info-datatransfer-kafka.clouds`).
Симптом: `1.controller...hc.one-infra.ru` — kafka-controller.service failed (exit 1) с
`IllegalArgumentException: node id 10001 must be included in controller.quorum.voters=Set(11001,12001,13001)`.

## PMS (values.do, read-only)

| Переменная | Значение |
|---|---|
| kafka.controller.quorum | 11001@kc,12001@pc,13001@uc — БЕЗ hc |
| kafka.layout | hc,kc,pc,uc — С hc |
| kafka.controller.properties | шаблон: node.id={{1*10000+dc_id*1000+instance_id}}, voters=pms('kafka.controller.quorum') |

Других PMS-ключей кластера с quorum нет (проверено полным дампом values.do).
**Противоречие: layout содержит hc, quorum — нет.** node.id=10001 на hc рендерится по формуле
из ДЦ → конфиг невалиден → сервис падает.

## Рендеры на хостах (расхождение во времени)

| Хост | node.id | voters в файле | mtime | статус |
|---|---|---|---|---|
| controller hc | 10001 | kc,pc,uc | 24.08 14:07 | failed |
| controller kc | 11001 | kc,hc,pc | 23.06 | active (с 19.08) |
| controller pc | 12001 | kc,hc,pc | 14.03.2025 | active (с 10.08) |
| controller uc | 13001 | kc,hc,pc,uc | 06.08 | active |
| broker kc | 21001 | kc,pc,uc | 24.08 12:59 | active |

Кворум в PMS менялся минимум дважды ({kc,hc,pc} → {kc,hc,pc,uc} ~06.08 → {kc,pc,uc}),
хосты перерендеривались вразнобой. confp --oneshot на hc рендерит тот же 3-voter кворум
(без hc) — PMS действительно отдаёт это значение.

## Вывод

Либо layout устарел (hc выводят из кластера, но layout/хост не почистили), либо quorum
ошибочно без hc. Решение — через modify-флоу mdb-data, не ручной записью в PMS.

## Действие (2026-08-24 ~14:3x, по явному указанию пользователя)

update.do: kafka.controller.quorum на `channel-info-datatransfer-kafka.clouds` — добавлен
`10001@1.controller.channel-info-datatransfer-kafka.hc.one-infra.ru:9093` (после kc, как в
прежнем 4-DC рендере). HTTP 200, верифицировано values.do байт-в-байт.
Старое значение ( Rollback ): 215 байт в quorum-old.txt (tmp) —
`11001@kc...,12001@pc...,13001@uc...` (без hc).

## Результат — MDBSUP-4812

✅ Изменение kafka.controller.quorum в PMS (добавлен hc 10001) + перерендер/рестарт
kafka-controller на hc **решили проблему**: контроллер hc поднялся, инцидент закрыт
(MDBSUP-4812). Смешанных voter-сетов после rolling-перезапуска не осталось.

Урок: если controller падает с `node id N must be included in controller.quorum.voters`,
а node.id рендерится по формуле из kafka.layout — сверять **пару** layout↔quorum в PMS
(расхождение = кривой modify/очистка кластера), фиксить quorum в PMS + confp + restart.

## Грабли

- `mcc logs <host> <stream>` — позиционные аргументы, НЕ `--stream`.
- kafka-metadata-quorum.sh describe --status с controller-хоста на :9093 — TimeoutException
  (client.properties не подходит к controller-listener) — не использовать для проверки кворума.
