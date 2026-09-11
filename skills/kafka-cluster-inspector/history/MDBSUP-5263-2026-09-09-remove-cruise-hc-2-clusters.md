# MDBSUP-5263 — Удаление Cruise Control из HC, 2 кластера (communal, stg-kafka)

- Дата: 2026-09-09
- communal `167c2774-1c41-494e-a427-00e11e946ba3` (prod, fullQueue
  `communal-integr-platform-kafka.integr-platform.db.production.mdb.prod`),
  cruise `1.cruise.communal-integr-platform-kafka.hc.one-infra.ru`
- stg-kafka `86a382dc-2f46-44f3-a0a2-8f3780214fac` (dev, fullQueue
  `stg-kafka-int-platf-test-kafka.int-platf-test.db.dev.mdb.prod`),
  cruise `1.cruise.stg-kafka-int-platf-test-kafka.hc.one-infra.ru`

## Суть

Пользователь уводит сервисы из HC. Перенос CC между ДЦ не поддерживается —
канон удаления (MDBSUP-4827/4918/5186), пересоздание в EC/KC/PC — сам юзер через UI.

## Отличие от чистого канона: висящая downscale-задача оператора (stg-kafka)

Предыдущая попытка delete_hosts (операция `96877859-9c0a-4c6c-a1e9-c22ef87d9cd5`)
упала с «Operator task in progress [object Object]». Причина: в операторе висела
задача `kafka.downscale-broker` (`--cloud=hc --replicas=0`, started 12:05:56),
застрявшая в precondition `isReadyForActions() false`; шагов не делала
(`serviceWithdrawn=false, storageWithdrawn=false`). Сам cruise-инстанс был
`RUNNING UNAVAILABLE` с 10:45 (мёртвый) — потому и precondition не сходился.

Решение — паттерн MDBSUP-4899/5092: **op_stop первым**, потом канон:

```bash
mcc --local -n infra -c HC op_stop "queue://stg-kafka-...db.dev.mdb.prod" kafka.downscale-broker
```

Дальше для обоих кластеров идентично (все в `-c HC -n infra`):
1. `stop cruise.<queue>` → FINISHED (коммунал ~40 c; stg STOPPING ~2 мин, инстанс UNAVAILABLE —
   stop через control plane всё равно отработал).
2. `withdraw cruise.<queue>` (сервис) — без уравнения, ~60 c, EntityNotFoundException = успех.
3. `withdraw <fullQueue>/cruise` (storage) — pexpect-уравнение (`7 mod 2`, `9*1`),
   → PURGEABLE (одному_volume под-состояния NORMAL после withdraw — норма, ресурс освобождён).
4. Прод-БД (docker cp + psql -f, BEGIN/COMMIT): `DELETE 1` host_state +
   `UPDATE 3` db_cluster_version (`jsonb - 'cruiseControl'::text`) на кластер. Верификация: 0/0.

`one_cloud_meta` (cruise-control-service) не трогали. Упавшую операцию 96877859
не правили — failed/attempts_left=0/in_processing=false терминальна, ничего не блокирует.

## Итог

После op_stop задача исчезла из `operators.kafka.tasks` (TASK_ABSENT). В host_state
по 6 хостов (3 broker + 3 controller ec/kc/pc), ключ cruiseControl удалён из всех версий.
Пересоздание cruise в целевом ДЦ — юзер через UI (кнопка активируется после чистки
kafkaParams.cruiseControl). one_cloud_meta переиспользуется create-флоу.

## Follow-up (09.09, ~18:20): kafka-m2b — круиз из DC, тот же канон

Кластер `6195dc89-89ec-483e-bf6c-abc842fbf8df` (kafka-m2b, project 55). Отличие: на кластере
висела активная `add_hosts` in_progress (99cfb970, старт 18:07) — юзер явно скомандовал
«удаляй, на операцию не смотри». Конфликта не было: cruise-сервис/очередь не пересекаются
с add_hosts по брокерам, withdraw отработал штатно (stop ~1 мин, service withdraw ~45 c,
storage → PURGEABLE, DELETE 1 / UPDATE 3, версий с cruiseControl 0). Вывод: канон
не блокируется чужой активной операцией в БД (guard «0 активных операций» — про
повторный запуск mdb-операций, не про ручной mcc-withdraw), но без явного указания
юзера лучше всё равно дождаться завершения.
