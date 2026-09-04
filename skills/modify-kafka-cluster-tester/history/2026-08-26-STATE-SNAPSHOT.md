# STATE SNAPSHOT: тесты modify/resize Kafka со сменой типа диска (MDBDEV-3231)

Обновлён: 2026-08-27 ~01:30 local (22:30 UTC 26.08). **Файл для восстановления контекста.**
Итоги завершённой вечерней сессии: `history/2026-08-26-evening-M2-M6-results.md` (обязательно к прочтению).

Ветки: mdb-data `ershov/MDBDEV-3231-allow-disk-type-change-on-kafka-modify`,
mdb-processing `ershov/MDBDEV-3231-check-disk-type-on-resize`.
Перед продолжением: SKILL.md этого скилла (секции «Образ docker», «Lost timer»,
«PREFAIL-брокеры», «Порядок фаз modify: UPDATE_THEN_RESIZE»).

## Инфраструктура (проверять живость)

| Сервис | Порт | Проверка |
|---|---|---|
| mdb-data | 8081 | `curl -s localhost:8081/actuator/health` (DOWN из-за redis-sentinel — НЕ блокер) |
| mdb-processing | 8080 | лог `/tmp/mdb-processing.log` |
| temporal | 8233 | `http://localhost:8233/api/v1/namespaces/default/workflows` |
| postgres | 6434 | контейнер `pg_backstage_plugin_mdb`, БД `backstage_plugin_mdb`, юзер `dev` |

Terminate workflow: GET temporal-ui → cookie `_csrf` → POST
`…/api/v1/namespaces/default/workflows/{wid}/terminate` + `X-Csrf-Token` + body `{"reason":...}`.
409 → `UPDATE operations SET status='done', in_processing=false WHERE id='<opId>';`
«No changes found» 400 → откатить свежий draft (jsonb_set diskType/diskGb/lanIn).

## Кластеры (kafka, project 160, ns INFRA)

| Кластер | ID | Draft-состояние на паузу | Примечание |
|---|---|---|---|
| test-modify3 | `9fc47c1b-011d-4aaa-b411-de5345a0204e` | nvme 2g, lanIn 1 (reverse PASS) | |
| test-downgrade5 | `184ac05d-64e7-4276-ad59-017475bf4f4a` | hdd 8 | **M3 недоигран**: hc/kc реально hdd, pc nvme + PREFAIL (UR=1) |
| test-downgrade6 | `69204f9d-723c-4ad7-848c-efcd1b2389bd` | nvme 10 (M2 PASS) | |
| test-downgrade7 | `23f108ac-1907-434e-a67b-dda01df316f4` | ctrl hdd 10 (M4 PASS) | ⚠️ НЕ uuid, искать LIKE '23f108ac%' |

## Статус сценариев

M1 ✅, M2 ✅, M4 ✅, M5 ✅, M6b ✅ (M6a lan-минимум невоспроизводим на dev — prod-блок
валидатора выключен), reverse modify3 ✅. Единственный открытый: **M3 ⏸ отложен**.

## Что сделать при продолжении M3

1. Починить/проверить pc-брокер d5: `curl http://1.broker.test-downgrade5…pc…:81/getstatus`
   (не через mcc — с хоста; через mcc sshexec). При «Has N partitions with min in-sync
   replicas» — это UnderReplicatedPartitions, разбор через /kafka-cluster-inspector.
2. `UPDATE operations SET status='done', in_processing=false WHERE id='a5247db0-1f1b-4e6a-9460-5f8e455e1f9e';`
3. Свежий draft d5 (hdd 8, create_ts 2026-08-26 23:29) откатить в nvme (jsonb_set).
4. PATCH `/tmp/m3-rerun.json` (nvme→hdd): hc/kc скипнутся, останется pc.
   Если pc ещё PREFAIL — родитель будет ждать снятия; следить за lost timer
   (TIMER_STARTED без FIRE >5 мин → terminate + rerun по протоколу выше).
5. После PASS — запись в history/ + таблица SKILL.md.

Шаблоны запросов живы в /tmp: m2-rerun.json, m3-rerun.json, m4-ctrl-d7.json,
m5-d5back-modify3-2gb.json, m6a-lan.json, m6b-dcs.json. Baseline — `db_cluster_version
WHERE status='scheduled'` (у d5 scheduled = nvme 8).
