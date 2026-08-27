# T15 Save-fallback: mdb-data недоступен в момент saveUpscaledControllers (2026-08-25)

**Результат: PASS — активность save ретраится, после восстановления mdb-data доехала без дублей**

Сценарий: mdb-data (8081) убит (kill) в момент, когда workflow дошёл до фазы save после
успешного controller reload.

## Прогон

1. `POST …/hosts/controllers?dc=ic` → op `6d84553c` (ic 1→2).
2. Дождался `_update-controller-config` RUNNING (controller reload идёт) → `kill <mdb-data pid>`.
3. Save-активность (`saveUpscaledKafkaControllers`, KafkaHostsActivityImpl:27) падала
   `Connect to http://localhost:8081 failed: Connection refused` — 7 ретраев подряд,
   workflow оставался RUNNING (activity retry policy работает, не валит операцию сразу).
4. mdb-data поднят заново (`./gradlew bootRun --args='--spring.profiles.active=local --server.port=8081'`,
   лог /tmp/mdb-data.log) → save доехал с первого полла после старта.

## Верификация

- Parent `6d84553c` COMPLETED.
- host_state: 8 контроллеров, 8 уникальных (dc=2, hc=2, kc=2, ic=2).
- PMS-кворум: 8 voters, без дублей (13002@2.controller.ic ровно один).
- mdb-data log: `Save upscaled Kafka controller hosts: 8` — ровно один успешный вызов.

## Вывод

Save-фаза устойчива к недоступности mdb-data: retry policy активности переживает даунтайм,
дублей и потерь нет. Приложение.kill-механика: окно «reload контроллеров идёт» надёжно
ловится поллингом статуса `_update-controller-config` (в отличие от T3a-окна между фазами).

T16 (ретрай после T15 без дублей) выполнялся фактически в рамках этого прогона:
save ретраился тем же раном; повторный запуск операции не потребовался. Отдельный
прогон T16 (новая операция после failed-save) не делал — состояние эквивалентно T2/T14.
