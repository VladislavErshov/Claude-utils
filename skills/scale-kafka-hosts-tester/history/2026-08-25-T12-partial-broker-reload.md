# T12 Частичный отказ reload брокеров + рестарт (2026-08-25)

**Результат: PASS**

Источник: прод-кластер `0455a8f4` (ads-kafka-rustore) — `Config reload failed for hosts: [5 brokers]`.

## Прогон

1. `POST …/hosts/controllers?dc=hc` → op `035ccc8d`. Deploy прошёл, broker reload пошёл
   последовательно dc → hc → kc (maxConcurrency 1).
2. `_dc_1`, `_hc_1` завершились успешно; на `_kc_1` → terminate child
   (`reloadKafkaBrokerInstance`, workflowId `<op>_update-broker-config_kc_1`).
3. **Родительский updateConfigKafkaBroker упал ровно как прод**:
   `Config reload failed for hosts: [1.broker.test-modify3-mdbdev-kafka.kc.one-infra.ru]`,
   type=`RELOAD_FAILED` (KafkaHostReloadHelper.assertReloadComplete:123, nonRetryable).
   Parent upscale FAILED тем же failure.
4. Промежуточное состояние: PMS-кворум уже 6 voters (upsert до deploy — допустимо),
   host_state 5 (save не выполнился), kc-брокер остался без reload.

## Рестарт

5. Закрыл операцию в БД, повторный `POST ?dc=hc` → op `fc76fe50` → COMPLETED:
   child skip (`already has 2 replicas`), broker reload прошёл все 3 ДЦ, controller reload,
   save = 6 хостов.

## Верификация

- host_state: 6 контроллеров, 6 уникальных (dc=2, hc=2, kc=1, ic=1).
- PMS `kafka.controller.quorum`: 6 voters, каждый ровно один раз (10001/10002/11001/11002/12001/13001).

## Вывод

Частичный отказ broker-reload корректно типизируется (`RELOAD_FAILED` non-retryable со списком
хостов — идентично прод-сообщению), PMS уже применён (upsert до deploy — by design), рестарт
идемпотентен и доводит reload до конца. Поведение соответствует ожиданиям, багов не найдено.
