# MDBSUP-4918 — kafka-stats-misc: вывод cruise из KC (27.08.2026)

Кластер `d39a4799-c46d-48a9-a5d2-2cf3b0ff941f` (kafka-stats-misc-adtech-kafka, Kafka,
adtech, ns infra). Cruise `1.cruise.kafka-stats-misc-adtech-kafka.kc.one-infra.ru` (dc=kc)
выведен из KC; пересоздание в UC — отдельный шаг тикета (не в этой записи).

## Выполнено (канон — MDBSUP-4827)

0. Проверка: активных операций (status NOT IN done/failed/canceled) по кластеру — 0.
   Enum operation_status: draft/scheduled/in_progress/need_retry/need_approval/done/canceled/failed
   (строки 'new'/'running' в enum нет — падает `invalid input value for enum`).
1. `mcc --local -n infra -c KC stop "cruise.kafka-stats-misc-adtech-kafka"` → STOPPING →
   FINISHED (~50 сек).
2. `mcc --local -n infra -c KC withdraw "cruise.kafka-stats-misc-adtech-kafka"` →
   через ~45 сек EntityNotFoundException по инстансу и сервису = успех.
3. `mcc --local -n infra -c KC withdraw kafka-stats-misc-adtech-kafka.adtech.db.production.mdb.prod/cruise`
   — pexpect-уравнение (`5+4`), ответ «NON EMPTY … will turn PURGEABLE» → tool_status:
   state PURGEABLE. Ресурсы освобождены.
4. Прод-БД (docker cp + psql -f, BEGIN/COMMIT):
   ```sql
   DELETE FROM host_state WHERE cluster_id='d39a4799-c46d-48a9-a5d2-2cf3b0ff941f'
     AND host='1.cruise.kafka-stats-misc-adtech-kafka.kc.one-infra.ru';               -- DELETE 1
   UPDATE db_cluster_version
   SET cluster_params = jsonb_set(cluster_params, '{kafkaParams}',
         (cluster_params->'kafkaParams')::jsonb - 'cruiseControl'::text),
       update_ts = now()
   WHERE cluster_id='d39a4799-c46d-48a9-a5d2-2cf3b0ff941f'
     AND cluster_params->'kafkaParams' ? 'cruiseControl';                             -- UPDATE 4
   ```
   Верифицировано: cruise-строк в host_state — 0; версий с cruiseControl — 0.

## Итог

В host_state осталось 6 хостов (3 broker + 3 controller: hc/pc/uc). one_cloud_meta
cruise-control-service (fake_id 9198, serviceName=cruise, fullQueue
kafka-stats-misc-adtech-kafka.adtech.db.production.mdb.prod) — НЕ тронут (прецедент 4827).

## Заметки

- В old-строке db_cluster_version 10659 был `cruiseUserPassword: ""` — ключ удалён
  вместе с cruiseControl целиком, пароль не сохранялся.
- Длина STOP: service с NVME=448M, replicas=1 — withdraw сервиса быстрый (<1 мин),
  storage PURGEABLE мгновенно после уравнения.
