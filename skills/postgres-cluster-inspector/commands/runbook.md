# Runbook для дежурного — PostgreSQL

**Канон — Confluence «Дежурство MDB: Postgres» (SSOT)**:
https://confluence.vk.team/pages/viewpage.action?pageId=1348619018

Покрывает: правила безопасной работы с PostgreSQL в MDB; если больше половины реплик
UNAVAILABLE; как дебажить, почему реплика UNAVAILABLE; подключение под суперпользователем
(postgres/pgbouncer); параметры вне UI; пользователи и базы (добавление/удаление/зависшие
операции/сброс пароля/права суперпользователя); подписки; pg_repack; медленные запросы;
отключение синхронной репликации; закончились подключения / shared memory; переналивка
реплики (3 случая); реплика не поднимается даже после полной переналивки (.history в S3);
добавление/удаление инстанса; переподнять постгрес при etcd без кворума; pgbouncer;
ipv6; SOC2; PgBouncer-инциденты. Вики живая — править там, копии не тащить.

**Шардированный PostgreSQL (Citus)** — отдельная страница
[«Дежурство MDB: шардированный PostgreSQL»](https://confluence.vk.team/pages/viewpage.action?pageId=2107500377):
архитектура координатор + шарды, диагностика pg_dist_node / poolinfo / authinfo,
citus_lock_waits, добавление БД в шардированный кластер, недоступность координатора/шарда,
DDoS PgBouncer после failover (comments-video).

Наши дополнения к переналивке (контроль хода pg_basebackup, маркеры успеха, частые
ошибки) — [reinit_replica.md](reinit_replica.md). Диагностика по симптомам —
[diagnostics.md](diagnostics.md), команды на хосте — [connection.md](connection.md).
