# Runbook для дежурного по шардированному Redis Cluster

**Канон — Confluence «Дежурство MDB: Redis», секция «Runbook для дежурного»** (SSOT):
https://confluence.vk.team/pages/viewpage.action?pageId=1351178658

Покрывает: вечную переливку реплик (redis-cli-сессия с config set repl-backlog-size /
repl-timeout / client-output-buffer-limit; скрипт применения на всех хостах), закончился
диск, зачистился диск (образ 2.0.0+/3.0.0+), битый AOF (копия `/mnt/appendonlydir`,
`redis-check-aof --fix`, полный лог-пример), 2 мастера в шарде, some nodes have
disconnected node (CLUSTER FORGET), ОММ реплик, dial tcp timeout (tcp-backlog 65535 +
sysctl net.core.somaxconn / net.ipv4.tcp_max_syn_backlog в Env очередей), «что-то другое»
(дашборды мониторинга), записи рассказа Лёни о Redis (видео + команды).

Вики живая — править там, копии не тащить.

Краткие версии, наш MDBSUP-кейс («expected 1 MASTER, found 2») и dial tcp из MDBSUP-2147 —
[../SKILL.md](../SKILL.md) → «Runbook для дежурного».
Доступ к хостам — скилл [`mcc-host-worker`](../../mcc-host-worker/SKILL.md).
