# Runbook для дежурного по Redis Sentinel

**Канон — Confluence «Дежурство MDB: Redis», секция «Runbook для дежурного»** (SSOT):
https://confluence.vk.team/pages/viewpage.action?pageId=1351178658

Покрывает: вечную переливку реплик (config set repl-backlog-size / repl-timeout /
client-output-buffer-limit), закончился диск, битый AOF (копия `/mnt/appendonlydir`,
`redis-check-aof --fix`), «что-то другое» (дашборды мониторинга: Replica backlog size,
Total Memory Usage, Errors/sec, Total CPU Usage Main Thread, Connected clients).
Вики живая — править там, копии не тащить.

Краткие версии — [../SKILL.md](../SKILL.md). Доступ к хостам — скилл
[`mcc-host-worker`](../../mcc-host-worker/SKILL.md).
