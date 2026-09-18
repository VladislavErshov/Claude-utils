# MDBSUP-5383 — zen-auth-shard1..4 + zen-users-shard1: delete_hosts (ДЦ dc) упал на get_result_mongodb_downscale_operator, задачи оператора умирают мгновенно (17.09.2026)

Тикет: MDBSUP-5383 (закрыт). Проект front, ns **dzen** (облака odkl: master.<dc>.odkl.ru, FQDN *.idzn.ru, mcc `-n dzen`).

## Проблема

5 кластеров, в каждом удаляли хост(ы) в ДЦ dc. Все операции delete_hosts — `failed, attempts_left=0, in_processing=false, finished_ts=NULL`, error `get_result_mongodb_downscale_operator: Operator task <fullQueue> in progress [object Object]`. Это **оператор one-cloud-ops** (не Temporal).

Операции/кластеры:
- zen-auth-shard1 `b1beaa6e-2f3d-49dc-854f-de57a280bb40` — op `b33a883e`
- zen-auth-shard2 `2e6e1494-1747-4905-a8e0-1bab5ce8243a` — op `34309774`
- zen-auth-shard3 `fa362a57-5b15-4a00-9a5a-645d83877253` — op `0f57cc43`
- zen-auth-shard4 `07dcbe23-fa8e-4e50-bdbc-fd0ee22bed03` — op `0f2a4904`
- zen-users-shard1 `c3d3892d-ec46-402a-a5b9-94baf2d30b45` — op `5e80fe43` (удалён db.dc; **hidden.dc** — `1.hidden.zen-users-shard1-front-mongo.dc` — остался живым в облаке и в rs-конфиге)

## Диагностика (что где нашли)

1. Прод-БД: статус операций, error_message — маркер операторного флоу.
2. `mcc --local -n dzen -c dc ops "queue://<fullQueue>" -f json` → `operators.mongodb.tasks`:
   - shard1/3/4 + users — задачи downscale-instances **нет** (ни в tasks, ни в taskinfos);
   - shard2 — задача висела 6 дней: `serviceWithdrawn: true, storageWithdrawn: false, isWithdrawing: true` (storage не снят);
   - alert `mongodb.watch[refreshAvailabilityState]` — "Cluster is AVAILABLE, no failed hosts" (репликасеты живы, partially_available в UI — от ghost-строк host_state).
3. Облако: `mcc instances "%<cluster>-front-mongo%" -f yaml` и `tool_status --type storage "<fullQueue>/<db|hidden>"`:
   - shard1/3/4 — в dc чисто (сервис+storage выведены);
   - shard2 — сервис выведен, storage db остался (NORMAL, 310G nvme, квоты 16 vCPU/128G);
   - users — db.dc чисто, hidden.dc RUNNING + storage MOUNTED.
4. Состав rs без admin-прав: mongosh на hidden-хосте юзером `backuper` (creds из `/etc/backups/mongo_config.ini`; ⚠️ mcc маскирует секреты в выводе `__SECRET_N__` — парсить ini **на хосте** в JS, не выводить). `db.hello()` даёт hosts/primary/me/hidden — достаточно для проверки членства. `replSetGetStatus`/`replSetGetConfig` backuper'у нельзя (нужен clusterMonitor).
5. PMS `zen.mongodb.hosts` (values.do, ns=dzen, application=mdb): dc-хостов нет у всех 5 — оператор свою часть обновил.

## Ключевой факт: задачи оператора умирают мгновенно

`op_stop` + `op_start mongodb.downscale-instances -- --cloud=dc --replicas=0`: задача появляется с чистым state (все false), живёт 3–20 сек и **исчезает, не выполнив ни одного invoked-действия**. Причина — модель оператора уже не содержит dc-сущностей (PMS/облако вычищены), downscale нечего делать; продуктовый флоу этим не доводится. Если после падения операции PMS/облако чистые — сразу ручная доводка + SQL.

## Фикс

1. **rs-reconfig для hidden.dc (zen-users)** — с primary через admin:
   - Пароль admin: vault `zkv/data/mdb/front/mongo/<fullQueue>/admin-password`, поле `value`. Чтение с любого хоста кластера: token в `~/.vault-token` (mcc-юзер), `VAULT_ADDR=https://dl.vault.idzn.io` (из /proc/1/environ), curl `X-Vault-Token` → `/v1/zkv/data/...` (kv v2). `vault` CLI на хосте в PATH mcc-юзера нет.
   - Вход `__system` + `/var/lib/mongo/secret.key` — **не работает** (юзера нет), не тратить время.
   - Скрипт JS заливать base64 (`echo B64 | base64 -d > /tmp/x.js`): Tcl жуёт скобки/`---`; в mongosh нет `cat()` — только `fs.readFileSync`; `print(a,b,c)` вместо скобочных литералов; grep-фильтры не должны пропускать пустой OUT (иначе `&&`-цепочка ретраит молча).
2. **Вывод hidden.dc из облака**: `mcc stop hidden.<cluster>` → FINISHED → `withdraw --type service` → ждать исчезновения инстанса → `withdraw --type storage "<fullQueue>/hidden"`. Уравнение — pexpect (`attempt N/3\): A op B`, бывает `mod`); `echo | mcc` молчит.
3. **shard2**: `withdraw --type storage "<fullQueue>/db"` → состояние PURGEABLE → `purge "<fullQueue>/db" all` ("A total of none to purge" — норм). Квоты освобождаются.
4. **SQL одной транзакцией**: `UPDATE operations SET status='done', in_processing=false, finished_ts=now(), error_message=NULL` (5 строк) + `DELETE FROM host_state` (6 ghost-строк: db.dc у всех + hidden.dc у zen-users). Верификационные SELECT внутри транзакции.

## Проверка

- instances/tool_status в dc пусто по всем 5; rs: hello() на hidden.rc (у users — на db.rc): primary на месте, hosts = rc/pc/ec.
- БД: ops done, host_state без dc-хостов. PMS без dc.

## Остаток

Причина смерти задач `mongodb.downscale-instances` при пустой модели не локализована (логи one-cloud-ops не смотрели; sshexec на cloud-ops хосты невозможен — TLS handshake). Рецидив → ручная доводка по этому сценарию. Общий паттерн как MDBSUP-5216 (kafka, задача исчезла при запаркованной операции), но здесь задачи умирают сразу, а не после рестарта оператора.
