# Администрирование шардированного Redis Cluster

**Канон — Confluence «Дежурство MDB: Redis», секция «Частые запросы»** (SSOT):
https://confluence.vk.team/pages/viewpage.action?pageId=1351178658

Покрывает: ERR Slot 10922 (FLUSHALL + CLUSTER RESET SOFT), не собирается кластер,
смена мастера, решардинг, удаление ноды (полный алгоритм c host_state / cluster_links /
PMS `zen.redis.hosts`+`zen.redis.backupHosts` / withdraw / рестарт оператора), возврат
долго лежавшей ноды, перебалансировка мастеров по ДЦ, модули, телепорт
(cluster-preferred-endpoint-type hostname), ACL (забанить команду, default-пользователь,
права для диагностики, долгое добавление, удаление), параметр вне UI, isPersistent,
access-логи, обновление 7→8, миграция 6.2→7.0 (отдельная страница, ссылка из вики),
восстановление из бэкапа (полный флоу `redis_cluster_restore_script.py`), бэкап другого
кластера, учения, скрипты CONFIG SET на всех нодах и CLUSTER FORGET (python + redis,
`/opt/redis-env-python/bin`). Вики живая — править там.

⚠️ Известная опечатка в вики (раздел про постфактум `cluster-preferred-endpoint-type`):
слитная строка `config set cluster-preferred-endpoint-type hostname cluster-announce-hostname $HOST`
неверна — это **две отдельные команды** (см. ниже).

## Наши дополнения к вики

### Включить дуалстек (ipv4 + ipv6)

Если у клиента был v4-only redis, возможно нет дырки. Сначала добавить v6 в манифест,
попросить заказчика проверить доступ. Только тогда пересобирать кластер. На всякий
случай можно предложить клиенту настроить `cluster-preferred-endpoint-type hostname` —
тогда при редиректе нода будет возвращать имя хоста, а не IP.

⚠️ **`cluster-preferred-endpoint-type=hostname` настраивается через UI** (через
`cluster_to_template` + `zen.redis.conf` в PMS + update через UI с минимальным
изменением), а не через `config set` в рантайме. `config set` — только для
экстренного применения в рантайме, но без фиксации в шаблоне настройка слетит при
следующем `confp --oneshot && systemctl restart redis`. См. вики «Выставить параметр,
которого нет в UI».

Сабмитим в манифесте v4, v6. Далее для каждого шарда:

⚠️ Операции проводим на репликах! Когда нужно будет работать с мастером — переключим его.

1. На реплике: `cluster forget <replica_id>`, на всякий случай на всём кластере.
2. Если нужно:
   - Добавить в PMS, в базе и на хосте `cluster-preferred-endpoint-type hostname`
     (через UI — см. предупреждение выше; `config set` — только в рантайме).
   - Проверить, что возвращает `config get cluster-announce-hostname`.
   - Если значение пустое — `config set cluster-announce-hostname <instance_name>`.
3. От этой же реплики: `cluster meet <master_ip_v6>`.
4. `cluster replicate <master_id>`.
5. Ожидать, когда нальётся. Сделать со всеми репликами по очереди. Когда дойдём до
   мастера — переключить его перед операцией.

⚠️ После `CLUSTER FORGET` ID ноды попадает в blacklist на ~60 сек, и gossip от неё
игнорируется остальными. Поэтому после `MEET v6 + REPLICATE` на реплике нужно
дополнительно сделать `CLUSTER MEET <replica_v6>` **с мастера** (и с остальных нод) —
иначе реплика будет реплицировать по v6, но мастер её в `cluster nodes` не увидит,
и failover на неё не произойдёт.

⚠️ Для обновления endpoint'а уже известной ноды на v6 (без изоляции) достаточно
`CLUSTER MEET <v6>` на остальных нодах — без `FORGET`. Это менее рискованно и
работает, когда нода уже в кластере, просто видна по v4. Например, после `failover`
бывший мастер остаётся видимым по v4 на других шардах — `CLUSTER MEET <v6>` на
каждой ноде обновляет endpoint на v6 без даунтайма.

⚠️ После `CLUSTER FAILOVER` бывший мастер автоматически становится slave'ом нового
мастера и подключается к нему по v6 (если v6-endpoint'ы уже были в `cluster nodes`).
Отдельный `FORGET + MEET + REPLICATE` для бывшего мастера не требуется — только
`CLUSTER MEET <v6_бывшего_мастера>` на остальных нодах для обновления endpoint'а.

`CLUSTER FAILOVER` отправляется **на реплику**, а не на мастер (на мастер вернёт
`ERR You should send CLUSTER FAILOVER to a replica`).

### Постфактум cluster-preferred-endpoint-type hostname на всём кластере (рантайм)

Перебрать хосты × ДЦ через скилл [`mcc-host-worker`](../../mcc-host-worker/SKILL.md)
(`mcc sshexec`; шаблон перебора — там же в скилле). Шаблон хоста:
`1.shard${i}-db.<queue>.${dc}.one-infra.ru`. Это **две отдельные команды**, не одна
(слитная строка в вики — опечатка):

```
redis-cli -c --user master -a <password> config set cluster-preferred-endpoint-type hostname
redis-cli -c --user master -a <password> config set cluster-announce-hostname $HOST
redis-cli -c --user master -a <password> config REWRITE
```

После применения в рантайме — обязательно зафиксировать в PMS (`zen.redis.conf`) и
в `cluster_to_template`, иначе слетит при следующем `confp --oneshot` или update.
Пароль — `cat /etc/redis/redis.conf | grep masterauth` на одном хосте.
