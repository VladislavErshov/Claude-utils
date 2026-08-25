# Получение данных для SENTINEL RESET

## Зачем

`SENTINEL RESET <master-name>` — сбрасывает known-slaves и known-sentinels для
указанного мастера на конкретном sentinel-инстансе. После сброса sentinel
пере-обнаруживает только живых пиров. Удалённый (зомби) хост никто не анонсирует —
он выпадает из state.

Применяется для лечения "забытый known-peer" — когда в `redis-sentinel.log` спам
`Failed to resolve hostname '<dead-host>'`. См. `read_sentinel_logs.md` для диагностики.

## Что нужно знать перед выполнением

Из `sentinel.conf` нужны два значения:
1. **`<master-name>`** — из строки `sentinel monitor <master-name> <host> <port> <quorum>`
2. **`<sentinel-pass>`** — из строки `sentinel sentinel-pass <password>`

Имя пользователя sentinel — всегда `master` (из `sentinel sentinel-user master`).
Порт sentinel — `26379` (из `port 26379`).

## Получить sentinel.conf с одного из хостов кластера

Пользователь даёт список хостов. Берём **любой** из них — sentinel.conf на всех
хостах кластера одинаковый по master-name и sentinel-pass (отличаются только
`sentinel announce-ip` и иногда `quorum`).

Скачать `/etc/redis/` (целиком директорию) с хоста — через скилл
[`mcc-host-worker`](../../mcc-host-worker/SKILL.md) (`mcc scp`, см.
`commands/scp.md` для шаблона). Шаблон хоста:
`1.db.<cluster>-cfs-redis.<dc>.one-infra.ru`.

## Вытащить master-name и sentinel-pass

```bash
grep -E "^sentinel (monitor|sentinel-pass) " ~/redis_conf/sentinel.conf
```

Вывод (пример):
```
sentinel monitor foresightmstrprod7 1.db.foresightmstrprod7-cfs-redis.kc.one-infra.ru 6379 2
sentinel sentinel-pass B49QFI2UA1xA0QDN9Y8iAYeHGXFdfE
```

Здесь:
- `master-name` = `foresightmstrprod7` (второе поле после `sentinel monitor`)
- `sentinel-pass` = `B49QFI2UA1xA0QDN9Y8iAYeHGXFdfE`

## Сформировать команду

```
redis-cli -p 26379 --user master -a '<sentinel-pass>' SENTINEL RESET <master-name>
```

Подставив значения из предыдущего шага:

```
redis-cli -p 26379 --user master -a 'B49QFI2UA1xA0QDN9Y8iAYeHGXFdfE' SENTINEL RESET foresightmstrprod7
```

## Выполнить на ВСЕХ хостах кластера

Команду нужно выполнить на **каждом** оставшемся sentinel-хосте кластера.
Пользователь заходит на хост (через скилл [`mcc-host-worker`](../../mcc-host-worker/SKILL.md)
или иным способом) и запускает `redis-cli` локально на хосте.

Ожидаемый ответ: `(integer) 1` — sentinel сбросил state для указанного мастера.

Пример сессии:
```
1.db.foresightmstrprod7-cfs-redis.pc.one-infra.ru: /etc/redis# redis-cli -p 26379 --user master -a 'B49QFI2UA1xA0QDN9Y8iAYeHGXFdfE' SENTINEL RESET foresightmstrprod7
Warning: Using a password with '-a' or '-u' option on the command line interface may not be safe.
(integer) 1
```

## После RESET

Подождать 30-60 сек и проверить, что спам `Failed to resolve hostname` прекратился
(см. `read_sentinel_logs.md` → "Проверка после SENTINEL RESET").

Если спам продолжается — значит хотя бы на одном хосте RESET не выполнен, либо
sentinel пере-поднял known-peer из persistence-файла. Во втором случае:

1. Остановить sentinel на проблемном хосте.
2. Из runtime-конфига (обычно `/etc/redis/sentinel.conf`, либо state-файл в `dir /mnt/redis/senti`)
   вручную убрать строки `sentinel known-replica ...<dead-host>...` и
   `sentinel known-sentinel ...<dead-host>...`.
3. Запустить sentinel заново.

## Корневая причина для mdb-data

В идеале `SENTINEL RESET <master>` должен вызываться автоматически при удалении
redis-хоста через mdb-data флоу — на всех оставшихся sentinel-инстансах кластера.
Если этого не происходит, удалённые хосты остаются в known-peers навсегда и
засоряют лог. Это баг флоу удаления, который надо чинить в коде mdb-data/mdb-processing.
