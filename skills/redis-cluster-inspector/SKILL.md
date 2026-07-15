---
name: redis-cluster-inspector
description: Инспекция Redis Sentinel-кластеров (mdb-data, cfs-redis) — проверка known-peers, поиск забытых удалённых хостов, диагностика спама "Failed to resolve hostname" в redis-sentinel.log, выполнение SENTINEL RESET. Список хостов задаёт пользователь (формат 1.db.<cluster>-cfs-redis.<dc>.one-infra.ru). Конфиги и логи читаются через mcc scp. Используй когда нужно проверить состояние Sentinel-кластера, найти "зомби"-хост в known-peers или сгенерировать команду SENTINEL RESET.
allowed-tools: [bash, read_file, write_file, edit_file]
---

# Скилл инспекции Redis Sentinel-кластеров

Скилл для разбора состояния Redis Sentinel-кластеров управляемых mdb-data. Покрывает:

1. **Поиск "зомби"-хостов** — удалённые из инфраструктуры хосты, которые Sentinel
   продолжает держать в `known-sentinel` / `known-replica` и пытается пинговать,
   засоряя лог `Failed to resolve hostname ...`.
2. **Чтение redis-sentinel.log** — поиск событий `+sentinel`/`+slave`/`+sdown`/`-sdown`/`+reboot`,
   хронология добавления/удаления пира.
3. **Генерация команды `SENTINEL RESET`** — собрать из `sentinel.conf` имя мастера
   и sentinel-пароль для целевого кластера.

⚠️ Скилл проверяет **только состояние Sentinel** (known-peers, субъективные down-ы,
события failover). Он НЕ проверяет здоровье Redis как БД: replication lag, память,
persistence, slowlog — всё это за пределами области действия.

## Известная проблема: забытые удалённые хосты в known-peers

**Симптом**: на всех оставшихся sentinel-хостах кластера в `redis-sentinel.log`
каждую секунду появляется строка:

```
<pid>:X <date> # Failed to resolve hostname '1.db.<cluster>-cfs-redis.<dc>.one-infra.ru'
```

**Причина**:
- При удалении redis-хоста через mdb-data флоу **не вызывает** `SENTINEL RESET <master>`
  на оставшихся sentinel-инстансах. Sentinel помнит удалённый хост в runtime-state
  (`known-sentinel` + `known-replica`) и продолжает раз в секунду пытаться его
  резолвить и пинговать.
- `+sdown` (субъективный down) — **не выкидывает** пир из конфигурации автоматически.
  Sentinel просто отмечает "не отвечающим", но не забывает. В итоге хост остаётся
  в known-peers навсегда.
- DNS-имя удалённого хоста перестаёт резолвиться → бесконечный спам в лог и
  нагрузка на sentinel (вплоть до `+tilt` mode при перегрузке).

**Хронология в логе** (пример для удалённого rc-хоста):
1. `<дата добавления>` — `+sentinel sentinel <myid> <rc-host> 26379 @ <master> <master-host> 6379`
2. `<дата добавления>` — `+slave slave <rc-host>:6379 <rc-host> 6379 @ <master> <master-host> 6379`
3. `<дата удаления>` — `+sdown sentinel <myid> <rc-host> 26379 @ ...`
4. `<дата удаления>` — `+sdown slave <rc-host>:6379 <rc-host> 6379 @ ...`
5. С тех пор каждую секунду: `Failed to resolve hostname '<rc-host>'`

**Лечение**: выполнить `SENTINEL RESET <master-name>` на **каждом** оставшемся
sentinel-хосте кластера. Команда сбрасывает known-slaves и known-sentinels для
указанного мастера; sentinel пере-обнаруживает только живых пиров. Удалённый хост
никто не анонсирует → он выпадает из state.

См. `commands/sentinel_reset.md` — как получить имя мастера и пароль из `sentinel.conf`
и сформировать команду.

## Когда применять

- В `redis-sentinel.log` спам `Failed to resolve hostname` — найти и сбросить
  known-peers для мёртвого хоста.
- После удаления redis-хоста через mdb-data — убедиться, что sentinel-ы про него
  забыли (или забыть вручную через `SENTINEL RESET`).
- При разборе "почему кластер считает удалённый хост живым" — на самом деле не
  считает, просто не забыл. Это частый источник путаницы.
- Для инспекции любых событий failover / sdown / reboot в sentinel-логах.

## Что нужно

- **mcc** (`/Users/vl.ershov/Documents/mcc/mcc`) — для доступа к хостам. Используем
  **только `mcc scp`** — `mcc ssh` не принимает аргументы с пробелами/пайпами,
  `mcc ssh` с одним instance_name открывает интерактивную сессию.
- **Доступ к хостам кластера** — пользователь даёт список вида
  `1.db.<cluster>-cfs-redis.<dc>.one-infra.ru`. ДЦ могут быть: `ec`, `hc`, `kc`, `pc`,
  `dc`, `uc`, `rc` (rc — не продовый, на проде 4 ДЦ: hc/pc/uc/kc; ec/dc/rc — другие
  окружения).

## Структура скилла

- `SKILL.md` — этот файл, общее описание и известные проблемы.
- `commands/read_sentinel_logs.md` — как скачать и анализировать `redis-sentinel.log`,
  искать события known-peers, хронологию добавления/удаления хостов.
- `commands/sentinel_reset.md` — как из `sentinel.conf` получить имя мастера и
  sentinel-pass, сформировать команду `SENTINEL RESET`.

## Хосты и пути

| Что | Путь на хосте |
|---|---|
| Конфиги | `/etc/redis/` (sentinel.conf, redis.conf, acl/) |
| Логи | `/mnt/logs/dbms/` (redis.log, redis-sentinel.log, redis-server-systemd-service.log) |
| Sentinel state (dir) | `/mnt/redis/senti` (из `dir` в sentinel.conf) |

⚠️ Путь именно `/mnt/logs/dbms` (с 's' в `logs`), не `/mnt/log/dbms`. Опечатка приводит
к `failed to read downloaded archive header: EOF` при scp.

## mcc scp особенности

- **Скачивание директории**: `mcc scp "<host>:/etc/redis/" "<local_dir>/"` — работает,
  локальная директория назначения должна существовать (`mkdir -p` заранее).
- **Скачивание одиночного файла**: `mcc scp "<host>:/path/file" "<local_dir>/"` —
  локальный путь должен быть **директорией**, не путём к файлу. Иначе ошибка
  `failed to open destination directory`.
- **SSL Handshake is not finished** — туннель к minion-у не успел подняться. Просто
  повторить команду (бывает через 1-2 ретрая).
- **EOF на tar header** — файл не существует по указанному пути, либо опечатка в пути
  (например `/mnt/log/dbms` вместо `/mnt/logs/dbms`).
