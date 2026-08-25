# Hot-deploy чекера `check_kafka.py` + верификация

Команда для быстрой проверки гипотезы: правим `check_kafka.py` локально в
`docker-images/ubuntu20-kafka-base/rootfs/etc/host_checker/checks/check_kafka.py`, заливаем
на хост тестового кластера без пересборки образа, перезапускаем `rscheck@kafka.service`,
проверяем что изменения подхвачены.

## Что делает чекер

`check_kafka.py` — Python-модуль, который `rscheck@kafka` дёргает каждые 10 сек (см.
`/etc/rscheck/kafka.conf.j2`, `kafka-availability` thread). Чекер:

1. Читает `node.id` и `process.roles` из `/opt/kafka/config/broker.properties` (или
   `controller.properties`).
2. Опрашивает Kafka через Jolokia (порт 7777) — MBean'ы `kafka.server:type=raft-metrics` /
   `kafka.server:name=BrokerState,type=KafkaServer` (зависит от версии Kafka).
3. Мапит KRaft-роль в display-имя для mdb-data UI:
   - `observer` → `broker`
   - `broker` → `broker`
   - `leader` → `leader-controller`
   - `follower` → `follower-controller`
4. Шлёт `HostDto` в backstage через `BackstageClient.send_info()`.

UI mdb-data берёт эти данные и показывает роль хоста.

## Файлы в образе

| Файл | Назначение |
|---|---|
| `rootfs/etc/host_checker/checks/check_kafka.py` | основной чекер |
| `rootfs/etc/host_checker/host_checker_config.ini.j2` | конфиг host-check.service |
| `rootfs/etc/rscheck/kafka.conf.j2` | конфиг rscheck@kafka (CheckKafka, interval=10, timeout=3) |
| `rootfs/etc/rscheck/modules/` | Python-модули: `backstage_client.py`, `backstage_utils.py` |

На хосте те же файлы лежат в `/etc/host_checker/checks/` и `/etc/rscheck/`.

## Шаг 1: правка локально

```bash
cd /Users/vl.ershov/Documents/Git/docker-images
$EDITOR ubuntu20-kafka-base/rootfs/etc/host_checker/checks/check_kafka.py
```

## Шаг 2: залить на хост

Доступ к хосту — через скилл [`mcc-host-worker`](../../mcc-host-worker/SKILL.md).

Специфика hot-deploy чекера: dest **всегда директория** (`/etc/host_checker/checks/`),
а перед заливкой нужно удалить старый файл (и возможную случайную директорию, если ранее
scp создал её с именем файла).

```bash
HOST=1.broker.test-<cluster>-mdbdev-kafka.dc.one-infra.ru
FILE=ubuntu20-kafka-base/rootfs/etc/host_checker/checks/check_kafka.py

# 2a. Удалить старый файл (и случайную директорию, если ранее scp создал её).
#     Зайти на хост через скилл mcc-host-worker (команда ssh) и выполнить:
rm -rf /etc/host_checker/checks/check_kafka.py

# 2b. Скопировать через скилл mcc-host-worker (команда scp, dest = директория!):
#     scp "$FILE" "$HOST:/etc/host_checker/checks/"
```

## Шаг 3: верификация + рестарт rscheck

Зайти на хост через скилл [`mcc-host-worker`](../../mcc-host-worker/SKILL.md)
и выполнить:

```bash
ls -la /etc/host_checker/checks/check_kafka.py
grep -c ROLE_DISPLAY /etc/host_checker/checks/check_kafka.py
systemctl restart rscheck@kafka && systemctl is-active rscheck@kafka
```

Что проверяем:
- `ls -la` — файл лежит, размер совпадает с локальным, **это файл, не директория**.
- `grep -c ROLE_DISPLAY` — счётчик совпадает ожидаемому (проверка что залитая версия = локальная).
- `systemctl is-active` — `active` после рестарта. Если `failed` — смотреть
  `journalctl -u rscheck@kafka --no-pager | tail -30`.

## Шаг 4: проверка что UI mdb-data обновился

`rscheck@kafka` шлёт данные в backstage каждые 10 сек. Подождать 10-30 сек, обновить страницу
хоста в mdb-data.

Если роль не обновилась:
- Проверить лог rscheck на хосте: `journalctl -u rscheck@kafka -n 50 --no-pager`.
- Проверить что `BackstageClient.send_info()` не падает — может быть проблема с auth/URL.
- Backstage кэширует данные; подождать ещё 30 сек.

## Шаг 5: если гипотеза подтвердилась

```bash
cd /Users/vl.ershov/Documents/Git/docker-images
git add ubuntu20-kafka-base/rootfs/etc/host_checker/checks/check_kafka.py
git commit -m "<MDBDEV-XXXX> <краткое описание>"
```

Деплой в тестовый/прод-кластер идёт через CI `docker-images` — отдельный цикл (full rebuild),
см. `commands/build_image.md` и `commands/deploy_image.md` (TODO).

## Частые грабли

- **`scp` с путём-файлом создаёт директорию** — см. выше. Симптом: `grep` падает с
  `Is a directory`, rscheck в логе: `ModuleNotFoundError` / `ImportError`.
- **Файл не перезаписывается, но scp выходит с 0** — у scp нет ошибки, но размер/timestamp на
  хосте не меняется. Решение: `rm -f` + scp (шаг 2a).
- **`rscheck@kafka` перезапустился, но роль в UI не обновилась** — backstage кэширует данные;
  подождать 10-30 сек, обновить страницу mdb-data.
- **MBean `kafka.server:type=raft-metrics/current-state` отсутствует** — удалён в Kafka 4.x.
  Использовать `kafka.server:name=BrokerState,type=KafkaServer`. Подробности —
  `/kafka-cluster-inspector/commands/diagnose_broker_dead.md`.

## Залить чекер сразу на несколько хостов

Для кластера с несколькими брокерами/контроллерами — повторить шаги 2-3 для каждого хоста.
Скрипт-обёртка (положить в `bin/` когда понадобится): принимает список хостов, для каждого
делает rm + scp + restart.

## Связанные команды

- `/kafka-cluster-inspector` `commands/run_commands.md` — выполнение команд на хостах через
  скилл `mcc-host-worker` (`mcc ssh`), сложные команды с кавычками, heredoc-трюк.
- `/kafka-config-inspector` — сверка что PMS-API значения физически применились в
  `/opt/kafka/config/` (не для чекеров, но полезно после передеплоя образа).
