# Правка systemd-юнитов (TODO)

⚠️ **Заглушка.** Заполнится по мере использования скилла.

## Scope

Файлы в `rootfs/etc/systemd/system/`:

- `kafka-broker.service` — сервис Kafka-брокера
- `kafka-controller.service` — сервис KRaft-контроллера
- `kafka-exporter.service` — kafka-exporter (consumer metrics)
- `logrotate.timer`

## Цикл

Systemd-юниты требуют `systemctl daemon-reload` после замены. Hot-reload на хосте:

1. Через скилл [`mcc-host-worker`](../../mcc-host-worker/SKILL.md) (`mcc scp`) скопировать
   файл в `/etc/systemd/system/` (dest = директория).
2. `systemctl daemon-reload`.
3. `systemctl restart <unit>`.

Но если юнит меняет env/exec — нужен full rebuild образа и передеплой.

## Что задокументентировать когда понадобится

- Разница между hot-reload и full rebuild для systemd-юнитов.
- Связь `kafka-*.service` с `/etc/sysconfig/kafka` (env) — отдельно через confp.
- Рестарт-политики, ExecStartPre (`pre-start-kafka-*.sh.j2`).
