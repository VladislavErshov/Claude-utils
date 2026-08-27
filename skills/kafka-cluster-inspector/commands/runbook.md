# Дежурный ранбук Kafka

**Канон дежурной инструкции — Confluence «Дежурство MDB: Kafka» (SSOT)**:
https://confluence.vk.team/pages/viewpage.action?pageId=1348619075
— секции «Описание/Важное», «Доступность кластера», «Логи», «Порты», «Про топики,
пользователей и acl», «Kafkactl» (конфиг kctl.yaml, примеры), «Встреча по кафке» (тайминги).
Вики живая — править там, сюда копии не тащить.

Конкретные проблемы — `troubleshooting.md`, операции с Cruise Control — `cruise_control_ops.md`,
рутинное администрирование (топики/ACL/users) — `administration.md`.

## Наши дополнения к вики

- Логи `/mnt/logs/dbms`: скачивание/анализ через скилл
  [`mcc-host-worker`](../../mcc-host-worker/SKILL.md) (`mcc ssh`/`mcc scp`),
  разбор маркеров — скилл `kafka-log-investigator`.
- Kafkactl — установка на хост брокера через [`mcc-host-worker`](../../mcc-host-worker/SKILL.md)
  (`scp` tar-архива в `/opt/kafka/config` + `ssh`), хосты в `kctl.yaml` вида
  `1.broker.<cluster>.<dc>.one-infra.ru:9092`, пароль `super` — из `/opt/kafka/config/jaas.conf`.
