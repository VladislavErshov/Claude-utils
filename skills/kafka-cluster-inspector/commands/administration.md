# Администрирование Kafka

**Канон — Confluence «Дежурство MDB: Kafka», секция «Администрирование»** (SSOT):
https://confluence.vk.team/pages/viewpage.action?pageId=1348619075

Покрывает: перед началом (`client.properties` для VK-кластеров), проверка видимости
брокеров (`kafka-metadata-quorum.sh`), удаление контроллера, Unregister, пользователи
(+квоты), топики (включая UNCLEAN перевыборы), запись и чтение в топики, ACL,
consumer groups. Вики живая — править там.

Каталог известных проблем — `known_issues.md`; операторные операции (вручную по шагам
оператора, downscale-broker) — `one_cloud_ops.md`.

## Наши дополнения к вики

### Unregister (выжимка — используется из one_cloud_ops.md)

```bash
/opt/kafka/bin/kafka-cluster.sh unregister --bootstrap-server $cloud_hostname:9092 \
  --config /opt/kafka/config/client.properties --id <broker-id>
```

Вариант single-file Java AdminClient — [one_cloud_ops.md](one_cloud_ops.md)
(скрипты-образцы в [history/MDBSUP-4895](../history/MDBSUP-4895-2026-08-26.md)).
