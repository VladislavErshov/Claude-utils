# `mcc ops` — проверка one-cloud-ops

```bash
mcc --local -n <namespace> -c <dc> ops <cluster-id-or-name>
```

Без `-n <namespace>` падает `NamespaceMissingException`.

## Маркеры ответа

- `EntityNotFoundException: Partition <cluster> is not managed by both one-cloud-ops and ops-temporal`
- `Not found ops by namespace <ns>`
- `Failed to create one-cloud client: Namespace cannot be resolved from <ns>`
- `dial tcp: lookup cdb.cloud-ops.clouds.vkcl.ru: i/o timeout` — с бекстейджа/локальной
  машины DNS может не резолвиться для namespace `vkontakte`. Запускать mcc с хоста, у
  которого есть доступ.
