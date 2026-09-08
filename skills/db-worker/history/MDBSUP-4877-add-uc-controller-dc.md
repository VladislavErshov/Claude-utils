# MDBSUP-4877 — добавить DC UC в controllerDcs кластера channel-info

Кластер: `c765ffd7-79b7-4f62-9386-db33bac8f030` (channel-info, Kafka 3.8).
Задача: добавить третий ДЦ `uc` в раздел контроллеров ДЦ в таблице `db_cluster_version` на проде.

## Что было на проде

Актуальные scheduled-версии с controllerDcs `["kc","pc"]`:
- `234083` — type update_instances (2026-08-25)
- `235719` — type create_additional_service (2026-08-26)

Устаревшие версии (2610, 2616, 2693, 2874, 2876, 2891, 2985 — с `hc`) не трогали.

## Выполненный UPDATE (прод, port-forward 53480)

```sql
UPDATE db_cluster_version
SET cluster_params = jsonb_set(cluster_params, '{kafkaParams,controller,controllerDcs}', '["kc","pc","uc"]'),
    hosts_params   = jsonb_set(hosts_params, '{units}', '[{"dc":"kc","shard":null},{"dc":"pc","shard":null},{"dc":"uc","shard":null}]'::jsonb)
WHERE id IN (234083, 235719);
-- UPDATE 2, проверено SELECT-ом: dcs ["kc","pc","uc"], units содержит uc
```

## Заодно

Скилл `db-seed` переименован в `db-worker` (папка + `name:` в SKILL.md).
