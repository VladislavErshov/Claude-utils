# MDBSUP-5226 — createKafkaCruise 404: образ вычищен из registry, снапшот dockers устарел

Дата: 2026-09-08
Кластер: `frontlogs-adtech-kafka` (`dbee9a79-1edf-41ca-b194-3f25cc1d425a`), Kafka 3.8, namespace **infra** (fullQueue `frontlogs-adtech-kafka.adtech.db.production.mdb.prod`), registry при этом `dzen-external-registry.odkl.ru` — образы MDB для adtech живут в dzen-registry независимо от namespace кластера.

## Симптомы

- Операция `create_additional_service` (createKafkaCruise), 2 попытки → failed, attempts_left=0, in_processing=true, error_message=`Could not execute request` (без деталей).
- В onecloud 404: `Image manifest dzen-external-registry.odkl.ru/ubuntu20-mdb-cruisecontrol:2.1.0 not found in registry`.
- В `db_version_dockers` для db_version 9 (Kafka 3.8) образ другой: `ubuntu20-mdb-cruisecontrol-2.5.147:1.0.2`.

## Корень проблемы

Операция createKafkaCruise берет образ из **jsonb-снапшота** `db_cluster_version.db_version->'dockers'`, а не из нормализованной `db_version_dockers`. Снапшоты пишутся при создании строки версии и потом **не пересинхронизируются**: в `db_version_dockers` образы обновили (схема именования сменилась: было `ubuntu20-mdb-cruisecontrol:<build>`, стало `ubuntu20-mdb-cruisecontrol-<cc-version>:<build>`), а снапшоты остались со старыми именами. Параллельно старые теги вычистили из registry → любой новый createKafkaCruise с устаревшим снапшотом получает 404.

Варианты имен, найденные в снапшотах до фикса: `ubuntu20-mdb-cruisecontrol:1.0.x/2.x` (много), `ubuntu20-mdb-cruisecontrol-2.5.141:1.0.8`, `...-2.5.147:1.0.0/1.0.1` — все неэталонные.

## Диагностика (быстрый чеклист)

```sql
-- статус операции
SELECT id, status, attempts_left, in_processing, left(coalesce(error_message,''),200)
FROM operations WHERE id='<op_id>';

-- что в снапшоте vs эталон (кластер)
SELECT v.id, v.status, v.type, v.db_version->'dockers'
FROM db_cluster_version v WHERE v.cluster_id='<cluster_id>' ORDER BY create_ts DESC;

-- эталон
SELECT * FROM db_version_dockers WHERE docker_type='cruise-control';
```

## Фикс

Идемпотентный UPDATE: все CC-записи в `dockers` заменить на одну целевую; строки, где CC уже правильный, не трогаются. Правки only по строкам, где есть CC-запись — CC не добавляется версиям, где его не было.

```sql
UPDATE db_cluster_version v
SET db_version = jsonb_set(
      v.db_version, '{dockers}',
      (SELECT jsonb_agg(d) FROM jsonb_array_elements(v.db_version->'dockers') d
       WHERE d->>'dockerType' <> 'cruise-control')
        || '[{"dockerTag":"1.0.2","dockerName":"ubuntu20-mdb-cruisecontrol-2.5.147","dockerType":"cruise-control"}]'::jsonb,
      true),
    update_ts = now()
WHERE EXISTS (SELECT 1 FROM db_cluster c WHERE c.id=v.cluster_id AND c.type='kafka')
  AND EXISTS (SELECT 1 FROM jsonb_array_elements(v.db_version->'dockers') d
              WHERE d->>'dockerType'='cruise-control'
                AND (d->>'dockerName', d->>'dockerTag') <> ('ubuntu20-mdb-cruisecontrol-2.5.147','1.0.2'));
```

2026-09-08 выполнено на проде: точечно frontlogs (7 строк) + массово по всем Kafka (UPDATE 3014: 3.8 — 2999, 4.3 — 15). Верификация: 3228/3228 CC-строк на целевом образе. История операций (`operations`) не трогалась — перезапуск руками из UI.

## Грабли

- **`one_cloud_meta` не содержит образов** — только queue/serviceName/domain. Не искать там.
- **Снапшот ≠ эталон:** `db_cluster_version.db_version` — денормализация; авторитет — `db_version_dockers`. При 404 на образ всегда сверять оба.
- **error_message в operations (`Could not execute request`) не содержит реальной ошибки** — реальные детали только в onecloud/операторе.
- **adtech/infra кластеры тянут образы из `dzen-external-registry.odkl.ru`** — это норма, не признак неверного namespace.
- Схема именования CC-образов: `ubuntu20-mdb-cruisecontrol-<CC-version>:<build>` (эталон `2.5.147:1.0.2`). Старые `ubuntu20-mdb-cruisecontrol:<build>` из registry удалены.
- **Продуктовый баг:** снапшот `db_cluster_version.db_version->'dockers'` не пересинхронизируется с `db_version_dockers` при обновлении образов версии — см. предложенный баг-тикет.
