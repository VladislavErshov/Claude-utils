# MDBDEV-2098: DB Seed Data для тестирования UpdateKafkaClusterWorkflow

## Порядок вставки

`namespaces` → `projects` → `db_cluster` → `one_cloud_meta` → `host_state` → `db_cluster_version` → `settings`

## Namespace

```sql
INSERT INTO namespaces (id, name, config, active, params)
VALUES (2, 'infra', '{"domain": "one-infra"}', true, '{"projects": [...]}')
ON CONFLICT (id) DO UPDATE SET name='infra', config=EXCLUDED.config;
```

**Проверка**: namespace `infra` должен быть в `app-config.mdb.local.yaml` → `backend.mdb.namespaces.infra`, иначе бэкенд не стартует.

## Project

```sql
INSERT INTO projects (id, name, product_id, idm_synced, queue_synced)
VALUES (160, 'mdbdev', 4819, true, true)
ON CONFLICT (id) DO UPDATE SET name='mdbdev';
```

## db_cluster

```sql
INSERT INTO db_cluster (id, type, create_ts, update_ts, create_by, name, project_id, deleted, sharded, namespace_id, environment, criticality)
VALUES (
  '33048de4-cdb5-4581-9d54-3d05777bfb60',
  'kafka',
  '2026-05-06 20:39:15.335', '2026-05-06 20:39:15.335',
  'vl.ershov',
  'test-cruise-cpu2',
  160, false, false, 2, 'production', 'D'
)
ON CONFLICT (id) DO UPDATE SET type='kafka', name='test-cruise-cpu2', project_id=160, namespace_id=2;
```

## one_cloud_meta

Три записи: broker, controller, cruise-control.

```sql
INSERT INTO one_cloud_meta (cluster_id, params, params_type)
VALUES
  ('33048de4-cdb5-4581-9d54-3d05777bfb60',
   '{"isWan": false, "queue": "test-cruise-cpu2-mdbdev-kafka", "domain": "one-infra", "fullQueue": "test-cruise-cpu2-mdbdev-kafka.mdbdev.db.production.mdb.prod", "rootQueue": "prod", "serviceName": "broker"}',
   'db-service'),
  ('33048de4-cdb5-4581-9d54-3d05777bfb60',
   '{"isWan": false, "queue": "test-cruise-cpu2-mdbdev-kafka", "domain": "one-infra", "fullQueue": "test-cruise-cpu2-mdbdev-kafka.mdbdev.db.production.mdb.prod", "rootQueue": "prod", "serviceName": "controller"}',
   'kafka-controller-service'),
  ('33048de4-cdb5-4581-9d54-3d05777bfb60',
   '{"isWan": false, "queue": "test-cruise-cpu2-mdbdev-kafka", "domain": "one-infra", "fullQueue": "test-cruise-cpu2-mdbdev-kafka.mdbdev.db.production.mdb.prod", "rootQueue": "prod", "serviceName": "cruise-control-service"}',
   'cruise-control-service')
ON CONFLICT DO NOTHING;
```

**Ключевые поля для processBrokerConfigUpdate**:
- `params->queue` → используется как `pmsHostName` = `{queue}.clouds`
- `params->fullQueue` → используется в downstream задачах

## host_state

3 брокера + 3 контроллера + 1 cruise = 7 хостов.

```sql
INSERT INTO host_state (id, cluster_id, host, update_ts, params, shard_id)
VALUES
  (86815, '33048de4-cdb5-4581-9d54-3d05777bfb60', '1.broker.test-cruise-cpu2-mdbdev-kafka.dc.one-infra.ru', '2026-05-06 20:39:18.107', '{"dc": "dc"}', NULL),
  (86816, '33048de4-cdb5-4581-9d54-3d05777bfb60', '1.broker.test-cruise-cpu2-mdbdev-kafka.uc.one-infra.ru', '2026-05-06 20:39:18.107', '{"dc": "uc"}', NULL),
  (86817, '33048de4-cdb5-4581-9d54-3d05777bfb60', '1.broker.test-cruise-cpu2-mdbdev-kafka.ic.one-infra.ru', '2026-05-06 20:39:18.107', '{"dc": "ic"}', NULL),
  (86818, '33048de4-cdb5-4581-9d54-3d05777bfb60', '1.cruise.test-cruise-cpu2-mdbdev-kafka.dc.one-infra.ru', '2026-05-06 20:39:18.676', '{"dc": "dc"}', NULL),
  (86819, '33048de4-cdb5-4581-9d54-3d05777bfb60', '1.controller.test-cruise-cpu2-mdbdev-kafka.dc.one-infra.ru', '2026-05-06 20:39:19.509', '{"dc": "dc"}', NULL),
  (86820, '33048de4-cdb5-4581-9d54-3d05777bfb60', '1.controller.test-cruise-cpu2-mdbdev-kafka.uc.one-infra.ru', '2026-05-06 20:39:19.509', '{"dc": "uc"}', NULL),
  (86821, '33048de4-cdb5-4581-9d54-3d05777bfb60', '1.controller.test-cruise-cpu2-mdbdev-kafka.ic.one-infra.ru', '2026-05-06 20:39:19.509', '{"dc": "ic"}', NULL)
ON CONFLICT (id) DO UPDATE SET cluster_id=EXCLUDED.cluster_id, host=EXCLUDED.host, params=EXCLUDED.params;
```

**Ключевое**: поле `host` (FQDN) маппится в DTO `Host.fqdn`, которое попадает в `brokers[]` запроса к Processing.

## db_cluster_version (базовая версия — без brokerConfig)

```sql
INSERT INTO db_cluster_version (id, cluster_id, cluster_params, create_ts, create_by, status, type, update_ts, attempts, hosts_params, hardware_preset_id, db_version)
VALUES (90770, '33048de4-cdb5-4581-9d54-3d05777bfb60',
'{"acl": {}, "name": "test-cruise-cpu2", "isWan": false, "lanIn": 10, "diskGb": 8, "lanOut": 20, "diskType": "nvme", "projectId": 160, "rootQueue": "prod", "kafkaParams": {"controller": {"controllerDcs": ["dc", "uc", "ic"], "controllerLanIn": 15, "controllerMemGb": 4, "controllerDiskGb": 10, "controllerLanOut": 50, "controllerVcores": 4, "controllerDiskType": "nvme", "controllerJvmHeapSizeMb": "1024"}, "cruiseControl": {"cruiseControlDc": "dc", "cruiseUserPassword": ""}, "jvmHeapSizeMb": 1024}, "needLanIpv6": true, "needWanIpv4": false, "needWanIpv6": false}',
'2026-05-06 20:39:15.344', 'vl.ershov', 'scheduled', 'new', '2026-05-06 20:39:15.344', 3,
'{"units": [{"dc": "dc"}, {"dc": "uc"}, {"dc": "ic"}]}',
30,
'{"id": 9, "dockers": [{"dockerTag": "1.0.7", "dockerName": "ubuntu20-mdb-cruisecontrol", "dockerType": "cruise-control"}, {"dockerTag": "2.3.1", "dockerName": "ubuntu20-kafka-3.8.0", "dockerType": "service"}], "sharded": false, "versionName": "3.8"}')
ON CONFLICT (id) DO UPDATE SET cluster_params=EXCLUDED.cluster_params, status=EXCLUDED.status;
```

**Ключевое**: `cluster_params` НЕ содержит `brokerConfig`. Новая версия (создаваемая через API) должна содержать `brokerConfig.config` — это триггерит `hasBrokerConfigChanges() === true`.

## settings (feature flag для workflow-пути)

```sql
INSERT INTO settings (type, value)
VALUES ('kafkaResizeProcessingEnabledProjects', 'mdbdev')
ON CONFLICT DO NOTHING;
```

Без этой записи задача `start_resize_kafka_workflow` не создаётся — идёт старый путь через CloudOps operator tasks.

## Сброс sequences

```sql
SELECT setval('db_cluster_version_id_seq', (SELECT COALESCE(MAX(id), 1) FROM db_cluster_version));
SELECT setval('host_state_id_seq', (SELECT COALESCE(MAX(id), 1) FROM host_state));
SELECT setval('settings_id_seq', (SELECT COALESCE(MAX(id), 1) FROM settings));
```

## Особенности

1. **namespace из БД** обязан быть в `app-config.mdb.local.yaml`, иначе `Missing config value backend.mdb.namespaces.<name>.vault.token`
2. **hardware_preset_id** (30) должна существовать в таблице `hardware_presets`
3. **db_version** — JSON-объект с `id` и `dockers`, не просто номер версии
4. **hosts_params.units** — массив с DC для хостов, соответствует `host_state` записям
5. **ON CONFLICT DO UPDATE** — используй для idempotent-вставок
