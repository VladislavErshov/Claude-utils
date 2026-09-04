# Kafka controller downscale в ДЦ ic — test-modify3 — 2026-08-13

Успешный тест удаления controller из ДЦ `ic` в кластере `test-modify3` (mdbdev, kafka 3.8). Workflow `downscaleKafkaController` прошёл до конца после заливки трёх vault-секретов и PEM truststore.

## Кластер

| Поле | Значение |
|---|---|
| cluster_id | `9fc47c1b-011d-4aaa-b411-de5345a0204e` |
| name | test-modify3 |
| type | kafka |
| project | 160 (mdbdev) |
| namespace | 2 (infra), PMS-ключ `test-modify3-mdbdev-kafka.clouds` |
| environment | production |
| db_version | 3.8 (docker `ubuntu20-kafka-3.8.0:2.4.0`, cruise `1.0.7`) |
| hardware_preset | 100 (m.pico) |
| baseline version | 200764, status=scheduled |
| controllerDcs (исходно) | `["dc","hc","kc","ic"]` |
| hosts (исходно) | 3 broker (dc/hc/kc) + 1 cruise (hc) + 4 controller (dc/hc/kc/ic) |

## Что нужно для запуска локально (чеклист)

### 1. Seed БД из прода

См. `/Users/vl.ershov/.claude/skills/db-seed/` — единый SELECT через `jsonb_build_object` для: `db_cluster`, `db_cluster_version` (последние 3), `host_state`, `one_cloud_meta`, `operations` (последние 5), `projects`, `namespaces`, `hardware_presets`, `db_versions`, `db_version_dockers`.

Важно:
- `db_versions` не имеет колонки `version` — только `id`, `type`, `sharded`, `version_name`, `is_default`. См. `db-seed/history/gotchas-remote-schema.md`.
- `db_cluster_version.db_version` — jsonb, не FK.
- Baseline version поставить `status='scheduled'` (не `draft`).
- Все `operations.in_processing=false` (иначе 409 "Already has active or failed operation").

### 2. Vault-секреты в локальный `mdb-processing-vault` (`http://localhost:8200`, token `root`)

Путь: `zkv/mdb/mdbdev/kafka/test-modify3-mdbdev-kafka.mdbdev.db.production.mdb.prod/<secret>`, ключ `password` (DEFAULT_KEY в `CachedVaultPasswordService`).

Нужно три секрета (взять с прода `vault kv get -field=password ...`):
- `super` — Kafka superuser-пароль (для SASL auth admin client)
- `keystore-password` — пароль к JKS keystore
- `truststore-password` — пароль к JKS truststore

Заливка:
```bash
for s in super keystore-password truststore-password; do
  docker exec mdb-processing-vault sh -c \
    "VAULT_ADDR=http://localhost:8200 VAULT_TOKEN=root vault kv put \
    zkv/mdb/mdbdev/kafka/test-modify3-mdbdev-kafka.mdbdev.db.production.mdb.prod/$s password=<VALUE>"
done
```

### 3. PEM truststore для Kafka admin client (SASL_SSL)

`KafkaConnectionPropertiesConverter` ставит `ssl.truststore.type=PEM` и `ssl.truststore.location` из `app.kafka.namespaces.infra.ssl-truststore-location`. В `application.yaml` это поле пустое → Kafka client пытается читать PEM из пустого пути → `Is a directory`.

**Решение** — скачать CA-сертификат с прод-broker хоста и прописать путь локально:

```bash
mkdir -p /tmp/kafka-secrets
mcc --local -n infra scp 1.broker.test-modify3-mdbdev-kafka.dc.one-infra.ru:/opt/kafka/ssl/tls_ca.crt /tmp/kafka-secrets/
cp /tmp/kafka-secrets/tls_ca.crt ~/.mccloud/kafka-tls-ca.crt
```

Файл на broker-хосте: `/opt/kafka/ssl/tls_ca.crt` (PEM, ~4.5 KB, `O=VK LLC, CN=one-cloud Infrastructure Vault Certificate Authority`).

В `mdb-processing/src/main/resources/application-local.yaml` добавить:
```yaml
app:
  kafka:
    namespaces:
      infra:
        ssl-truststore-location: ${HOME}/.mccloud/kafka-tls-ca.crt
```

После изменения — перезапустить mdb-processing.

⚠️ **Это изменение нужно откатить перед коммитом** — это локальный workaround, не для prod-конфигурации.

### 4. PMS-сертификаты уже есть

mTLS для `pms.cloud.vk.team` берётся из `~/.mccloud/{client.cert,client.key,ca.crt}` — работает из коробки.

## Запуск

```bash
# Сброс старых failed-операций (иначе 409)
docker exec pg_backstage_plugin_mdb psql -U dev -d backstage_plugin_mdb -c \
  "DELETE FROM operations WHERE cluster_id='9fc47c1b-011d-4aaa-b411-de5345a0204e';"

# DELETE controller from DC ic
curl -X DELETE "http://localhost:8081/api/v2/mdb/kafka/clusters/9fc47c1b-011d-4aaa-b411-de5345a0204e/hosts/controllers?dc=ic"
# → HTTP 202, operation delete_hosts создана, workflow downscaleKafkaController запущен
```

## Workflow

`downscaleKafkaController` (cluster_id = workflow_id операции). Активности:

1. `cloud_getServiceInfo` ✓
2. `cloud_getInfosForServices` ✓
3. `getVariable` (kafka.controller.quorum) ✓
4. `removeControllerFromQuorum` ✓ → PMS `kafka.controller.quorum` потерял voter `15001@1.controller.test-modify3-mdbdev-kafka.ic.one-infra.ru:9093`
5. `kafka_host_getLeaderId` ✓ (тут падали до заливки vault-секретов и PEM truststore)
6. ... дальше workflow идёт на stop controller service в Cloud API + saveDownscaledKafkaControllersInfo

## Найденные проблемы и фиксы

| Шаг | Симптом | Причина | Фикс |
|---|---|---|---|
| `kafka_host_getLeaderId` (попытка 1) | vault 404 `{"errors":[]}` | В локальном vault нет секрета `super` для test-modify3 | Залить `super` с prod-vault |
| `kafka_host_getLeaderId` (попытка 2) | `Failed connection check ... [SASL_SSL, SASL_PLAINTEXT]`. SASL_SSL: `Failed to load PEM SSL keystore` / `Is a directory` | Пустой `ssl-truststore-location` + нет `keystore-password`/`truststore-password` в vault | Залить оба пароля + прописать PEM CA-сертификат |
| Workflow после фиксов | ✓ работает | — | — |

## Cleanup

```sql
DELETE FROM operations WHERE cluster_id='9fc47c1b-011d-4aaa-b411-de5345a0204e';
DELETE FROM host_state WHERE cluster_id='9fc47c1b-011d-4aaa-b411-de5345a0204e';
DELETE FROM db_cluster_version WHERE cluster_id='9fc47c1b-011d-4aaa-b411-de5345a0204e';
DELETE FROM one_cloud_meta WHERE cluster_id='9fc47c1b-011d-4aaa-b411-de5345a0204e';
DELETE FROM db_cluster WHERE id='9fc47c1b-011d-4aaa-b411-de5345a0204e';
```

Vault-секреты в `mdb-processing-vault` можно оставить — пригодятся для следующих тестов downscale/modify на test-modify3.

## Файлы

- Seed SQL: `/tmp/seed_test_modify3.sql`
- CA-сертификат: `~/.mccloud/kafka-tls-ca.crt` (также копия в `/tmp/kafka-secrets/tls_ca.crt`)
- Логи: `/tmp/mdb-data.log`, `/tmp/mdb-processing.log`
