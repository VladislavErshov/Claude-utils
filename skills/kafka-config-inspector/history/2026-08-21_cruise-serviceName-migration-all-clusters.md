# 2026-08-21 — Миграция serviceName cruise-control → cruise + PMS vault-pki.certs

## Контекст

Кластер fd812e93 (dzen-comments4) готовился к даунгрейду 4.3→3.8 (образец — test-downgrade7 23f108ac).
Выяснилось: в `one_cloud_meta` у части кластеров отсутствует запись `cruise-control-service`,
а у части — легаси `serviceName: "cruise-control"` вместо `"cruise"`. В PMS у легаси-кластеров
файл `vault-pki.certs` висит на ключе `cruise-control.<queue>.clouds`, правильный — `cruise.<queue>.clouds`.

## Прод-БД (backstage_plugin_mdb @ 1.db.mdb-etp-pgsql.pc.wan.idzn.ru, port-forward 53480)

### 1. Вставка недостающих cruise-записей (106 кластеров)

```sql
INSERT INTO one_cloud_meta (cluster_id, params, params_type)
SELECT m.cluster_id, jsonb_set(m.params - 'serviceName', '{serviceName}', '"cruise"'), 'cruise-control-service'
FROM one_cloud_meta m
JOIN db_cluster dc ON dc.id = m.cluster_id
WHERE dc.type = 'kafka' AND dc.deleted = false
  AND m.params_type = 'db-service'
  AND EXISTS (SELECT 1 FROM one_cloud_meta k WHERE k.cluster_id = m.cluster_id AND k.params_type = 'kafka-controller-service')
  AND NOT EXISTS (SELECT 1 FROM one_cloud_meta c WHERE c.cluster_id = m.cluster_id AND c.params_type = 'cruise-control-service');
-- INSERT 0 106 (102 production + 4 dev). Проверка: params broker/controller идентичны кроме serviceName у всех 106.
```

### 2. Переименование serviceName cruise-control → cruise (16 живых + 59 deleted)

16 целевых (production): aigen, core-blacklists-p, core-privacy-p, dzen-common-kafka, frontlogs,
kafka-ts-in, ml-platform-prod, ml-platform-test, ok, others, palantir, rum-transfer, tetrika-stage,
ucp-e2e, vkclips, vkfeeds.

⚠️ UPDATE без фильтра `deleted=false` задел ещё 59 soft-deleted (fake_id список в ERRORS.md от 2026-08-21).
Решение пользователя: НЕ откатывать. Урок: условия массового UPDATE обязаны дословно повторять SELECT согласования.

### 3. db_cluster_version fd812e93 → версия downgrade7 (строка 224001, draft/update)

kafka `ubuntu20-kafka-3.8.0:2.4.4→2.4.3` + cruise-control `ubuntu20-mdb-cruisecontrol-2.5.147:1.0.2` (id=9, versionName=3.8).

## PMS vault-pki.certs (app=ok-pyvault), ключ cruise-control.<q>.clouds → cruise.<q>.clouds

Решение пользователя: существующее содержимое кластера НЕ менять — копировать байт-в-байт
(даже если это легаси nginx-стиль: dir /etc/security/ssl, www-data, reload nginx).

| Кластер | ns | Действие | Итог |
|---|---|---|---|
| kafka-ts-in | infra | копия 252b | VERIFIED |
| aigen | infra | копия 252b | VERIFIED |
| palantir | infra | копия 252b | VERIFIED |
| rum-transfer | infra | копия 252b | VERIFIED |
| tetrika-stage | infra | копия 252b | VERIFIED |
| dzen-common-kafka | dzen | копия 277b | VERIFIED |
| ml-platform-prod | dzen | копия 247b | VERIFIED |
| ml-platform-test | dzen | копия 247b | VERIFIED |
| ucp-e2e | dzen | копия 277b | VERIFIED |
| frontlogs, vkfeeds, others | infra | уже идентичны | OK |
| ok | infra | уже мигрирован (старого ключа нет) | OK |
| vkclips | infra | ⚠️ оба ключа (cruise.* и cruise-control.*) установлены в ИСХОДНОЕ WAN-значение старого ключа (282b, alt-names с cloud_hostname_wan) — решение пользователя после отката моей первой синхронизации без WAN | VERIFIED |
| core-blacklists-p, core-privacy-p | vkontakte | создать НЕ удалось: update.do → ACCESS_DENIED (у vl.ershov нет прав на запись в vkontakte::ok-pyvault). Чтение работает. Содержимое (252b, от staging-близнеца cruise.core-privacy-s-vkontakte-kafka.clouds) — ждать прав или создать через web-UI | PENDING |

## Грабли (добавлены в SKILL.md)

- PMS write-API: POST /api/conf/update.do, JSON {applicationName, hostName, propertyName, propertyValue, userComment}, HTTP 200 + тело "0" = успех. Перед записью — спрашивать пользователя.
- values.do ловит rate-limit на серию запросов (HTML → jq parse error): один fetch на namespace, ключи разбирать локально.
- zsh не сплитит `$var` в `set -- $var` — массовые циклы через bash-файл.
- Копирование байт-в-байт: `jq -j '.["<key>"]'` (без \n) + `jq -n --rawfile val <file>`.
- Namespace `.vkcl.ru`-кластеров = `vkontakte` (НЕ `vkcl` → HTTP 400). Источник: таблица namespaces в БД (`config: {"domain":"vkcl"}`).
