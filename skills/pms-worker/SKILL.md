---
name: pms-worker
description: Работа с PMS (pms.cloud.vk.team) — чтение и запись property через mTLS API (values.do/update.do), namespaces (infra/dzen/vkontakte), PMS-ключи хостов (queue.clouds / controller.queue.clouds / host-mdb), скрипт pms-read.sh, грабли (rate-limit, байт-в-байт, zsh). Плюс настройки warnings MDB в PMS — health.prod.rtconfig.warnings (включение чеков mdb-health) и data.prod.rtconfig.warnings.cluster/cluster_list/db_list (отображение в UI через Pebble-шаблоны mdb-data: enabledWarningTypes, templates, isRed). Используй при запросах «прочитай/поменяй PMS-переменную», «включи чек», «настрой отображение варнингов», «что в rtconfig».
---

# Скилл работы с PMS (Property Management System)

PMS — хранилище property для сервисов MDB: конфиги кластеров (`kafka.*`, `zen.*`),
rtconfig-настройки сервисов (`health.prod.rtconfig.*` для mdb-health,
`data.prod.rtconfig.*` для mdb-data). Значения рендерятся на хосты через confp
(см. скиллы-инспекторы) или читаются сервисами напрямую.

## Когда применять

- Прочитать/изменить PMS-переменную любого MDB-сервиса
- Включить/выключить чек mdb-health, тумблер для группы проектов
- Настроить отображение warnings в UI (тексты, шаблоны, красность)
- Разобраться «почему конфиг на хосте не совпал с PMS» (сами хосты — mcc-host-worker)

## Что нужно

- mTLS-сертификаты в `~/.mccloud/` (`client.cert`, `client.key`, `ca.crt`)

## Web-UI (посмотреть глазами)

```
https://pms.cloud.vk.team/client/#/props-search?ns=<namespace>&a=mdb&h=<host>&s=<search>&n=<filter>
```
- `ns` — namespace (`infra` / `dzen` / `vkontakte`), `a` — application (почти всегда `mdb`),
  `h` — PMS-ключ хоста, `s` — подстрока поиска, `n` — фильтр по имени.
- Пример warnings: `...?ns=infra&a=mdb&h=host-mdb&s=mdb%2Fdata.prod.rtconfig.warnings.cluster&n=warning`

## Модель данных

`namespace / application / host (PMS-ключ) / property → value`

Типичные PMS-ключи хостов:

| Ключ | Что лежит |
|---|---|
| `<queue>.clouds` | Переменные кластера (брокерские + все controller-настройки, кроме sysconfig) + cruise |
| `controller.<queue>.clouds` | **Только** `kafka.sysconfig` для controller-хостов (грабля: остальные controller-переменные на брокерском ключе!) |
| `host-mdb` | Сервисные rtconfig-настройки mdb-health / mdb-data (`health.prod.rtconfig.*`, `data.prod.rtconfig.*`) |

FQDN → ключ (Kafka): `1.broker.<queue>.<dc>.one-infra.ru → <queue>.clouds`,
`1.controller.<queue>... → controller.<queue>.clouds`, cruise делит ключ с broker.
Скрипт `bin/pms-read.sh` конвертирует автоматически.

## Чтение: values.do

```bash
curl -s --cert ~/.mccloud/client.cert --key ~/.mccloud/client.key --cacert ~/.mccloud/ca.crt \
  -H "x-namespace: infra" \
  "https://pms.cloud.vk.team/api/conf/values.do?application=mdb&property=<property>"
# → { "<host>": "<value>" } — карта host → значение; {} = свойства нет
```

### Скрипт pms-read.sh

```bash
# Одна переменная:
~/.claude/skills/pms-worker/bin/pms-read.sh <host-or-fqdn> kafka.sysconfig [namespace] [application]
# Список через запятую (произвольные свойства):
~/.claude/skills/pms-worker/bin/pms-read.sh host-mdb "health.prod.rtconfig.warnings,data.prod.rtconfig.warnings.cluster" infra mdb
# Пусто → дефолтный список известных Kafka-переменных (19 шт.)
~/.claude/skills/pms-worker/bin/pms-read.sh 1.broker.<queue>.<dc>.one-infra.ru "" infra mdb
```

## Запись: update.do — ТОЛЬКО после явного подтверждения пользователя

⚠️ **ПРАВИЛО: перед КАЖДОЙ записью покажи пользователю что, куда и какое значение.**
Рутинный путь изменения `kafka.*` — modify-флоу mdb-processing; ручная запись — для
миграций/тумблеров/копий ключей.

```bash
curl -s --cert ~/.mccloud/client.cert --key ~/.mccloud/client.key --cacert ~/.mccloud/ca.crt \
  -H "x-namespace: <namespace>" -H "Content-Type: application/json" \
  -X POST "https://pms.cloud.vk.team/api/conf/update.do" \
  -d '{
    "applicationName": "mdb",
    "hostName": "<pms-ключ>",
    "propertyName": "<property>",
    "propertyValue": "<точное значение>",
    "userComment": "что и зачем"
  }'
# HTTP 200 + тело "0" = успех. Верифицировать повторным values.do байт-в-байт.
```

## Namespaces

| Namespace | Признак | Заметки |
|---|---|---|
| `infra` | FQDN `*.one-infra.ru` | Дефолт |
| `dzen` | FQDN `*.idzn.ru` | Без явного ns — пусто/`<NOT_SET>` |
| `vkontakte` | FQDN `*.vkcl.ru` | НЕ `vkcl` (400). На запись ACCESS_DENIED у обычного mdb-аккаунта — права просить в web-UI |

Namespace кластера лежит в БД: `SELECT ns.name FROM db_cluster dc JOIN namespaces ns ON ns.id = dc.namespace_id WHERE dc.id = '<cluster_id>'`.

## Грабли

- **Rate-limit**: серия запросов → HTML 429/5xx. Делай ОДИН values.do на namespace
  и разбирай ключи локально jq-ем, не дёргай по разу на каждый ключ.
- **Копирование значения — байт-в-байт**: `jq -j '.["<key>"]'` (`-j` без `\n`), затем в тело
  через `jq -n --rawfile val <file>`.
- zsh не сплитит `$var` — массовые циклы через bash-скрипт.
- Изменение PMS ≠ применение на хосте: нужен confp-рендер/рестарт (см. скилл нужной СУБД).

## Варнинги MDB в PMS

Пайплайн: чеки mdb-health → таблица `cluster_warnings` (UNIQUE `cluster_id + warning_type`,
дедуп по warningType — **один тип = максимум один варнинг на кластер**) → mdb-health `/warnings`
ручки → mdb-data `WarningsFacade` (фильтр enabledWarningTypes + criticality) → Pebble-рендер
`WarningTemplateRenderer` → `/api/v2/warnings` → UI.

### Воркфлоу: как добавить и раскатать новый варнинг

1. **Код чека** (mdb-health, `warnings/check/`): уникальный `warningType`, в `warningParams`
   максимум полей (для шаблонов), в `warningMessage` рабочий русский текст — он же fallback
   для UI. Один тип = одна карточка: если нужны отдельные сообщения — делай отдельные типы
   (ветка в одном шаблоне тоже работает, но не даёт отдельных карточек/включателей).
2. **Включение вычислений** — `health.prod.rtconfig.warnings` (ключ `host-mdb`): добавить
   `<checkName>`-блок. Раскат: сначала `enabled=false` + `forceEnableProjectIds=[<тестовый проект>]`,
   проверить на тестовом, потом `enabled=true` (+ `dbTypes`).
3. **Включение отображения** — `data.prod.rtconfig.warnings.{cluster,cluster_list,db_list}`
   (все три!): добавить тип в `enabledWarningTypes`. Без шаблона UI покажет `warningMessage`
   из кода — этого достаточно для старта; красивый `templates.<type>` можно докрутить потом.
4. **Точечный показ на тестовых проектах**: в display-настройках есть `forceEnabled`
   (`projectIds` + свои `dbTypes`/`criticality`/`enabledWarningTypes`/шаблоны с префиксом
   `forceEnabled.`) — работает пока `enabled=false` глобально.
5. **Изменение значения** — через update.do (правила выше: подтверждение, верификация
   байт-в-байт; значение — многострочный YAML/HOCON, копировать через `jq -j`).
6. **Локальное тестирование отображения** — скилл `mdb-local-tester` (локальный mdb-data
   читает `data.testing.rtconfig.*`; полный стэк UI: Backstage 7007 + vkone-stub 8090).

Два независимых слоя настроек, оба на ключе `host-mdb` (ns=infra, app=mdb):

### 1. `health.prod.rtconfig.warnings` — включение чеков (mdb-health)

```json
{
  "enabled": true,
  "enabledDbTypes": ["postgresql", "kafka", "..."],
  "kafkaQuorum": {          // <имя чека>: enabled / dbTypes / forceEnableProjectIds
    "enabled": true,        // глобальный тумблер
    "forceEnableProjectIds": [195, 160],   // принудительно для проектов
    "dbTypes": ["kafka"]
  }
}
```
Раскладка вычислений: `enabled=true` → для всех dbTypes из списка; иначе только
forceEnableProjectIds. Локальный контур mdb-health: prefix `health.testing.rtconfig`
(deploy/vars/dev.yaml), прод `health.prod.rtconfig`.

### 2. `data.prod.rtconfig.warnings.{cluster|cluster_list|db_list}` — отображение (mdb-data)

| Свойство | Страница |
|---|---|
| `warnings.cluster` | Страница кластера (карточки по каждому варнингу) |
| `warnings.cluster_list` | Уведомления по clusterId (список кластеров базы) |
| `warnings.db_list` | Уведомления по dbType (страница продукта) |

Структура значения:

```yaml
enabled: false                 # глобальный тумблер отображения
collapsable: true
dbTypes: [...]                 # для каких СУБД показывать
criticality: ["A", "B"]        # фильтр по criticality кластера (A/B/C/D/UNKNOWN)
enabledWarningTypes: [...]     # КАКИЕ ТИПЫ ВАРНИНГОВ ПОКАЗЫВАТЬ (lowercase compare)
forceEnabled:                  # переопределение для проектов (projectId в списке)
  projectIds: [160, 34, 195]
  dbTypes: [...]
  criticality: [...]
  enabledWarningTypes: [...]
isRed: '''<pebble>'''          # непустой рендер = красная подсветка
title/header/summary/footer: '''<pebble>'''   # общие части блока
templates:
  <warningType>: '''<pebble>'''               # карточка конкретного варнинга
```

Контексты Pebble:
- общие части (`title`/`header`/`summary`/`footer`/`isRed`): `warnings` (список:
  `warningType`, `warningParams`, `dbType`, `clusterId`, `productId`), `warningTypes`,
  `warningsCount`, `dbType`, `projectId`, `productId`
- `templates.<type>`: `warningParams` (map), `warningType`, `dbType`, `projectId`, `productId`

Ключевое:
- **Fallback**: нет `templates.<type>` → UI получает `warningMessage` из кода чека
  (WarningTemplateRenderer.java). Тексты в коде — рабочий дефолт, шаблон — beautify.
- `forceEnabled.<...>` версии шаблонов/фильтров применяются, если projectId в
  `forceEnabled.projectIds` (тестовые проекты).
- Pebble с `autoEscaping=false` — HTML разрешён; стили через css-переменные консоли
  (`var(--g-color-base-danger-light)`, `var(--g-color-text-danger-heavy)`).
- Код: mdb-data `facade/warnings/WarningsFacade.java`, `service/warnings/WarningTemplateRenderer.java`,
  `WarningsView` (маппинг свойств); mdb-health `warnings/check/**`, README.md.
- ⚠️ local-профиль mdb-processing пишет в РЕАЛЬНЫЙ PMS (bean `pmsRestClient`) — только
  dev-кластеры, снапшот до/после.

### Пример: три типа Kafka KRaft-quorum (2026-09)

Чек `KafkaQuorumVotersCheck` (mdb-health) эмитит три типа с параметрами
`controllers`, `voters_count`, `voters`, `dead_voters`, `leader_id`:
`kafka_controller_quorum_voters_mismatch` / `_even` / `_dead`.
Для отображения добавить типы в `enabledWarningTypes` (все три свойства) и опционально
шаблоны в `templates` (`warnings.cluster`):

```pebble
<span style="display: block; padding: 8px 12px; border-radius: 6px; background-color: var(--g-color-base-danger-light); color: var(--g-color-text-danger-heavy)">
<b>Кворум KRaft-контроллеров не соответствует составу кластера</b>:
в кворуме {{ warningParams.voters_count }} voter'ов при {{ warningParams.controllers }} контроллерах в кластере.
В кворуме остался фантомный voter или один из контроллеров не вошёл в кворум —
отказ одного контроллера может привести к полной недоступности кластера. Создайте тикет в поддержку.
</span>
```

`_even`: `<b>Чётное число voter'ов в KRaft-кворуме ({{ warningParams.voters_count }})</b> — запас отказоустойчивости тот же, как у меньшего нечётного состава: лишний voter не даёт выгоды. Удалите один контроллер через UI облака, чтобы число voter'ов стало нечётным.` (⚠️ не писать «отказ одного voter'а приведёт к потере кворума» — это неверно: при N=4 отказ одного оставляет 3/4, кворум сохраняется)

`_dead`: `<b>Voter'ы {{ warningParams.dead_voters | join(", ") }} не синхронизируются с лидером кворума</b> (лидер — {{ warningParams.leader_id }}): кластер работает без запаса отказоустойчивости. Создайте тикет в поддержку.`

Для красности в `isRed` добавить:
```pebble
{%- for w in warnings %}
  {%- if w.warningType == "kafka_controller_quorum_voters_mismatch" or w.warningType == "kafka_controller_quorum_voters_dead" %}yes{% endif %}
{%- endfor %}
```

## Соседние скиллы

- `kafka-config-inspector` — сверка PMS ↔ отрендеренные конфиги на Kafka-хостах
- `mcc-host-worker` — доступ к хостам (PMS-API — это НЕ mcc, отдельный curl+mTLS)
- `backstage-local-tester` / `mdb-local-tester` — локальные флоу, где PMS-таски обходятся SQL-ем
