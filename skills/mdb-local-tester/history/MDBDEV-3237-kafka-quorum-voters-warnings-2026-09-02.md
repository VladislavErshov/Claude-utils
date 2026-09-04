# MDBDEV-3237 — Kafka quorum voters warnings: локальный E2E + rtconfig/UI + прод-PMS (2026-09-01/02)

## Что сделано

Чек `KafkaQuorumVotersCheck` (ветка ershov/MDBDEV-3237-split-kafka-quorum-voters-warning-into-per-reason-warnings) полностью протестирован E2E локально, rtconfig отображения доведён до прод-PMS.

## Проверенные сценарии (все на реальных кластерах mdbdev)

| Кластер | Сценарий | Результат |
|---|---|---|
| test-modify3 | 4 контроллера, кворум 4 (после чистки фантома) | `_even`: «Четное число контроллеров (id: ...) не увеличивает надежность...» |
| test-modify3 | фантомный voter 11002 (2.controller.hc — остаток upscale) | `_mismatch` + `_dead`; после `confp --oneshot && systemctl restart kafka-controller` на всех контроллерах — самоочистка, остался только `_even` |
| test-downgrade7 | фантом 10001 (dc, удалён из кластера) + dead 13001 (pc) | `_mismatch` + `_dead` (обычная ветка) + topology; after чётность по controllersCount (3 — нечёт) — `_even` не стреляет |
| test-downgrade6 (Kafka 4.3) | вайп `/mnt/data/log/*` на hc, потом kc (поочерёдно stop→rm→start) | 1-й вайп: `_dead (id: 10001)`; 2-й вайп: кворум потерян → describe пустой → «KRaft-кворум потерян. Запись в кластер заблокирована.» с FQDN упавших; voter ожил → самоудалиение |
| test-modify4 | 2/3 контроллера упали | потеря кворума (FQDN); подняли 1 → обычная ветка `_dead (id: 12001)` |

## Итоговые сообщения чека (Java, дебаг-формат)

- `_mismatch`: «В KRaft-кворуме зарегистрированы контроллеры: {voterIds}, в кластере есть контроллеры: {FQDNs}. [фантомный контроллер / не все вошли]. Обратитесь в поддержку.»
- `_even`: «Четное число контроллеров (id: {voterIds}) не увеличивает надежность. Для оптимизации ресурсов удалите один контроллер через UI, сделав состав нечетным.»
- `_dead` обычная: «Контроллеры (id: {deadIds}) не синхронизируются с лидером KRaft-кворума. Кластер работает без запаса отказоустойчивости.»
- `_dead` потеря: «Контроллеры ({downFqdns}) не синхронизируются с лидером — KRaft-кворум потерян. Запись в кластер заблокирована.»

## Логика чека (финальная)

- `check()` → guards (kafka, rtconfig `warnings.kafkaQuorum.*`, ≥2 контроллеров, namespace) → `describeQuorum`
- describe OK → `collectWarnings(state, hosts, quorumInfo)`: `createVotersMismatchWarning` / `createEvenControllersWarning` (чётность по **controllersCount из host_state**, не по voters!) / `createDeadVotersWarning` — все `Optional<ClusterWarning>`
- describe null → `quorumLostWarnings(state, hosts)`: статусы хостов из Redis (`hostService.getHosts`), если available < majority → dead-warning (FQDN упавших, params: controllers/available/down/cluster_controllers)
- Фабрики: `createWarning(state, type, params, message)` + overload с quorumInfo; `createBaseWarningParams(count)` + quorum-надстройка
- Дедупликация в rtconfig: таблица по **первому** кворум-варнингу (флаг done), `_mismatch` гасит dead-заголовок и в развёрнутом, и в кратком (summary)
- kafka-clients 3.8.0: `ReplicaState` БЕЗ endpoints() (это 4.x) — ID кластерных контроллеров недоступны, только voter-ID из describe. Никакой рефлексии/кэша — только «здесь и сейчас»

## rtconfig (warnings.cluster) — финальная структура

- `enabledWarningTypes` (+forceEnabled): + 3 kafka-типа
- per-type templates = стабы `<span></span>` (скрыть из contents — санитайзер вырезает тег → hasRenderedContent=false)
- ВСЁ рендерится в **footer** (контекст: `warnings` с `warningParams`, `warningTypes`):
  1. topology: «Размещение хостов» + таблица (инлайн)
  2. разделитель-линия
  3. kafka-заголовки (mismatch подавляет dead) + ОДНА таблица + «Мониторинг Kafka»
  4. разделитель-линия
  5. учения/DR (оригинал прода)
- summary: topology-строки первыми, kafka — внизу; title условный; isRed + kafka
- Таблицы topology и quorum — идентичный инлайн (одинаковый DOM-контекст footer, без CSS-виджета):
  - th: `background-color: #f3f3f3; color: var(--g-color-text-secondary); font-weight: 400; border: 1px solid var(--g-color-line-generic); padding: 4px 12px`
  - td: `border: 1px solid var(--g-color-line-generic); padding: 4px 12px`
  - table: `border: 1px solid var(--g-color-line-generic); border-radius: 6px; margin: 4px 0`
  - danger-строки: `background-color: var(--g-color-base-danger-light)`
- Статусы: Лидер НЕ пишем — только «Синхронизирован / Не синхронизируется»

## Прод-PMS

`mdb/data.prod.rtconfig.warnings.cluster` (ns=infra, host=host-mdb, app=mdb) — залит финал, верифицирован байт-в-байт. Гейтинг прода нетронут: `enabled: false`, `criticality: [A,B]`, `forceEnabled.projectIds: [160,34,195]`. Активация — после деплоя чека в mdb-health. Прод-mdb-data перечитывает PMS с задержкой (минуты) — дубль при обновлении значения исчезает сам.

## Грабли (занесены в SKILL.md)

- Санитайзер UI: `hr`/`div` вырезаются; `border-top`/`padding-top` вырезаются; рабочий разделитель = `<span style="display: block; background-color: #ccc; padding: 1px; margin: 12px 0"></span>` (shorthand `padding` разрешён)
- CSS таблиц — на `.warnings-block-content` (только contents): footer-таблицы его не получают → идентичность только переносом обеих в footer
- mdb-health `rtconfig/local.hjson` — строгий JSON (ключи в кавычках)
- Корневые флаги `warnings.enabled` + `warnings.enabledDbTypes` обязательны для автономного sync→compute цикла (~5 мин)
- `setval('warnings.cluster_warnings_id_seq', max(id))` после сида
- Kafka CA: `~/app/infra/infra_kafka_ca.crt` ← `~/.mccloud/kafka-tls-ca.crt`; пароль super → локальный vault (`mdb-processing-vault`, root, zkv)
- Контроллеры слушают CONTROLLER://:9093; клиент — через брокеров 9092 (SASL_SSL, client.properties на хостах)
- Ground truth кворума: `kafka-metadata-quorum.sh describe --replication` (LastFetch=-1 = dead voter)
- Вайп мета для фантома: `systemctl stop kafka-controller; rm -rf /mnt/data/log/*; systemctl start` — поднять поочерёдно
- zsh-сплит в сид-скриптах: ключи Redis получали суффикс-статус («…ru UNAVAILABLE») — проверять `--scan`
