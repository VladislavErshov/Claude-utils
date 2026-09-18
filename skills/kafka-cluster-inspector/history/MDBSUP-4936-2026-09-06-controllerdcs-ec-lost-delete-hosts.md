# MDBSUP-4936 — ads-kafka / vkcluster-kafka: delete_hosts вычеркнул ДЦ из controllerDcs, add_hosts его туда не вернул

Дата: 2026-09-06. Тикет: «при смене зон брокеров сбрасываются зоны контроллеров в форме
создания кластера» (UI-аспект формы создания — отдельно, тут прод-аспект данных).
Два продовых кластера с рассинхроном `controllerDcs` (в тикете скриншот с зонами
ICVA/SDNX/GZNK — в БД таких зон нет, это display-имена формы).

## Кластеры и состояние

| Кластер | id | controllerDcs (было → стало) | Фактические контроллеры |
|---|---|---|---|
| ads-kafka | `6f4133c3-1cfa-4ed5-be46-a60532a29952` | `["kc","hc","pc"]` → `["kc","pc"]` | ec, kc, pc |
| vkcluster-kafka | `eba4c8ec-ce96-4997-9e14-e83adaac1f71` | `["kc","hc","pc"]` → `["kc","pc"]` | ec, kc, pc |

## Кто испортил — операции delete_hosts

- **ads-kafka**: операция `39099247-7eed-4e3b-937b-8c098bc7514a` (delete_hosts,
  `host.dc=hc`, a.nevgin, создана 26.08 17:31, завершена **02.09 13:19:25**) →
  версия `257118` (update_instances) создана 02.09 13:19:17 уже с `["kc","pc"]`.
  Контекст: миграция контроллера hc→ec (add_hosts `0455a8f4` 24–25.08 добавил
  `1.controller.*.ec`, delete_hosts удалил контроллер hc).
- **vkcluster-kafka**: операция `bdf8bf6b-2ee0-44cc-96aa-4fe0892f3847` (delete_hosts,
  `host.dc=hc`, g.pankevich, создана 29.08 00:13, завершена **31.08 20:30:51**) →
  версия `249149` (new) создана 31.08 20:30:48 уже с `["kc","pc"]`.

Время создания версии совпадает с `finished_ts` операции до секунды — финальный шаг
delete_hosts переписывает актуальную строку `db_cluster_version` и **вычитает весь ДЦ
удалённого контроллера из `kafkaParams.controller.controllerDcs`**.

## Две половины бага

1. **add_hosts с контроллером не добавляет его ДЦ в `controllerDcs`** — у ads-kafka
   контроллер ec добавлен 25.08, а в `controllerDcs` он так и не появился.
2. **delete_hosts вычитает ДЦ удалённого контроллера, но не синхронизирует список
   с фактическими контроллерами в `host_state`.**

Net-эффект: `controllerDcs=["kc","pc"]` при реальных контроллерах ec/kc/pc — UI/API
считают, что контроллеров в ec нет.

## Отдельные наблюдения

- vkcluster-kafka: контроллеры ec/kc/pc существуют с создания (28.02.2025), но в
  `controllerDcs` с самого начала был `hc` вместо `ec` — рассинхрон старше операций.
- Старые кластеры kafka-dbaas-3 (`825dcd72-…`) и camp-bannerd (`f9374193-…`) — в
  актуальных версиях поля `controllerDcs` нет вообще (старый формат jsonb), это НЕ
  результат операций, не трогать.
- Поиск всех кластеров с проблемой: latest `db_cluster_version` на кластер +
  `jsonb_array_length(cluster_params->'kafkaParams'->'controller'->'controllerDcs') < 3`
  при `count(DISTINCT host_state.params->>'dc') >= 3`.

## Связанное

- `history/MDBSUP-4970-2026-09-02-ads-kafka-quorum-voters-mismatch.md` — тот же
  ads-kafka, quorum-аспект той же миграции hc→ec.
- `history/MDBSUP-5056-2026-09-02-plaintext-stage-delete-controller.md`,
  `history/MDBSUP-5093-2026-09-03-leadads-quorum-stale-voter-after-delete-hosts.md` —
  соседние проблемы флоу delete_hosts.

## Статус / план

- Фикс данных: UPDATE актуальных версий 257118 и 249149 → `controllerDcs=["ec","kc","pc"]`
  (выполнен 2026-09-06, см. заглушку в jira-mdbsup-solver).
- Дальше: выкат фиксов кода (пересчёт controllerDcs при add/delete_hosts из фактических
  контроллеров), затем повторная сверка по коду; UI-аспект сброса зон в форме создания —
  отдельная задача.
