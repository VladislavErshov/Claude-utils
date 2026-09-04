# T17/T18 Upscale в новый ДЦ ic — найден и пофиксен баг discovery (2026-08-25)

**Результат: PASS после фикса; T18 (mix нового + существующих ДЦ) покрыт тем же прогоном**

## Баг 4 (найден T17): discovery падает на ДЦ без controller-сервиса

- Первый запуск (через mdb-data, `?dc=ic`) упал на первой активности:
  `discoverKafkaHosts` → `getInfosForServices` → `getServiceInfo(ic)` → `CloudException$NotFound`
  — controller-сервиса в новом ДЦ ещё нет. Это ОСНОВНОЙ прод-юзкейс (~80% запусков
  upscaleKafkaController на проде — добавление контроллера в новый ДЦ).
- Фикс в `KafkaHostReloadHelper.discoverKafkaHosts`: сначала `cloudActivity.getExistingServiceDcs`
  (NotFound глотается внутри, как в upgrade-флоу), затем `getInfosForServices` только по существующим.
  ⚠️ Грабля контракта: `getExistingServiceDcs` ждёт КОРОТКОЕ имя роли ("controller", сам клеит
  ".<queue>"), а не полный serviceName — первый вариант фикса вернул пустой список →
  `getSourceHost` упал `NoSuchElementException` на пустом списке хостов.
- Джавадок для discoverKafkaHosts добавлен.

## Прогон после фикса (workflow 6c1b5758)

- Фазы: getExistingServiceDcs → getInfosForServices → upsert×4 (dc/hc/kc/ic; ic — layout+кворум
  с новым nodeId 13001) → 4 child параллельно:
  - **ic: `copyAndSubmitService`** (ветка нового сервиса) + submitQueueIfNeeded — очередь ic уже
    существовала (broker-очередь кластера), сервис скопирован с хоста-образца dc;
  - dc/hc/kc: skip (1==1).
- Reload брокеров (dc/hc/kc) + контроллеров. save.
- **db_cluster_version 223841 создана**: controllerDcs расширен до [dc,hc,kc,ic] — первый случай,
  когда upscale создаёт новую версию (как и задумано — только при новом ДЦ).
- host_state: +1.controller…ic. PMS кворум: 4 voters (13001@…ic). Layout без дублей.
- Новый контроллер в ic жив (follower, ACC=0), кластер собрал кворум 4.

## Наблюдения

- id новой версии = 223841 (инкремент от 223840) — версия создаётся через
  saveNewControllerDcsToClusterVersion при появлении нового ДЦ.
- mdb-data корректно строит controllersPerDc c новым ДЦ из запроса `?dc=ic` (direct-start не нужен).
