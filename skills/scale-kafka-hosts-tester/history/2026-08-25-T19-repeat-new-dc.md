# T19 Повторный upscale в новый ДЦ (2026-08-25)

**Результат: PASS**

- Повторный `POST …/hosts/controllers?dc=ic` (ic уже =1) → workflow 618c00fd COMPLETED:
  все child skip, reload идемпотентен, save без дублей.
- host_state 5|5 (уникальны), PMS кворум 5 voters, db_cluster_version НЕ пересоздана
  (saveNewControllerDcsToClusterVersion идемпотентен: newDcs пуст — версия не создаётся).

T20 (namespace dzen) — на этом стенде неприменим (кластер в infra; dzen требует своего
кластера/vault/PMS-пути). Не покрыто.
