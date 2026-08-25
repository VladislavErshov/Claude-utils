# T5 Downscale guard (2026-08-25)

**Результат: PASS**

## Запуск

Прямой temporal-запуск (mdb-data API не позволяет послать цель ниже текущей — всегда current+1):
`temporal workflow start --type upscaleKafkaControllerInCluster --task-queue kafka-activities-queue
--workflow-id t5-downscale-guard-test --input-file` с `controllersPerDc {dc:2, hc:1, kc:1}`
при фактических hc=2. Input скопирован с реального (queueInfo из T1), hardwarePreset опущен
(для уже существующего сервиса не нужен).

## Результат

- child `_hc` FAILED мгновенно на workflow-уровне (0 упавших activity): 
  `Upscale does not allow reducing controller count. Current: 2, target: 1 in DC hc`
  (= DOWNSCALE_NOT_ALLOWED, non-retryable).
- dc/kc children COMPLETED (skip — цель достигнута), их фазы discovery/upsert прошли.
- Parent FAILED `Controller upscale failed in 1 DC(s): [hc]`.
- **Side-effects нет**: PMS quorum=5 (без дублей), host_state=5 — guard отработал до rescale.

## Заметки

- temporal CLI из контейнера работает с `--address host.docker.internal:7233`.
- Для прямых запусков достаточно `clusterId + queueInfo + controllersPerDc + brokerDcs`.
