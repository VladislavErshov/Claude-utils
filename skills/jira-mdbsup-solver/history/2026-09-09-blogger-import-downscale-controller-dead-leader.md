# blogger-import — delete_hosts упал на «не-RUNNING» HC-лидере (без тикета)

Дата: 2026-09-09. Kafka, ns dzen, облако HC/KC/PC/EC.

- Кластер `1135e80d-8cff-4d7a-b873-c6ec4d5cc2c8` (blogger-import)
- Операция `d0206f30-f44c-4398-94cf-2c4817674cc0` (delete_hosts, downscale kc-контроллера)
  failed: удаляли kc, но упал `restartAndRestoreControllerInstanceSsh` по **лидеру hc** —
  облако считает его не RUNNING (UI «unknown»), `CloudException$NotRunning`, ретраи исчерпаны.
- По коду: лежащий удаляемый контроллер операцию бы не сломал (SSH только по оставшимся+лидер,
  удаляемый чистится PMS-API + cloud-API withdraw).

Полный разбор: [../../kafka-cluster-inspector/history/2026-09-09-blogger-import-downscale-controller-dead-leader-cloud-state.md](../../kafka-cluster-inspector/history/2026-09-09-blogger-import-downscale-controller-dead-leader-cloud-state.md)
