# 2026-08-28: update-broker-config vkcluster-kafka (dpkafka) — sshExec умирает от 30s read-timeout

## Запрос

`903a4dcc-f8ce-4d42-bb58-664d5793b55a_update-broker-config` — вечные падения детей
`*_ec_N/_hc_N/_kc_N/_pc_N`, операция не сходится часами (run1 10:14→13:05 по TTL, run2
с 13:05 снова падает). Вопрос: облако или наш код?

## Симптомы

- Activity `kafka_host_restartBrokerInstanceSsh` падает стабильно за **~33 сек** на попытку
  (3 попытки: 33+10+33+20+33 ≈ 129s → RETRY_STATE_MAXIMUM_ATTEMPTS_REACHED).
- Failure-цепочка: `CloudException: Unknown error while calling cloud` ←
  `HttpException: Failed exec call: <host>::confp --oneshot && systemctl restart kafka-broker.service` ←
  `HttpException: Invalid type of response received: class one.nio.http.Response !`
- Распределение фейлов по ДЦ: ec 40/40, hc 40/40, kc 20/40, pc 1/40 — коррелирует с
  длительностью остановки брокера, не с ДЦ-инфраструктурой.

## Корневая причина (наш код, НЕ облако)

1. proxylib `CloudConfigurationProperties.defaultTimeout = Duration.ofSeconds(30)`
   (default, в application.yaml не переопределён) → к master-строке добавляется
   `timeout=30000` → socket read timeout 30s у всех вызовов облака, включая
   стриминг терминала sshExec.
2. Команда `confp --oneshot && systemctl restart kafka-broker.service`: confp стримит
   вывод ~3-5с, затем `systemctl restart` молчит ~85-90с (graceful stop Kafka на
   тяжёлых брокерах dpkafka). На 30-й секунде тишины `TerminalCall.readUntil` ловит
   SocketTimeoutException → `HttpException("Socket error")` → в `SshSupportImpl.exec`
   catch(Throwable) возвращает **фейковый `Response(503)`** → наверх `HttpException
   "Invalid type of response received: class one.nio.http.Response !"`. Реальная причина
   (SocketTimeout) теряется полностью.
3. Брокеры с быстрым рестартом (<30с тишины) проходят — отсюда pc 39/40 и успешные
   операции на других кластерах в то же время.

## Воспроизведение руками (облако ОК)

- `mcc sshexec` полная команда на ec_1: **93.7с**, рестарт прошёл успешно (клиент mcc
  без 30s read-timeout).
- `sleep 100 && echo DONE` через mcc: выживает 100с полной тишины → idle-лимита прокси нет.
- curl с самого воркера его же сертификатами: `307 → 101 UPGRADED → вывод` за 0.8с,
  цепочка master-редиректов работает.
- 5 параллельных mcc exec на разные хосты ec — все ОК.

## Дополнительные находки

- `start-to-close-timeout-seconds: 90` у kafka-activities-queue: даже с починенным
  read-timeout команда 93+с НЕ влезает — надо поднимать оба.
- Воркеры mdb-processing деградировали: kc — инстанса нет, hc — stopped ("not scheduling
  on a minion"), pc — java.log-пайп умер 08:00, uc×2 — java.log умер при рестарте
  27-го 16:33 (логи приложения фактически недоступны, journalctl пуст). Из-за 3 воркеров
  activity стоит в очереди ~96с до старта.
- Грабли macOS: `timeout` отсутствует (zsh: command not found), фоне-грепы по пустому
  вводу давали ложные «нет совпадений». Использовать background+kill или gtimeout.

## Фиксы (кандидаты)

Итоговое решение (в mdb-processing): `KafkaCommand.UPDATE_KAFKA_BROKER_CONFIG` →
`confp --oneshot && systemctl restart kafka-broker.service --no-block`. systemctl
возвращается мгновенно → sshExec короткий → 30s read-timeout не страшен. Готовность
проверяется уже существующим пингом `KafkaHostWaiter.waitSshRestartedBrokerInstanceReady`
(active + свежее время старта процесса, бюджет 15 мин) — его семантика корректно
обрабатывает --no-block (пока старый процесс жив, start time старый → not ready).
Второй вызов (resize, recovery-ветка) покрыт `pingSshBrokerActive` в readiness-цикле.
Конфиг-правки (one-cloud.default-timeout, start-to-close 300) откатаны.

Не сделано (вне репо): маскировка ошибок в one-cloud-client `SshSupportImpl` — предложить
владельцам прокси lib. Также не тронуто: UPDATE_KAFKA_CONTROLLER_CONFIG /
UPDATE_KAFKA_CRUISE_CONFIG (их рестарты быстрые, проблемы не было).

## Ключевые файлы

- `one.cloud.client.SshSupportImpl.exec` (fake 503, «Invalid type of response»)
- `one.cloud.client.TerminalCall.readUntil` (30s тихого стрима = смерть)
- `one.cloud.mdb.proxylib.cloud.config.CloudConfigurationProperties` (defaultTimeout=30s)
- `mdb-processing …/kafka/util/KafkaCommand.java:41` (UPDATE_KAFKA_BROKER_CONFIG)
- `application.yaml` mdb-processing: `one-cloud.masters` без default-timeout,
  `temporal.activity-options.kafka.start-to-close-timeout-seconds: 90`
