# MDBSUP-4739 — CC после пересоздания хоста долбится в localhost:9092: конфиги pms под неправильным hostname

**Дата**: 2026-08-20
**Кластер**: `ecom-events-adtech-kafka` (KRaft, брокеры в hc/pc/kc/uc)
**Хост**: `1.cruise.ecom-events-adtech-kafka.uc.one-infra.ru`

## Симптом

Cruise-хост пересоздали (лежал, поднят заново). После этого:

- `systemctl status cruise-control` → `inactive (dead)`, в journald пусто, `/mnt/logs/dbms/`
  пустой (логи потерялись при пересоздании — истории «почему упал» нет).
- После `systemctl start cruise-control` процесс `active`, но CC не подключается к кластеру —
  в `cruise-control.out.log` бесконечный спам каждые ~100 мс:
  ```
  [AdminClient clientId=adminclient-1] Node -1 disconnected. (org.apache.kafka.clients.NetworkClient)
  [AdminClient clientId=adminclient-1] Connection to node -1 (localhost/127.0.0.1:9092) could not be established. Broker may not be available.
  ```
  За ~30 секунд — 1000+ повторов, CC не стартует дальше.

## Диагностика

| Что | Значение | Вердикт |
|---|---|---|
| `grep ^bootstrap.servers cruisecontrol.properties` | `localhost:9092` | сток образа, рендера не было |
| Дата `cruisecontrol.properties` | Mar 26 2025 (= даты всего `/opt/cruise-control/config/`) | файл из образа, confp его ни разу не трогал |
| `grep -i cruise /etc/confp/confp.yml` | пусто | маппинги не здесь (не грабля, см. ниже) |
| `/etc/confp/resources.d/cruise-control.yml` | есть, маппит `kafka.cruisecontrol.properties` → `/opt/cruise-control/config/cruisecontrol.properties` | маппинг на месте |
| Java на хосте | OpenJDK 11.0.27 | не Java-мисмач (сервис стартует, нет `UnsupportedClassVersionError`) |

Маркер проблемы: **рендеренный конфиг выглядит как сток образа** — дата файла = дате сборки
образа, `bootstrap.servers=localhost:9092`. Значит `confp` ни разу не отрендерил
cruise-конфиги на этом хосте.

## Корень проблемы

Конфиги круиза в pms лежали под **неправильным hostname**:
`cruise-control.ecom-events-adtech-kafka.clouds` вместо правильного
`cruise.ecom-events-adtech-kafka.clouds`. confp резолвит значения pms по имени хоста →
`kafka.cruisecontrol.properties` под правильным именем не находился → рендер не происходил,
оставался дефолт из образа с `bootstrap.servers=localhost:9092`. На cruise-хосте брокера
нет, поэтому CC вечно переподключался к localhost.

Вероятно, опечатка при заведении конфигов (в имени хоста pms написано `cruise-control.`,
а FQDN-шаблон хоста — `1.cruise.<cluster>.<dc>.one-infra.ru`, pms-host — `cruise.<cluster>.clouds`).

## Фикс

1. В pms переложить конфиги круиза (`kafka.cruisecontrol.properties`, `capacity.json`,
   `jaas.conf`, `log4j`, sysconfig) под правильный hostname `cruise.<cluster>.clouds`.
2. На cruise-хосте:
   ```bash
   confp --oneshot && systemctl restart cruise-control
   ```
   (в нашем случае первый `confp --oneshot` упал на vault-pki — см. грабли №1 — второй прогон прошёл полностью)
3. Проверки успеха:
   - в `cruise-control.out.log` появляется `Cluster ID: <uuid>` — метаданные кластера получены;
   - спам `Connection to node -1 ... localhost` прекращается;
   - `/state` на `localhost:8080` отвечает: `MonitorState: RUNNING`, `ExecutorState: NO_TASK_IN_PROGRESS`;
   - java слушает 8080 (REST), 9000 (JMX), 7777 (Jolokia), 8081 (Prometheus);
   - `bootstrap.servers` в отрендеренном конфиге = список реальных брокеров, `security.protocol=SASL_SSL`.

## Грабли

1. **Первый `confp --oneshot` на свежеподнятом cruise-хосте может упасть на vault-pki и не
   дойти до рендера cruise-конфигов**:
   ```
   KeyError: "getpwnam(): name not found: 'kafka'"
   ```
   ok-pyvault-запись пишет CA в `/opt/kafka/ssl/` с владельцем `kafka`, которого на cruise-образе
   нет (там пользователь `cruisecontrol`). При этом CA-серт успевает записаться, и **повторный**
   `confp --oneshot` проходит полностью. Не останавливаться на первом падении — просто повторить.
   (Сама ok-pyvault-запись с владельцем `kafka` на cruise-хосте — отдельный косяк в pms, не
   блокирующий.)

2. **`mcc status` / `mcc instances` не находят cruise-хост** (`EntityNotFoundException: Service...
   not found` / `No instances found matching %cruise.<cluster>%`) — при этом `mcc ssh`/`sshexec`
   работает. Для cruise-хостов не тратить время на интроспекцию через master — идти сразу по ssh.

3. **CC REST API — порт 8080, не 9000** (9000 занят JMX-агентом, curl туда даст
   `Empty reply from server`). Как в `cc_manifest_outdated_servlet_capacity.md`, грабля №1.

4. **После успешного старта `/state` показывает `isProposalReady: false`,
   `MonitorState: RUNNING(0.000% trained)`** — норма для свежеподнятого CC. В логе при этом:
   `Skipping proposal precomputing because load monitor does not have enough snapshots`.
   Proposals готовы через ~10–30 мин (CC копит снапшоты метрик, 5 окон по 5 мин). Не рестартить
   CC повторно из-за этого.

5. **`UnknownTopicOrPartitionException` при старте sample store** — разовый шум от
   `KafkaSampleStore` (топики сэмплов ещё пустые/создаются). Маркер успеха — строка
   `Sample loading finished. Loaded 0 partition metrics samples and 0 broker metric samples`.

6. **`Too early. SSL Handshake is not finished`** при частых повторных `sshexec` на один хост —
   sleep 3–6 сек и повторить (общая грабля mcc, здесь стреляла регулярно).
