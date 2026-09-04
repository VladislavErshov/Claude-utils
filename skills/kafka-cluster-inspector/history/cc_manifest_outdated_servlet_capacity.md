# CC не отвечает: «There are already 5 active user tasks» — устаревший манифест/конфиг круиза

Дата: 2026-08-20
Кластер: `vkmarket-events-p-adblogger-kafka` (KRaft, 3×1 в hc/pc/kc)
Хост: `1.cruise.vkmarket-events-p-adblogger-kafka.hc.one-infra.ru`

## Симптом

CC не отдаёт цели оптимизации — **любой** HTTP-запрос к сервлету CC падает с HTTP 500 и
одной и той же ошибкой:

```
Error processing GET request '/state' due to: 'There are already 5 active user tasks,
which has reached the servlet capacity.'.
```

Это же исключение на **все** endpoints: `/state`, `/user_tasks`, `/kafka_cluster_state`,
`/load`, `/rebalance`, `/proposals` и т.д. `UserTaskManager` имеет хардкод-лимит в 5 активных
async tasks — когда слот занят, новые запросы не принимаются.

## Что проверили — всё «нормально», кроме самого сервлета

| Что | Значение | Вердикт |
|---|---|---|
| `systemctl status cruise-control` | `active (running)`, Main PID 818 (java) | процесс жив |
| `ss -lntp` на java PID | 8080 (HTTP API), 9000 (JMX), 7777 (Jolokia), 8081 (Prometheus) | порты слушает |
| `Generated ... partition metric samples` в `cruise-control.out.log` | регулярно, последнее за текущие сутки | метрики от брокеров идут |
| `broker metric samples` | 3 (по числу брокеров) | все 3 брокера отдают метрики |
| `NotEnoughValidWindowsException` в логе | отсутствует | не проблема покрытия метрик |
| `Proposals are not ready` в логе | отсутствует | цели вырабатываются |
| `Skip generating metric sample for broker X` в логе | отсутствует | нет битого брокера (как в `MDBSUP-4614.md`) |
| Брокеры (hc/pc/kc) `availability` | RESERVED у всех 3 | Governor пометил, но метрики идут |
| `Node 20001/21001/22001 disconnected` в логе | есть, по 6 шт. на каждого | не блокирует генерацию сэмплов |

То есть **сама CC работает** (метрики копит, proposals вычисляет), но **сервлет заблокирован**.

## Корень проблемы — устаревший/несогласованный манифест и конфиг круиз-контрола

Корень — не в метриках и не в брокерах. Нужно было **обновить манифест сервиса cruise в PMS
и конфиги круиза** в app=`mdb`:

- манифест `cruise` (one-cloud service manifest — `image.version`, `alloc`, `network`, `ports`)
- `kafka.cruisecontrol.sysconfig` (параметры JVM)
- `kafka.cruisecontrol.properties` (`bootstrap.servers`, `security.protocol`, goals, etc.)
- `kafka.cruisecontrol.capacity.json` (DISK / NW_IN / NW_OUT — должны совпадать с реальными)

После актуализации — `confp --oneshot` + рестарт `cruise-control`:

```bash
mcc --local sshexec -n infra 1.cruise.<cluster>.<dc>.one-infra.ru \
  "confp --oneshot && systemctl restart cruise-control"
```

После рестарта CC копит метрики ~10 минут, потом `/state` отвечает нормально и proposals
 становятся доступны.

## Что НЕ было корнем (ложные следы)

1. **MDBSUP-4614 (NotEnoughValidWindowsException, 0 valid windows)** — похожий симптом
   «CC не может выполнить операцию», но там **другая ошибка** в логе: `Skip generating
   metric sample for broker X because the following required metrics are missing`. Здесь
   этого маркера нет. Поискать этот маркер в `cruise-control.out.log` — если нет, не
   MDBSUP-4614.

2. **rscheck spam /state → 5 зависших user tasks** — это **симптом**, а не корень.
   `/etc/rscheck/cruisecontrol.conf` дёргает `GET /state` каждые 10 сек с timeout=3 сек.
   rscheck обрывает соединение, async user task в Jetty остаётся активным → копятся 5
   штук → servlet capacity reached. Но это **следствие**, а не причина — CC сам по себе
   отвечает достаточно быстро, чтобы rscheck не накапливал зависшие tasks. Когда CC
   замедляется из-за устаревшего конфига (например, по expired proposals или долгому
   precompute), rscheck начинает обрывать соединения → копятся user tasks → CC не
   отвечает вовсе. Лечить надо CC, а не rscheck.

3. **Cisco-сканер спамит `/+CSCOT+/...`** — это видно в `nginx_access.log` (пути
   `/+CSCOT+/translation-table`, `/+CSCOT+/oem-customization`). Но nginx отсекает этот
   спам, к CC сервлету он не доходит. Маркер в Jetty access log: запросы к CC только от
   `127.0.0.1` (rscheck), внешнего спама нет.

## Грабли

1. **HTTP API CC на порту 8080, а не 9000.** Порт 9000 в образе круиза занят JMX
   (`-Dcom.sun.management.jmxremote.port=9000`). curl на `localhost:9000/kafkacruisecontrol/state`
   → `Empty reply from server` (JMX не отвечает на HTTP). Реальный REST API — на 8080,
   проксируется через nginx (443). Перед curl-диагностикой CC смотреть `ss -lntp` и
   `cruisecontrol.properties` для порта.

2. **Jetty access log лежит внутри `cruise-control.out.log`**, отдельного файла нет.
   Строки вида `INFO 127.0.0.1 - - [date] "GET /kafkacruisecontrol/state ... HTTP/1.1" 500 3689`
   — это логи Jetty-сервлета, а не nginx. Для поиска спама по endpoint'ам:
   ```bash
   grep -oE 'GET /kafkacruisecontrol/[a-z_]+' /mnt/logs/dbms/cruise-control.out.log | sort | uniq -c | sort -rn | head
   ```

3. **`mcc sshexec` обрывает соединение** при долгой команде (CC не отвечает за 10 сек →
   ssh умирает). Делать curl через `nohup` + запись в `/tmp/*.json` + `sleep`, потом
   читать файл:
   ```bash
   mcc --local sshexec -n infra <cruise-host> "nohup bash -c \
     'curl --max-time 8 -s http://localhost:8080/kafkacruisecontrol/state > /tmp/cc_state.json' \
     >/dev/null 2>&1 & sleep 10; cat /tmp/cc_state.json"
   ```

4. **Два конфликтующих набора G1GC опций в `kafka.cruisecontrol.sysconfig`** — JVM
   получает `-XX:+UseG1GC -XX:-G1UseAdaptiveIHOP -XX:G1HeapRegionSize=32M ...` и тут же
   `-server -XX:+UseG1GC -XX:MaxGCPauseMillis=20 -XX:InitiatingHeapOccupancyPercent=35 ...`.
   Это маркер того, что sysconfig был пропатчен поверх старого без очистки. Не причина
   текущего зависания, но признак того, что конфиг устарел/несогласован — подтверждает
   корень.

5. **`availability: RESERVED` у брокеров** не блокирует CC. Метрики от них всё равно
   идут (`Generated 3 broker metric samples` — по числу брокеров). Не копать в эту
   сторону, пока не подтверждена проблема с метриками.
