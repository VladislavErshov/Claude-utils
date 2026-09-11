# videoq-prod-uv-kafka: broker в RECOVERY + exporter crash-loop — корень: миньон (фикс: миграция хоста)

**Дата**: 2026-09-08
**Кластер**: `videoq-prod-uv-kafka` (5 брокеров dc/kc/pc/uc/zc + 3 контроллера dc/pc/uc + cruise в dc)
**Хост**: `1.broker.videoq-prod-uv-kafka.zc.one-infra.ru` (контейнер `main`)

## Симптом (жалоба)

«systemctl-сервисы частично лежат» — на брокере `kafka-exporter.service` в
`activating (start-pre)`, `kafka-controller.service` failed.

## Разбор

1. `kafka-broker.service` — active/running, но `BrokerLifecycleManager` пишет
   «The broker is in RECOVERY» каждые 2с, порт 9092 — **Connection refused**.
   Брокер нечисто упал ночью (~00:28), поднялся в 12:54 и восстанавливает 198G логов
   (`Recovering unflushed segment`, ребилд producer state). 224 партиции, темп
   ~3 партиции/мин (`num.recovery.threads.per.data.dir=1`).
2. `kafka-exporter.service` — **это не поломка**: его pre-start
   (`/opt/kafka/scripts/pre-start-kafka-exporter.sh`) циклом ждёт
   `nc -z $cloud_hostname 9092`, упирается в TimeoutStartSec (90с), systemd рестартует
   (NRestarts рос 6→9 за минуты). Штатное ожидание открытия порта брокером.
3. `kafka-controller.service` failed на брокере — норма (роль broker).

**Вывод: запускать руками ничего не нужно** — все «лежащие» юниты либо норма,
либо ждут завершения RECOVERY. Порт 9092 открывается только после перехода
RECOVERY → RUNNING.

## ПМС-проверка (заодно)

- `kafka.layout=dc,kc,pc,rc,uc,ec,zc,ic` + `kafka.controller.quorum`
  (10001@dc,12001@pc,14001@uc) — согласованы: node.id на zc = 20000+6*1000+1=26001 ✓.
- ⚠️ `kafka.cruisecontrol.properties`: `bootstrap.servers` содержит
  `1.broker...rc` — **такого брокера в облаке нет**, а zc-брокер в списке отсутствует.
  Для CC некритично (bootstrap метаданных с любого живого), но список протухший.

## Грабли / уроки

1. **kafka-exporter в activating(start-pre) при лежащем брокере — диагностический
   маркер, не отдельная проблема**: чинить нечего, лечится само после поднятия 9092.
   Проверка прогресса RECOVERY: `grep -a 'LogLoader' kafka-broker.out.log | awk '$2>="<start>"'`
   + `sort -u` по partition; порт — `timeout 3 bash -c '</dev/tcp/localhost/9092'`
   (`ss`/`netstat` на хосте нет).
2. **`mcc instances` требует `-c <dc>`** для каждого ДЦ — полный состав кластера
   собирается циклом по ДЦ (в zc нашёлся только zc-хост, в других — свои).
3. Ночь накануне: брокер регистрировался каждые ~31 мин (21:51→00:28) — похоже на
   рестарт-луп, причина не найдена (journalctl за вчера пуст — не пережил рестарт
   контейнера). Если повторится — ловить в моменте.

## Итог (тот же день)

**Корень проблемы — миньон** (нода облака, на которой крутился контейнер хоста).
Хост мигрировали на другой миньон — всё поднялось и рекавери прошло штатно.
Вчерашний рестарт-луп (~31 мин) — следствие той же проблемы с миньоном.

- PMS `kafka.cruisecontrol.properties` исправлен (rc→zc в bootstrap.servers), update.do
  + верификация байт-в-байт OK. На cruise: `confp --oneshot` → файл обновился →
  `systemctl restart cruise-control` → active, NRestarts=0, веб отвечает 200.
- Грабля: веб CC на этом кластере слушает **8080**, не 9090 — проверка порта по дефолту
  даёт ложный FAIL. Надёжный маркер живости CC — HTTP 200 в `cruise-control.out.log`
  (CruiseControlPublicAccessLogger GET /kafkacruisecontrol/state).
- Брокер zc вышел из RECOVERY сам, kafka-exporter поднялся сам после открытия 9092 —
  ручных вмешательств в сервисы не потребовалось.
- Урок: если брокер ведёт себя аномально (рестарт-луп, вечное RECOVERY, диск/IO ведёт
  себя странно) — проверять **миньон** (mcc instances/-F minion, миграция хоста на другой
  миньон) раньше, чем копать в конфиги самого хоста.
