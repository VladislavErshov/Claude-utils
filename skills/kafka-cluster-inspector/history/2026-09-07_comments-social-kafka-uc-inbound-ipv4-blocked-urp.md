# comments-social-kafka (dzen) — URP=30 из-за заблокированного входящего IPv4 до переезденного uc-брокера

Дата: 2026-09-07. Кластер `comments-social-kafka.social.db.production.mdb.prod`, **namespace dzen** (`*.idzn.ru`).
Состав: 5 брокеров (по одному на ДЦ: dc=20001, hc=21001, kc=22001, pc=23001, uc=24001) + 3 контроллера (hc/pc/uc).

## Симптом

В Grafana offline/under-replicated > 0, запрос «почему offline partitions».

Фактическое состояние на момент разбора:
- `OfflinePartitionsCount=0` на **всех трёх контроллерах** — офлайнов нет, лидеры у всех партиций есть.
- `UnderReplicatedPartitions=30` **только на uc** (24001), у остальных брокеров 0.
- `kafka-topics --describe`: **31 партиция с `Leader: 24001, Isr: 24001`** — фолловеры (20001/21001/22001)
  вылетели из ISR и не возвращаются. Кластер виден целиком в этом состоянии с ~2026-09-04.
- «Offline» в дашборде — показания панели поверх этой же истории (лидеры есть, партиции не offline).

Паттерн инвертирован относительно MDBSUP-4166: там был мёртвый брокер в Replicas/Isr;
здесь живой лидер uc, вокруг которого схлопнулся ISR, потому что **к нему не могут подключиться фолловеры**.

## Причина

1. uc-хост пересоздан ~2026-09-04 (свежий runid позже остальных, новый IPv4 `10.103.17.217`).
2. **Входящий IPv4 до нового uc-хоста из других ДЦ не работает** (сетевая политика/маршрут облака
   не обновился под новый IP), входящий IPv6 — работает. Асимметрия: исходящий uc→dc по v4 — OK.
3. Kafka-JVM по умолчанию предпочитает IPv4 → `ReplicaFetcherThread-0-24001` на dc/hc/kc циклит
   `Disconnecting from node 24001 due to socket connection setup timeout` (сессии вечно
   `sessionId=INVALID, epoch=INITIAL`). Точно так же висли AdminClient-команды (`kafka-topics --describe`)
   с dc/hc — metadata refresh упирался в недостижимый 24001.
4. Фетч не проходит → фолловеры не догоняют LEO → ISR не расширяется → вечно `Isr: 24001`, URP=30.

## Диагностика (ключевые шаги)

- Состав кластера: `mcc --local -n dzen -c <dc> instances '%.broker.comments-social-kafka%'` по ДЦ
  (для idzn.ru — `-n dzen`, не `-n infra`).
- Состояние: Jolokia `BrokerState` (=3 на всех), `UnderReplicatedPartitions` (30 на uc, 0 у остальных),
  `OfflinePartitionsCount` на контроллерах (=0). Ошибок ERROR/WARN за сегодня в логе uc нет — брокер
  «здоров», проблема сетевая.
- Лог dc: `ReplicaFetcherThread-0-24001` — непрерывные `socket connection setup timeout` каждые ~15-30с.
- **Развязка v4/v6** (главный шаг): с dc/hc на uc:9092
  ```bash
  timeout 8 openssl s_client -4 -connect 1.broker.comments-social-kafka.uc.idzn.ru:9092 </dev/null   # HANG
  timeout 8 openssl s_client -6 -connect 1.broker.comments-social-kafka.uc.idzn.ru:9092 </dev/null   # Verify return code: 0
  timeout 8 openssl s_client -4 -connect 1.broker.comments-social-kafka.pc.idzn.ru:9092 </dev/null   # OK — контроль
  ```
- Контроль асимметрии: с uc `openssl -4` до dc:9092 — OK (исходящий v4 жив, закрыт только входящий до uc).

## Грабли

- **TCP/TLS-тест без `-4`/`-6` врёт**: `bash /dev/tcp` и `openssl` идут по порядку getaddrinfo
  (в этом окружении v6 первым) и показывают «всё ок», тогда как JVM Kafka сидит на v4.
  Всегда проверять семейства раздельно флагами `-4`/`-6`.
- mcc sshexec часто рвёт долгие команды (`*** Connection closed by remote host ***`) — kafka-topics
  запускать с редиректом в файл на хосте и читать вторым вызовом; ретраи 3-5 раз.
- `grep '^Topic:'` матчит только summary-строки; партиционные строки начинаются с табуляции —
  фильтровать по `Partition:.*Replicas:`.
- Разбиение «115 из 146 строк без 24001» сначала выглядит как «uc не в кворуме» — на деле
  31 строка С 24001 и есть проблемные (ISR схлопнут на uc-лидере).
- Ошибки `Expected partition ... to exist, but it was missing. Creating...` в логе от 4.09 —
  след пересоздания хоста, к текущей сетевой проблеме отношения не имеют.

## Фикс (варианты)

1. **Правильный**: эскалация в облачную сеть — восстановить входящий v4 (mdb wlan/wan) до
   `10.103.17.217` с mdb-подсетей других ДЦ. После этого фетчеры восстановятся сами,
   ISR расширится за `replica.lag.time.max.ms`.
2. **Workaround без сети**: `-Djava.net.preferIPv6Addresses=true` в KAFKA_OPTS
   (`/etc/sysconfig/kafka` ← PMS `kafka.j2`, механика — скилл `pms-worker`) + поочерёдный
   рестарт брокеров — JVM пойдёт по v6, репликация восстановится. Минус: фикс должен попасть
   на все хосты кластера, иначе часть соединений останется на v4.

Похожие кейсы: MDBSUP-4166 (offline из-за мёртвого брокера в Replicas — там unclean election + reassign),
MDBSUP-5137 (проверка связности v4/v6 раздельно). См. также `known_issues.md` →
«Offline partitions из-за удалённого брокера в Replicas».
