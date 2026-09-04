# MDBSUP-5056 — stage-vk-support-kafka: ручной downscale hc-контроллера на Kafka 3.8 + корень «Failed connection check» (PLAINTEXT-кластер против SASL-check)

**Дата:** 2026-09-02
**Кластер:** `stage-vk-support-kafka` (vk-support, stage, ns infra; `6a5d52e1-6f4c-45e9-8f90-d684eceed9ee`)
**Тикет:** MDBSUP-5056. Операция delete_hosts `1a776d19-bd7e-48d7-8e97-0247b17b7303`
(удаление hc-контроллера, Temporal `downscaleKafkaControllerInCluster`), Kafka **3.8.0**.
Хосты на момент работ: брокеры ec/kc/pc (21001/22001/23001), контроллеры ec=13001/kc=11001/pc=12001/hc=10001, cruise pc.

## Корень падений операции (не транзиент!)

Симптом: `kafka_host_getLeaderId` — `KafkaAdminClientException: Failed connection check to
hosts <все 3 брокера>:9092 by created KafkaAdminClients for supported security protocols:
[SASL_SSL, SASL_PLAINTEXT]`, ретраи до MAXIMUM_ATTEMPTS. Падало 28.08 (delete_hosts),
02.09 (рестарт операции — снова то же), и 21.08 (create_database de2465d3, canceled).

Причина: **брокерский листенер кластера — PLAINTEXT**
(`listeners=BROKER://:9092`, `listener.security.protocol.map=...BROKER:PLAINTEXT...`,
шаблон stage-поколения), а mdb-processing делает connection-check только SASL-клиентами.
SASL-handshake против PLAINTEXT-листенера → брокер дропает коннект (`Unexpected error ...
closing connection` спамом в kafka-broker.out.log от IP processing) → check всегда падает.
У здоровых кластеров нового поколения (напр. channel-info, ads-kafka):
`listeners=INTERNAL://:9092` + `INTERNAL:SASL_SSL` — там getLeaderId работает.
Флоу без AdminClient (PMS/ops) проходят: `removeControllerFromQuorum` оба раза отработал,
удаление hc-брокера 28.08 прошло. Отсюда «полуцелое» состояние: PMS
`kafka.controller.quorum` уже 3-voter (без hc), а `kafka.layout`, runtime-кворум,
host_state — со hc.

⚠️ Диагностика «брокеры пингуются ⇒ проблема транзиентная» неверна: TCP/меж-ДЦ связность
была ОК, а SASL-check падал детерминированно. Проверять, чем именно клиент стучится
(SASL-протоколы из ошибки) против `listener.security.protocol.map` кластера.

## Ручное доведение (02.09, последовательность)

1. Рендер без рестартов: `confp --oneshot` на контроллерах kc/pc/ec и брокерах
   ec/kc/pc → в controller.properties/broker.properties `controller.quorum.voters`
   стал 3-voter, `10001@` нигде не остался, **node.id не сместились**
   (`kafka.layout=hc,kc,pc,ec` НЕ трогали — позиция ДЦ в layout даёт dc_id для
   node.id, удаление hc сдвинуло бы id kc/pc/ec, механика I48592; hc в layout
   остаётся «дыркой» — так же оставлено и в ads-kafka после MDBSUP-4970).
2. Рестарт контроллеров **kc → pc → ec (лидера последним)**; hc держали живым —
   на каждом рестарте оставалось 3/4 активных voter'ов. После рестарта ec лидером
   стал kc (11001, epoch 46603), CurrentVoters=[11001,12001,13001] — hc вне кворума.
3. `systemctl stop kafka-controller` на hc (ушёл в failed-стейт systemd, процесс
   завершён штатно shutdown-hook'ом — для выводимого хоста неважно).
4. Withdraw hc из облака: `mcc -c hc stop controller.stage-vk-support-kafka` →
   FINISHED → `withdraw --type service` (инстанс исчез из one-cloud) →
   `withdraw --type storage "…prod.hc/controller"` (уравнение через pexpect;
   в сообщении mcc фигурирует кластерное имя storage без `.hc` — но фактически
   PURGEABLE стал только hc-иерархия, ec/kc/pc остались MOUNTED — проверено).
5. `systemctl restart rscheck@kafka` на всех 6 живых хостах (канон 4970 — кэш
   voter-листа).
6. SQL: DELETE hc из `host_state`; UPDATE operations → `done`,
   in_processing=false, finished_ts=now(), error_message=NULL.

## Грабли

- **Выборы при рестарте лидера на mixed-config**: между stop и start ec (20:07)
  лидером успел избран hc (10001, старый 4-voter конфиг). ec стартанул уже с
  3-voter конфигом, прочитал в своём kraft-состоянии «leader 10001» и упал:
  `IllegalStateException: Leader 10001 must be in the voter set VoterSet({11001,12001,13001})`
  (паттерн 4833). systemd-стейт — failed. **Лечение — вайп данных контроллера**
  (`systemctl reset-failed`; `rm -rf /mnt/data/log/* /mnt/data/metadata/*`;
  start): PMS/конфиг корректны → ec чисто зафетчил метаданные с лидера и вошёл
  в кворум (13001 в observers→voters). Простой рестарт падал бы снова.
  Вывод: рестарт лидера в 3.8 mixed-config — окно на паразитные выборы; после
  него выводимый voter может успеть стать лидером и сломать старт любого
  рестартующегося узла.
- `kafka-topics --describe --unavailable-partitions` на 3.8 льёт INFO-дамп
  AdminClientConfig в stdout — `wc -l` считает мусор; фильтровать по `grep 'Topic:'`.
- `FencedBrokerCount` MBean — 4.x-only, на 3.8 контроллере его нет (InstanceNotFound);
  облачная модель «fenced brokers N» после даунскейла рассасывается сама
  (брокеры в метаданных только легитимные 21001/22001/23001).
- Сообщение `withdraw --type storage` показывает **кластерное** имя и число
  инстансов на момент (в т.ч. упавших) — пугает, но применяется к указанной
  DC-иерархии; после withdraw сверить `tool_status --type storage` по всем ДЦ.

## Итог

Кластер: 3 брокера (kc/pc/ec) + 3 контроллера (kc/pc/ec) + cruise(pc); кворум
лидер 11001, voters [11001,12001,13001], HWM растёт, лаг 0, URP=0,
unavailable=0, heartbeat-ошибок нет; в host_state 7 хостов; операция done.
`kafka.layout` осознанно оставлен `hc,kc,pc,ec` (стабильность node.id).
Открытый продуктовый вопрос: stage-кластеры с PLAINTEXT-листенером несовместимы
с connection-check mdb-processing (SASL_SSL/SASL_PLAINTEXT) — либо кластеры
переводить на SASL-шаблон, либо processing учитывает PLAINTEXT.
