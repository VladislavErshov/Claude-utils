# I53179 (2026-09-15..18) — adtech-mdb8553-kafka: лаг брокеров из-за noisy neighbor на диске

## Симптомы
- INCALL-53179 «Растет лаг по feature-log-generated», P2, продукт DataTransfer
- Broker Max Lag (= `kafka.server_replicafetchermanager_maxlag`, ReplicaFetcherManager MaxLag)
  рос на подмножестве брокеров; URP до 200+, at-min-ISR до 20
- 15.09: падали брокеры, шел update образов (TOS agent) 3/12 шардов, снижали retention до 4ч,
  CC-ребаланс 7+ ТиБ (миграция реплик со старых брокеров 1-8 на новые 9-12)

## Кластер
- adtech (mdb8553), 36 брокеров: по 12 в hc/pc/uc; ID-карта: 200xx=hc, 210xx=pc, 220xx=uc
- CC REST: порт **9000** (не 9090), на 1.cruise.<cluster>.<dc>; медленный — таймауты 240с+
- Конфиг брокера: `/opt/kafka/config/broker.properties` (НЕ server.properties — там сток);
  PMS `kafka.broker.properties`: num.replica.fetchers=8 — согласовано с хостами

## Цепочка диагностики (что сработало)
1. Сначала грешили на Kafka/конфиги: троттл CC 100 МиБ/с → подняли 300 → 2000 → снизили до 700
   (влияние есть, но хвост на 1.hc не уходил); `num.replica.fetchers` 8→16 динамически
   (`kafka-configs --alter`, валидация ≤2x текущего; `replica.fetch.*` — НЕ динамические,
   probe: «Cannot update these configs dynamically»)
2. `min.insync.replicas=2` выставлен на все 16 бизнес-топиков (был broker default = не задан = 1)
3. Рост лага 1.hc с 5 утра = утренний трафик; produce проверен через
   `BrokerTopicMetrics BytesInPerSec` = всего 5 МБ/с на брокер — квоты на продюсеров бессмысленны
4. **Развязка**: iostat nvme1n1 на 1.hc — util 83-99%, aqu-sz 123-309, запись 1.28 ГБ/с —
   при produce 5 МБ/с и ReplicationBytesInPerSec ≈ 0 (фетчеры не качают, т.к. диск занят)
5. **Noisy neighbor доказан**: запись устройства 1.28 ГБ/с против записи контейнера
   (cgroup blkio дельта) **78 КБ/с** → 99.9% записи — сосед по миньону srvh81 (поток ~108КБ/запись)
6. Миньон инстанса: `mcc --local -n infra -c <dc> instances '*broker.<cluster>*'` → minion: srvh81

## Решение
- `mcc --local -n infra migrate --relocate --auto_solve "adtech-mdb8553-kafka.mdb8553.db.production.mdb.prod/broker/4(или/N)"`
  (canon: `mcc-host-worker/commands/migrate.md`; имя — storage/shard, не FQDN)
- Мигрированы 1.hc (srvh81 → новый миньон, тома ПЕРЕНЕСЛИСЬ С ДАННЫМИ: 313-351G на месте,
  Kafka Server started через ~4 мин) и 4.hc (srvh2689)
- После миграции 4.hc: инстанс PREEMPTED «not scheduling on a minion» → нужен `mcc start <fqdn>`
- CC-исполнение реассигна прервалось при миграции (11:07 «Inter-broker partition movements stopped»);
  throttled.replicas остались в конфигах топиков как мусор — почистить при необходимости

## Проверка после миграции
- `systemctl is-active kafka-broker`, BrokerState=3, `Kafka Server started` в логе
- `df -h /mnt/data` — данные на месте (не пустой том!)
- Повторная blkio-дельта на новом миньоне — соседа нет
- MaxLag начал падать (27.8M → 24-26M за первые минуты)

## Фоллоу-апы
- Тикет хостингу по srvh81: сосед с записью 1.2 ГБ/с; проверить 12.pc/12.uc
  (load 113-162 при пустых дисках — CPU-сосед)
- RF=2 партиции: recommender-vk-ad-lead-features-log p60, recommender-vk-ad-vk-group-features-log
  p46/p72 — доресайзнуть после полного завершения CC (один reassign-таск на кластер)
- nvme-cli/smartctl не установлены на хостах — SMART (media_errors, percentage_used) недоступен;
  health дисков смотреть в облаке (goc) или завести задачу на установку nvme-cli
- Урок: «диск не справляется» на брокере — сначала доказать ЧЬЯ это нагрузка
  (устройство vs контейнер vs java), потом крутить Kafka
