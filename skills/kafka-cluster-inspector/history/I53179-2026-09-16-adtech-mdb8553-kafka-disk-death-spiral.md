# I53179 — каскадная гибель брокеров adtech-mdb8553-kafka: переполнение дисков из-за retention 24ч при потоке 1.38 TB/час

Дата: 2026-09-16/17
Кластер: adtech-mdb8553-kafka (KRaft, 3 ДЦ hc/pc/uc × 12 брокеров = 36, диски 4TB,
ID-серии hc=200xx, pc=210xx, uc=220xx; конфигурация pure-KRaft — дир партиций БЕЗ хэш-суффикса)
Канон-инструкция: wiki «Дежурство MDB: Kafka» (pageId=1348619075), секция «Кончилось место на брокерах»

## Симптом

Брокеры падают один за другим на 100% `/mnt/data` (UNAVAILABLE/UNKNOWN в UI),
каскад растягивается на сутки. Пик — 10 лежащих хостов одновременно:
1.hc, 2.pc, 3.pc, 5.pc, 5.hc, 5.uc, 6.hc, 7.pc, 7.uc, 8.hc, 8.uc, 1.pc(зомби).
Каждый поднятый заполняется снова (~24 GB/ч входа на старый брокер) и умирает за сутки.

## Корень

1. Три топика `recommender-vk-ad-{vk-group,lead,cpc}-features-log` — **1.38 TB/час
   записи (≈383 МБ/с) суммарно**, 96 партиций каждый, retention 24ч →
   **33.5 TB уникальных данных ≈ 100 TB raw** — физически не помещается на кластере
   со старым раскладом.
2. Дисбаланс: старые брокеры 1–8 забиты (85–100%), добавленные 9–12 пустые (2–17%) —
   ребаланс после масштабирования не делался. CC был не готов («Proposal not ready»).
3. Смерть полного брокера → его партиции деградируют → перераспределение нагрузки →
   следующий полный умирает. Спираль.

## Хронология действий (что сработало)

1. **Stray-чистка** (по вики): 1.hc ~194 GB, 1.uc ~60 GB, 5.hc ~26 GB.
2. Ручная чистка логов (`/mnt/logs/dbms`, rm kafka-*) — по вики, малый эффект
   (на 1.pc 9.6G занимал не кафка; реальная разгрузка логов — только рестарт,
   отпускающий утёкшие fd).
3. Retention-чистка `find -mmin +N` на мёртвых — **0 GB**: данные внутри retention,
   диск забит валидными данными. Чистить руками нечего — нужен reassign или retention.
4. Прокатка **TOS-агента**: `confp --oneshot && systemctl restart kafka-broker`
   поочерёдно на все 36 — 33/36 active.
5. **Ручные реассигны (4 волны, ~38 партиций ~8 TB) с мёртвых и жирных брокеров
   на пустые 9–12 своих ДЦ** через `kafka-reassign-partitions.sh --additional --execute`
   БЕЗ throttle (throttle-шаг падает на мёртвых участниках — см. грабли).
   Правила отбора: живой лидер, ISR не деградирует (1 партиция = 1 удаление),
   исключены in-flight и тупики (`Leader: none`, ISR только на мёртвом).
6. **Конвейер оживления мёртвых**: ls диры → кросс-референс с текущими Replicas
   (свежий `kafka-topics --describe` → python на 2.hc) → rm деассigned-дир →
   df → `systemctl start kafka-broker` → проверка регистрации. Недостаточно места —
   приём «снос крупных назначенных дир» (брокер перекачает с живых лидеров на старте).
7. **Retention 24ч → 20ч → 4ч** через `kafka-configs --alter
   --entity-type topics --entity-name <t> --add-config retention.ms=...`
   — брокеры сами удалили ~28 TB уникальных (~84 TB raw) за десятки минут,
   диски 100% → 12–23%. **Спираль остановлена именно этим.**
8. `num.replica.fetchers` 1→8 (PMS `kafka.broker.properties` + поочерёдный
   confp+restart, 36/36) + `replica.lag.time.max.ms=60s` (динамика) — против
   «репликация не успевает» и ISR-флейпов.
9. Зависший reassign lead-46 (Adding+Removing одна реплика, лидер=удаляемая):
   отмена сабмитом до-переездного назначения + `kafka-leader-election --election-type
   unclean --partition 46` на ISR-реплику (выбор из ISR = ноль потерь).
10. CC `OngoingExecutionException` после пустой очереди — стейт старого прерванного
    исполнения в памяти CC → `systemctl restart cruise-control` → разблокирован.

## Итог

- 36/36 брокеров в кластере, диски 12–23%, URP/under-min/at-min → нули
  (at-min остаток = RF2-партиции, юзерам рекомендовано вернуть RF3).
- Retention 4ч в кафке (kafka.sync направлен кафка→UI — в UI дублировать не нужно).
- CC перезапущен с чистым стейтом, ребаланс — опциональная косметика.

## Открытые хвосты

- Контроль: kafka.sync не откатил retention (периодически проверять retention.ms).
- Список RF2-партиций юзерам для возврата RF3.
- Подтвердить выкачку консьюмер-бэклога на тупиковых партициях (46/63/56/68) —
  до этого не снижать retention ниже их бэклога.
- CC dry-run rebalance в спокойный период для выравнивания размещения.

## Грабли и приёмы (полная версия — ~/.paiw/.learnings/2026-09-16-kafka-reassign-dead-broker-throttle.md)

- `kafka-reassign --execute --throttle` падает `TimeoutException ... incrementalAlterConfigs`,
  если среди участников мёртвый брокер → запускать БЕЗ --throttle (сетью рулит TOS).
- `Cannot execute because there is an existing partition assignment` → флаг `--additional`.
- `Unknown broker id X` — контроллер АВТОразрегистрировал мёртвого брокера; его id нельзя
  упоминать в reassign-json вообще (даже в остающихся репликах). Расклинивание:
  снос части дир → старт → перерегистрация → reassign.
- Большой inline-base64 в `mcc sshexec` → `431 Request Header Fields Too Large`:
  файлы гнать gzip|base64 чанками по 1200 с `printf %s` и `while IFS= read -r C || [ -n "$C" ]`.
- **zsh не сплитит unquoted $VAR** (`for X in $LIST` = одна итерация!) — только `${=LIST}`.
- mcc внутри `while read`-пайпа жрёт stdin → `</dev/null`.
- Jolokia (7777) GET-формат: `.../read/kafka.server:type=ReplicaManager/UnderReplicatedPartitions`
  (атрибут БЕЗ `name=`); атрибуты в ответе по алфавиту (AtMin < UnderMin < UnderReplicated).
- `kafka-configs` этой версии: `--entity-name`, НЕ `--topic-name`; в describe
  `grep 'retention.ms='` ловит и `delete.retention.ms` — якорить регулярку.
- Оценки лагов/объёмов: kafka-log-dirs снимок + max-реплика по партиции;
  темп записи — пара сэмплов `kafka-get-offsets --time -1` с интервалом.
- Приоритет при каскаде: оживить хостов больше > идеально вылечить один
  (перекачка при старте — приемлемая плата), НО не удалять диры партиций,
  у которых этот хост — единственная ISR-копия (тупики, пример vkg-63).
