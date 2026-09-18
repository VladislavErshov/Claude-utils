# MDBSUP-5179 — диски 100% на брокерах rc/uc: чистка протухших test/stage-топиков по ретеншену (диски расширить нельзя)

Дата: 2026-09-07
Тикет: https://jira.vk.team/browse/MDBSUP-5179
Кластер: `vk-video-dscv` (`53ea5f26-25b2-4595-ae8f-e161f22d1a7b`), KRaft, 3×broker rc/uc/kc
Хосты: `1.broker.vk-video-dscv-mdb10084-kafka.{rc,uc,kc}.one-infra.ru`
Канон процедуры: Confluence «Дежурство MDB: Kafka» (pageId=1348619075), секция
«Кончилось место на брокерах» → «В остальных случаях».

## Симптом

RC и UC брокеры лежат (`kafka-broker failed`), в логах
`java.nio.file.FileSystemException: /mnt/data/log/__cluster_metadata-0/... No space
left on device` при старте. Кластер UNAVAILABLE (1 из 3 брокеров жив). Диски:

| Хост | /mnt/data | /mnt/logs |
|---|---|---|
| rc | 1000G **100%** (free 1.1M) | 5% |
| uc | 1000G **100%** (free 936K) | 5% |
| kc | жив, ~15% | — |

Расширить диски нельзя: у юзера нет квоты (другая операция держит ресурсы) →
чистим место руками.

## Диагностика

1. `df -h` + `du -sk /mnt/data/log/* | sort -rn | head` на rc/uc — `-stray` партиций
   **нет**, диск забит двумя топиками:

   ```
   unisearch_vk_video_dscv_generic_vk_sessions_test   ~434 GB (32 партиции × ~7.4GB)
   unisearch_vk_video_dscv_generic_vk_sessions_stage  ~434 GB (32 партиции × ~7.4GB)
   ```
   Итого ~868 GB из 992 GB занятого /mnt/data/log на каждом брокере.

2. Ретеншен топиков (с живого kc):
   `kafka-configs.sh --bootstrap-server <kc>:9092 --command-config /opt/kafka/config/client.properties
   --entity-type topics --entity-name <topic> --describe`:
   - `cleanup.policy=delete`, `retention.ms=604800000` (**7 суток = 10080 мин**), `retention.bytes=-1`
3. Возраст файлов: `ls -lt` в партиции — свежайший сегмент **17 августа 13:14** (21 день),
   старейший 13 августа → **весь диск с этими топиками давно протух по собственному
   ретеншену**. Писать перестали ~17.08, брокеры упали, ретеншен-клинер не работал,
   а стартовать без места брокер не может (замкнутый круг).

## Фикс (по доке, брокер остановлен — безопасно)

На каждом broker-хосте rc/uc, по всем 64 партициям двух топиков:

```bash
cd /mnt/data/log
for d in unisearch_vk_video_dscv_generic_vk_sessions_test-* unisearch_vk_video_dscv_generic_vk_sessions_stage-*; do
  [ -d "$d" ] || continue
  cd "/mnt/data/log/$d"
  cp -p partition.metadata leader-epoch-checkpoint /tmp/ 2>/dev/null
  find . -type f -mmin +10080 -delete          # 604800000 мс = 10080 мин
  mv /tmp/partition.metadata /tmp/leader-epoch-checkpoint . 2>/dev/null
  cd /mnt/data/log
done
df -h /mnt/data
```

Результат: rc и uc `100% → 14%` (освобождено 869/867 GB), `partition.metadata` и
`leader-epoch-checkpoint` на месте. Удалено ровно то, что Kafka сам бы удалил по
retention.ms (все сегменты старше 7 суток; write-ам 21 день).

Дальше — `systemctl restart kafka-broker` (в our case рестарт через sshexec упал с
`OCI runtime error 129` на rc, юзер поднимал руками; проверять `is-active` и
регистрацию брокера в кластере после старта).

## Корень

Test/stage-топики unisearch (vk video) налили по ~434 GB на брокера, 17.08 продюсер
остановился. Ретеншен 7 суток не отработал, потому что брокеры к тому моменту уже
лежали с переполненным диском (первопричина переполнения — сами эти топики при
retention.bytes=-1 и записи до отказа диска). Классическая петля: брокер не стартует
из-за полного диска, а чистить некому, кроме рук.

## Грабли

1. **Не угадывать FQDN кластера**: `1.broker.vk-video-dscv.rc.one-infra.ru` →
   `EntityNotFoundException`. Реальное имя с суффиксом из UUID:
   `1.broker.vk-video-dscv-mdb10084-kafka.rc.one-infra.ru`. Список хостов брать через
   `mcc --local -n infra -c <dc> instances "%.broker.<cluster>%" -f xargs:host`.
2. **`__cluster_metadata-0` в ошибке — не мишень для чистки**: fatal просто проявился
   на KRaft-метадате (нечем писать). Чистим только data-партиции топиков.
3. **`find -delete` сносит не только данные**: вместе с `.log/.index/.timeindex`
   удаляются `partition.metadata` и `leader-epoch-checkpoint` — в доке упомянут
   только первый, бэкапить/возвращать **оба**.
4. Перед `find -mmin` проверить возраст свежайшего сегмента (`ls -lt`): если данные
   внутри ретеншена — удаление по mtime зацепит живое. Здесь всё было старше 21 дня
   при ретеншене 7 суток.
5. awk-суммарки внутри sshexec работают, если `$1` экранировать как `\$1` внутри
   локальной одиночной кавычки (см. I49678 про вложенные кавычки — здесь шаблон
   `du -sk ... | awk '{s+=\$1} END {print ...}'` прошёл).
6. `systemctl restart` через `mcc sshexec` может отдать `OCI runtime error: 129` —
   рестарт руками через `mcc ssh` / пользователем.
7. На UC часть файлов была датирована днём инцидента (16:15) — след недавней попытки
   старта брокера (пересоздал маленький active segment перед повторной смертью).
   Файлы свежее порога ретеншена `find` не трогает — это нормально.

## Итог

- Брокеры rc/uc подняты пользователем вручную (`systemctl start kafka-broker`) после
  чистки; диск 14% на обоих.
- Причина переполнения у юзера: test/stage-топики unisearch с `retention.bytes=-1`
  при активной записи до отказа диска; продюсер встал 17.08. Предложить юзеру
  выставить retention.bytes/retention.ms на таких топиках или удалить их, иначе
  повторится.
