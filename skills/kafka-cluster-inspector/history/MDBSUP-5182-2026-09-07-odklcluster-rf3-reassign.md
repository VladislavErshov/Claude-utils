# MDBSUP-5182: odklcluster-kafka — поднять RF=3 и min.insync.replicas=2 на всех топиках

**Дата**: 2026-09-07
**Кластер**: `odklcluster-kafka` (6ad6f8df-f56f-4f4c-92a4-6b768d2ce38e), project 80, production
**Топология**: 240 брокеров (60 × dc/ec/pc/rc) + 3 контроллера (ec/pc/rc), KRaft, **Cruise Control нет**
**Namespace mcc**: `dzen`, FQDN в облаке `*.idzn.ru` (в прод-БД host_state — `*.wan.idzn.ru`, другой вариант имени!)

## Запрос

Фичареквест: RF=3 и min.insync.replicas=2 на всех топиках кластера.

## Диагностика

1. **Прод-БД**: 243 хоста, все операции `done` (ничего не блокирует).
2. **Один `kafka-topics --describe` с брокера → в файл → `cat` через sshexec** (scp упал
   `failed to read downloaded archive header: EOF` ×5; `cat` file через sshexec работает).
   46 топиков, 1290 партиций. RF=2 на 5 топиках (432 партиции):
   `userActivity` (72, **~6 TB**), `objectMetadataUcp` (72, ~139 GB), `videoEvents` (144, пуст),
   `pulseTopicsEvents` (72, пуст), `byObjectId_metadataUpdates72` (72, пуст).
3. **Размещение RF=2**: пары ДЦ (ec,pc)/(pc,rc)/(ec,rc) по 144 партиции. Все RF=3 топики
   размещены строго `(ec,pc,rc)` — **ДЦ dc данных не держит** (конвенция кластера,
   третью реплику класть только в недостающий ДЦ из тройки!).
4. **min.insync.replicas=2 уже стоял broker default** (статика `broker.properties`,
   динамических/topic-level override'ов нет) — по min.insync делать было нечего,
   по решению пользователя дополнительно запинили topic-level на все 46.
5. **Rack-метки реальны и отличаются от таблицы скилла**: `kafka-broker-api-versions`
   показал `rack`: dc=20xxx, **pc=21xxx, rc=22xxx, ec=23xxx** (в таблице SKILL.md
   пример был pc=23001 — тут иначе; IDs брать только из live-API, не из таблицы).

## Фикс

1. Reassign-JSON сгенерирован локально из describe: к каждой из 432 партиций третья
   реплика в недостающий ДЦ, внутри ДЦ round-robin по 60 id → ровно 144 новых реплик
   на ДЦ, 2–3 на брокера.
2. **Заливка JSON на хост** — scp не работает в обе стороны (EOF). Рабочий способ:
   `base64 -b 1000` → строки по 1KB → expect-скрипт через интерактивный
   `mcc ssh` + heredoc `cat > /tmp/f.b64 << 'EOF64'` → decode, **сверить md5**.
   ⚠️ `sshexec` с аргументом >~7KB падает `431 Request Header Fields Too Large`;
   чанки 8KB НЕ влезают, а частично приложившийся мусор портит файл — чистить и
   перезаливать.
3. `kafka-reassign-partitions.sh --execute` **без throttle** (решение пользователя).
   Все 432 партиции стартовали одной командой.
4. **Прогресс**: `--verify` → счётчик `still in progress` (мелкие топики ушли за ~2 мин).
   Скорость мерить через `kafka-log-dirs` сумму size: в KRaft (KIP-455) `isFuture:true`
   НЕ появляется — новые реплики сразу в Replicas (216 записей = 72×3).
   ⚠️ awk-сумма байтов упирается в 2^31 — выводить `printf "%.0f"`.
   Фактическая скорость ~116 GiB/мин (~2 ГБ/с), ~3.1 TB перелились за ~30 мин.
5. **Пин min.insync**: `kafka-configs --alter --add-config min.insync.replicas=2` на все
   46 топиков одним sshexec-циклом (1.4KB команда влезла).
6. **Чистка throttle-хвостов**: `--execute` выставил
   `leader/follower.replication.throttled.replicas=*` на 27 топиков, а `--verify`
   их НЕ убрал (противоречит заметке в kafka-stats-conv-кейсе — там не появились).
   Удаление на топиках без этих конфигов даёт `Invalid config(s): ...` — это норма
   (нешибка = чистить нечего). Итог: 0 остатков.

## Финальная валидация

46/46 RF=3, ISR полный везде, leaderless нет, `min.insync.replicas=2` на 46 топиках,
throttled-остатков нет. Тикет закрыт с комментарием.

## Грабли

- В новой строке describe есть `TopicId:` → парсер строк `Topic: X\tPartition:` должен
  допускать поле TopicId, а topic-level Configs живут в summary-строке с
  `PartitionCount/ReplicationFactor`.
- `kafka-topics.sh` не в PATH под sshexec — полный путь `/opt/kafka/bin/...`,
  `--command-config /opt/kafka/config/client.properties`.
- sshexec регулярно роняет вывод `OCI runtime error`/`Connection closed` после полезных
  строк — данные/файл на месте, перечитывать файл, не паниковать.
- Логи AdminClient пишутся в stdoutdescribe-файлы — фильтровать при парсинге.
