# 2026-08-20: Вывод мёртвых брокеров 200xx/230xx из Replicas на eventstream-general-adtech-kafka

**Дата**: 2026-08-20
**Кластер**: `eventstream-general-adtech-kafka` (ID `4d52f5b5-16b5-4c44-a3c0-ea5fe3959e63`)
**Версия Kafka**: 3.8.0 (KRaft)
**Живые брокеры**: 18 шт — pc=21001-21006, rc=22001-22006, ec=24001-24006 (по 6 на ДЦ)
**Controllers**: 11001@pc, 12001@rc, 14001@ec
**RF**: 3, схема «1 реплика на ДЦ» (pc/rc/ec)

## Контекст

Брокеры 20001-20006 и 23001-23004 (бывшие ДЦ, хосты удалены из облака) остались в metadata
Kafka в Replicas партиций. DescribeCluster их не показывал (живых 18), но **159 партиций**
держали мёртвые ID в Replicas. Unavailable = 0 (лидеры живы, unclean election не потребовался),
under-replicated = 242 строк. Затронуты топики: `stat`, `statd-general`, `staging`,
`__consumer_offsets`, `__CruiseControl*` (CC на кластере есть, но мёртвых не убрал).

## Диагностика

1. Живые брокеры — `kafka-broker-api-versions.sh | grep -oE "id: [0-9]+"` → 18 ID.
2. Все ID в Replicas — `kafka-topics.sh --describe | grep -oE 'Replicas: [0-9,]+' | tr ',' '\n' | sort | uniq -c`
   → нашлись 20001-20006 и 23001-23004, отсутствующие в describeCluster.
3. Список битых партиций — `--describe | grep -E 'Replicas:.*(20001|...|23004)' > /tmp/dead_partitions.txt` (159 строк).

Маппинг ДЦ на ID (уточнён по `controller.quorum.voters` и node.id хоста 1.broker.ec=24001):
21xxx=pc, 22xxx=rc, 24xxx=ec, мёртвые: 20xxx и 23xxx.

## Генерация reassign.json

Python-скрипт локально (`/tmp/opencode/reassign/gen_reassign.py`), стратегия:
- заменять только мёртвые ID, живые реплики не трогать (preferred leader сохраняется);
- приоритет — ДЦ, отсутствующие в живых репликах партиции (восстановление схемы pc+rc+ec);
- выбор брокера — least-loaded с учётом уже назначенных в этом прогоне;
- второй проход: 9 партиций с двумя rc-репликами (наследие старого размещения) — дубликат
  заменён на недостающий ДЦ.

Проверки перед выполнением: 0 мёртвых ID, 0 дубликатов, 159/159 с 3 разными ДЦ.
Нагрузка новых реплик: ~26-27 на брокера.

## Выполнение

- `--execute --throttle 104857600` → `TimeoutException: incrementalAlterConfigs` —
  та же грабля, что в MDBSUP-4166. Throttle не применился, reassign не стартовал.
- `--execute` без throttle → `Successfully started partition reassignments` (все 159).
- Через 60 сек `--verify` → **159 completed, 0 in progress**. Данные прокачались быстро
  (статовые топики + CC samples; __consumer_offsets 24 партиции тоже успели).

## Результат

```
200xx/230xx в Replicas: 0  (было 159 партиций)
Under-replicated: 0  (было 242 строк)
Unavailable: 0
```
Throttle чистить не пришлось (execute без --throttle не ставит throttle-конфиги).
Распределение реплик по живым брокерам неравномерное (78/78/51/35/36/37 pc, 93/41/43/42/63/33 rc,
78/60/43/59/37/38 ec) — наследие старого размещения живых реплик; выравнивание — задача Cruise Control.

## Грабли

1. **`mcc instances` не находит кластер** ни по UUID, ни по FQDN (`EntityNotFoundException`),
   при этом `mcc sshexec` на конкретный хост работает. Не тратить время на instances/status —
   сразу заходить на известный хост.
2. **`mcc scp` молча не заливает файл** (локально OK, на хосте файла нет, ошибки нет).
   Рабочий путь — base64 чанками по 800 символов через `mcc ssh + expect` (46 чанков на 27KB).
3. **Throttle → TimeoutException на incrementalAlterConfigs** — повторилось второй раз
   (первый — MDBSUP-4166). Похоже, систематическое поведение: при выводе мёртвых брокеров
   запускать сразу `--execute` без `--throttle`.
4. **Sanity-check генератора обязателен**: баг `b // 100` вместо `b // 1000` дал бы 12 партиций
   с двумя брокерами одного ДЦ. Проверять: нет мёртвых ID, нет дубликатов, 3 разных ДЦ в каждой партиции.

## Файлы

- `/tmp/opencode/reassign/dead_partitions.txt` — исходные 159 партиций
- `/tmp/opencode/reassign/gen_reassign.py` — генератор (least-loaded + восстановление ДЦ-схемы)
- `/tmp/opencode/reassign/reassign.json` — итоговый reassign (27201 байт)
- Rollback JSON — в stdout execute («Save this to use as the --reassignment-json-file option during rollback»)
