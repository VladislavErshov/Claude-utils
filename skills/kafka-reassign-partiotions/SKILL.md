# Скилл ручного перераспределения партиций Kafka

Скилл для генерации и выполнения `kafka-reassign-partitions.sh` по **заданной схеме
размещения реплик**, когда встроенный Cruise Control неприменим (нужно специфическое
распределение «жирных»/«средних» партиций, вывод брокера из кластера, ручная балансировка
дисковой нагрузки между ДЦ).

⚠️ Скилл покрывает **только ручное перераспределение партиций через
`kafka-reassign-partitions.sh`**. Он НЕ покрывает:
- автоматический ребаланс через Cruise Control — см. `kafka-cluster-inspector` (`commands/cruise_control_ops.md`);
- инспекцию логов / метрик / KRaft quorum — см. `kafka-cluster-inspector`, `kafka-metrics-investigator`;
- throughput / latency — к Prometheus/Grafana напрямую.

## Когда предлагать скилл

Предлагай этот скилл, когда пользователь:
- Просит «перераспределить партиции» / «сделать реассигн» / «reassign partitions» для конкретного топика или набора партиций.
- Даёт **явную схему размещения** (какие партиции на какие брокеры должны попасть), а не просит «сбалансировать автоматически».
- Хочет вывести брокер из кластера и переселить его партиции.
- Видит дисбаланс дисковой нагрузки между брокерами/ДЦ и хочет вручную перекинуть часть партиций.
- Упоминает `kafka-reassign-partitions.sh` явно.

**НЕ предлагай**, если:
- Просит автоматический ребаланс — это к Cruise Control (`kafka-cluster-inspector`).
- Нужно изменить RF (replication factor) — это `kafka-configs.sh --alter --add-config replication.factor=...`, не reassign.
- Нужно просто добавить партиции — это `kafka-topics.sh --alter --partitions N`.

## Документация

- https://kafka.apache.org/documentation/#basic_ops_cluster_expansion — добавление брокеров и reassign
- https://kafka.apache.org/documentation/#replication_throttling — throttle при reassign
- https://docs.vk.team/mdb/docs/kafka/kafka.html — детали MDB Kafka

## Архитектура

### Формат reassign.json

```json
{
  "version": 1,
  "partitions": [
    {
      "topic": "<topic-name>",
      "partition": <N>,
      "replicas": [<broker_id_1>, <broker_id_2>, <broker_id_3>],
      "log_dirs": ["any", "any", "any"]
    }
  ]
}
```

- `replicas` — **полный** новый список broker IDs для партиции (включая те, что не меняются).
  Порядок важен: первый — preferred leader.
- `log_dirs` — всегда `["any", ...]` для обычного reassign (используется только при переносе между дисками).

### Команды

```bash
# Выполнить reassign (с throttle 100 МБ/с)
/opt/kafka/bin/kafka-reassign-partitions.sh \
  --bootstrap-server $cloud_hostname:9092 \
  --command-config /opt/kafka/config/client.properties \
  --reassignment-json-file /tmp/reassign.json \
  --execute --throttle 104857600

# Проверить статус
/opt/kafka/bin/kafka-reassign-partitions.sh \
  --bootstrap-server $cloud_hostname:9092 \
  --command-config /opt/kafka/config/client.properties \
  --reassignment-json-file /tmp/reassign.json \
  --verify
```

`$cloud_hostname` — это переменная окружения на брокере, резолвится в bootstrap-server кластера.

### Throttle

- `--throttle <bytes/sec>` ограничивает скорость переноса реплик.
- Без `--throttle` лимита нет (быстрее, но может перегрузить сеть/диск).
- При `--execute` без `--throttle` throttle-конфиг на топик **не ставится** — чистить нечего.
- Если throttle был установлен, после завершения reassign его **нужно снять**:

```bash
/opt/kafka/bin/kafka-configs.sh \
  --bootstrap-server $cloud_hostname:9092 \
  --command-config /opt/kafka/config/client.properties \
  --entity-type topics --entity-name <topic> \
  --alter --delete-config leader.replication.throttled.replicas,follower.replication.throttled.replicas
```

## Стратегия сохранения cross-DC redundancy

В MDB Kafka типичный RF=3 с одной репликой в каждом ДЦ (hc/kc/pc). При ручном reassign:

1. **Меняем только одну DC-реплику за раз** — остальные две оставляем на месте.
2. Перед заменой убеждаемся, что новый брокер находится в том же ДЦ, что и заменяемый.
3. Никогда не ломаем схему «1 реплика на ДЦ» — иначе при потере ДЦ теряем данные.

Например, для кластера `kafka-m2b-adtech-kafka` (hc=20001-20006, kc=21001-21007, pc=22001-22006):
- Если меняем kc-реплику — заменяем `210xx` на другой `210xx` в списке `replicas`.
- hc (`200xx`) и pc (`220xx`) реплики остаются на своих позициях.

## Процесс генерации reassign.json

1. **Собрать текущее состояние** — `kafka-topics.sh --describe --topic <topic>`, распарсить `Replicas:` поле.
2. **Составить схему нового размещения** — какая партиция на какой брокер должна перейти.
3. **Сгенерировать JSON** — Python-скриптом, заменяя только нужные broker IDs, сохраняя порядок и hc/pc реплики.
4. **Проверить** — вывести diff «что меняется», сколько партиций затронуто.
5. **Выполнить** — `--execute --throttle 100MB`.
6. **Верифицировать** — `--verify` до статуса `completed` для всех партиций.
7. **Снять throttle** — если был установлен.

### Шаблон Python-скрипта

Скрипт-генератор хранит `CURRENT_REPLICAS` (словарь partition → список broker IDs из `kafka-topics.sh --describe`) и `NEW_<DC>_REPLICA` (словарь partition → новый broker ID для конкретной DC). Затем заменяет DC-реплику в текущем списке, сохраняя порядок иPreferred leader'а.

Примеры — в `history/INCALL-42698.md` (кампания с fat/medium партициями), `history/MDBSUP-4166.md` (вывод удалённого broker 22026 из Replicas через unclean election + reassign) и в файле `/tmp/gen_reassign.py`.

## Инструменты и нюансы выполнения

> **Сначала читай [`/mcc-host-access`](../../mcc-host-access/SKILL.md)** — все паттерны
> доступа к хостам (ssh/sshexec/scp/expect), грабли Tcl/expect, ANSI-коды, base64-загрузка
> файлов собраны там. Ниже — только специфика reassign.

- **ANSI-коды в выводе** — `grep` подсвечивает совпадения цветом, что ломает парсинг.
  Фильтровать через `sed -E 's/\x1b\[[0-9;]*[mK]//g'`.
- **Загрузка reassign.json на хост** — `mcc scp` либо молча падает, либо создаёт
  директорию вместо файла (см. `mcc-host-access/commands/scp.md`). Рабочие варианты:
  - `mcc scp /tmp/reassign.json <host>:/tmp/` (dest **директория** с trailing `/`).
  - При нестабильном scp — base64 поверх `mcc ssh + expect`. **Важно:** `mcc sshexec`
    падает с `414 URI Too Long` уже на ~8KB base64 (команда уходит в URL). Поэтому
    base64 нужно дробить на чанки по ~800 символов и собирать на хосте:
    ```bash
    python3 -c "
    import base64
    b64 = base64.b64encode(open('/tmp/reassign.json','rb').read()).decode()
    chunks = [b64[i:i+800] for i in range(0, len(b64), 800)]
    lines = ['set timeout 120', 'spawn mcc --local ssh <host>', 'expect \"/# \"',
             'send \"rm -f /tmp/r.b64\\r\"', 'expect \"/# \"']
    for i, c in enumerate(chunks):
        op = '>' if i == 0 else '>>'
        lines.append(f'send \"printf %s \\'{c}\\' {op} /tmp/r.b64\\r\"')
        lines.append('expect \"/# \"')
    lines += ['send \"base64 -d /tmp/r.b64 > /tmp/reassign.json && wc -c /tmp/reassign.json\\r\"',
              'expect \"/# \"', 'send \"exit\\r\"', 'expect eof']
    open('/tmp/upload.exp','w').write('\\n'.join(lines)+'\\n')
    "
    expect -f /tmp/upload.exp 2>&1 | tail -20
    ```
- **JMX-метрика размера лога** — `kafka_log_log_size{partition="N",topic="X",}` на порту 8080 broker-хоста. Используется для оценки дисковой нагрузки партиции.

## Структура скилла

- `SKILL.md` — этот файл.
- `history/` — разобранные инциденты с полным ходом работы (сбор информации, генерация JSON, выполнение, верификация). Полезно перед началом работы смотреть, нет ли похожего случая.

## Что НЕ покрывает скилл

- Cruise Control execution — см. `kafka-cluster-inspector`.
- Добавление партиций — это `kafka-topics.sh --alter --partitions`.
- Intrinsic Kafka CLI особенности (создание топиков, ACL) — см. `kafka-cluster-inspector` (`commands/administration.md`).

## Что покрывает скилл, но неочевидно

- **Снижение RF** (replication factor) — это reassign с укороченным списком `replicas`.
  `kafka-configs.sh --alter --add-config replication.factor=...` **не работает** для
  изменения RF — это static-конфигурация темы. Реальный путь: сгенерировать
  reassign.json, где для каждой партиции указан новый (более короткий) список реплик,
  и выполнить `--execute`. Важно сохранить preferred leader (первую реплику) и
  кросс-DC redundancy (минимум 3 разных DC в новом списке). Пример — см.
  `history/` (кампания по снижению RF с 5/4/7 до 3 на топике uvThinStatPure).
