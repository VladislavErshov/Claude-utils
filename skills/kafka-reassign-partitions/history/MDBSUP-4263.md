# MDBSUP-4263: Снижение RF топика uvThinStatPure с 5/4/7 до 3

**Дата**: 2026-07-30
**Кластер**: `uv-stat-prod-uv-kafka`
**Брокер доступа**: `1.broker.uv-stat-prod-uv-kafka.ec.one-infra.ru`
**Версия Kafka**: 3.8.0 (KRaft)
**Исполнитель**: Claude

## Контекст

Топик `uvThinStatPure` (1024 партиций) в метаданных значился как RF=5, но фактическое
число реплик по партициям было неоднородным — результат незавершённой ранее операции
снижения RF. Распределение на старте:

| RF | Кол-во партиций |
|----|-----------------|
| 3  | 933 (уже норма) |
| 4  | 63              |
| 5  | 26              |
| 7  | 2 (P246, P563)  |

Кластер 5-DC по broker-id: 200xx, 210xx, 220xx, 230xx, 240xx. RF=5 партиции содержали
по 1 реплике в каждом DC. RF=7 партиции (P246, P563) — аномалия с дублями по DC:
2×ec (240xx) + 2×hc (200xx) + по 1 из kc/pc/uc.

## Стратегия

Для 91 партиции с RF>3 — сохранить preferred leader (первую реплику) + первые 2 реплики
из разных DC от лидера и друг от друга. Гарантирует:
- лидер не меняется (нет leader election);
- 3 разных DC в каждой новой тройке;
- детерминированный выбор.

Примеры:
```
P0   (RF=5): [24004,21002,23001,20001,22011] -> [24004,21002,23001]  (ec,kc,uc)
P246 (RF=7): [24008,22003,24044,20023,21005,20013,23001]
          -> [24008,22003,20023]  (ec,pc,hc)  — 24044 пропущен (дубль ec)
```

## Ход работы

### 1. Сбор состояния

```bash
mcc --local sshexec -n infra 1.broker.uv-stat-prod-uv-kafka.ec.one-infra.ru \
  "/opt/kafka/bin/kafka-topics.sh \
   --bootstrap-server \$cloud_hostname:9092 \
   --command-config /opt/kafka/config/client.properties \
   --describe --topic uvThinStatPure" > /tmp/topic_describe.txt
```

Вывод сохранить в файл — 1024 партиций, ~130KB, через `grep -oE "Replicas: [0-9,]+"`
считаем RF по `awk -F, '{print NF}' | sort | uniq -c`.

### 2. Генерация reassign.json

Python-скрипт `/tmp/gen_reassign.py`:
- парсит `Partition:` / `Leader:` / `Replicas:` из describe-вывода;
- для партиций с RF<=3 — пропускает;
- для остальных — `pick_new_replicas()`: leader + первые 2 реплики из других DC
  (DC определяется как `broker_id // 1000`);
- пишет `/tmp/reassign.json`.

```python
def pick_new_replicas(replicas):
    leader = replicas[0]
    result = [leader]
    seen_dc = {dc_of(leader)}
    for r in replicas[1:]:
        d = dc_of(r)
        if d in seen_dc:
            continue
        result.append(r)
        seen_dc.add(d)
        if len(result) == 3:
            break
    return result
```

Результат: 91 партиция в reassign.json (63+26+2).

### 3. Загрузка reassign.json на брокер

`mcc scp` и `mcc sshexec` оба не справились:
- `mcc scp /tmp/reassign.json <host>:/tmp/reassign.json` — молча ничего не загружает
  (создаёт директорию с именем файла, если указать путь с именем);
- `mcc scp ... <host>:/tmp/` — тоже не сработал;
- `mcc sshexec` с base64 (26KB) — `414 URI Too Long` (sshexec кодирует команду в URL).

Рабочий вариант — **chunked base64 через `mcc ssh + expect`**: base64 делится на чанки
по 800 символов, каждый чанк отправляется через `printf '%s' 'chunk' >> /tmp/r.b64`,
после чего `base64 -d /tmp/r.b64 > /tmp/reassign.json`. См. готовый паттерн в
`SKILL.md` → «Инструменты и нюансы выполнения».

Итоговый размер: 26104 байта b64, 33 чанка, 19577 байт JSON на хосте.

### 4. Execute

```bash
expect -c '
set timeout 180
spawn mcc --local ssh 1.broker.uv-stat-prod-uv-kafka.ec.one-infra.ru
expect "/# "
send "cat > /tmp/run_reassign.sh << \"EOF\"\r"
...
send "/opt/kafka/bin/kafka-reassign-partitions.sh \\\r"
send "  --bootstrap-server \$cloud_hostname:9092 \\\r"
send "  --command-config /opt/kafka/config/client.properties \\\r"
send "  --reassignment-json-file /tmp/reassign.json \\\r"
send "  --execute --throttle 104857600 2>&1 | tail -50\r"
...
'
```

Throttle 100 МБ/с. Команда через expect+heredoc из-за вложенных кавычек (`$cloud_hostname`
нужно экранировать как `\$`, `<< "EOF"` запрещает локальную подстановку).

Успешный старт:
```
The inter-broker throttle limit was set to 104857600 B/s
Successfully started partition reassignments for uvThinStatPure-0,...,uvThinStatPure-971
```

### 5. Verify

Поскольку мы только **удаляли** реплики (без перекладки данных), reassign завершился
мгновенно — `--verify` сразу показал `is completed` для всех 91 партиций, throttle
снят автоматически на всех 175 брокерах и на топике:

```
Reassignment of partition uvThinStatPure-0 is completed.
...
Reassignment of partition uvThinStatPure-971 is completed.
Clearing broker-level throttles on brokers 24064,24065,...
Clearing topic-level throttles on topic uvThinStatPure
```

### 6. Финальная проверка

```bash
mcc --local sshexec -n infra 1.broker.uv-stat-prod-uv-kafka.ec.one-infra.ru \
  "unset KAFKA_OPTS JMX_PORT; /opt/kafka/bin/kafka-topics.sh \
   --bootstrap-server \$cloud_hostname:9092 \
   --command-config /opt/kafka/config/client.properties \
   --describe --topic uvThinStatPure 2>/dev/null \
   | grep -oE 'Replicas: [0-9,]+' | awk '{print \$2}' \
   | awk -F, '{print NF}' | sort | uniq -c"
```

```
   1024 3
```

Все 1024 партиции теперь RF=3.

## Нюансы

- **Снижение RF — это reassign, а не `kafka-configs.sh`.** `replication.factor` —
  static-конфигурация темы, динамически не меняется. Единственный путь — reassign
  с укороченным списком `replicas`. До этого правки в `SKILL.md` утверждали обратное.
- **Удаление реплик не требует перекладки данных** — verify завершается мгновенно,
  throttle фактически не нужен (но ставить всё равно стоит для безопасности).
- **`mcc sshexec` и большие команды** — лимит URL ~8KB, после чего `414 URI Too Long`.
  Для base64-загрузки >8KB использовать только `mcc ssh + expect` с чанками.
- **DC по broker-id** — префикс `broker_id // 1000` даёт номер DC (20=hc, 21=kc,
  22=pc, 23=uc, 24=ec). Удобно для проверки cross-DC redundancy в Python-генераторе.
- **`$cloud_hostname`** — переменная окружения на брокере, резолвится в FQDN
  bootstrap-сервера. В expect экранировать как `\$cloud_hostname`.

## Что обновлено в скилле

- `SKILL.md` → «Инструменты и нюансы выполнения»: добавлен явный callout читать
  `/mcc-host-access` первым, плюс готовый chunked-base64 паттерн загрузки JSON.
- `SKILL.md` → «Что покрывает скилл, но неочевидно»: добавлен раздел про снижение RF
  через reassign (с исправлением прежнего неверного утверждения про `kafka-configs.sh`).
