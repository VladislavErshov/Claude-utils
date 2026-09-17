# Профайлер из облака: `mcc perf` / `mcc profile`

CPU-профилирование контейнера силами облака (движок `perf` на миньоне, тот же, что
в UI cloud.vk.team → instance → Profile). Проверено на MDB Kafka 4.3 (2026-09-15, MDBSUP-3644).

## Две команды

```bash
# РАБОЧИЙ ПУТЬ — perf-движок (как engine=perf в UI)
mcc -n infra perf -d 30 -e cpu-clock <fqdn> > /tmp/perf.out 2>&1

# Обычно НЕ работает в контейнерах MDB: flat-вывод идёт через async-profiler,
# а `jattach` в контейнере отсутствует → "bash: jattach: command not found"
mcc -n infra profile --container main -e cpu -d 30 --threads -o flat <fqdn>
```

Опции `perf`: `-d` секунды (default 30), `-e cpu-clock` (событие, требует MINION-прав),
`-i` интервал (нс, default 50ms; в UI используют 10000000 = 10ms), `-G` cgroup,
`--output_format` — формат обёртки ответа (json/yaml), сам профиль всегда `f(...)`-строки.

Соответствие параметрам UI-ссылки (`/api/clouds/<DC>/instance/profile?engine=perf&duration=30
&event=cpu-clock&interval=10000000&start_options=--call-graph%20dwarf&output=flamegraph`):
`mcc -n infra perf -d 30 -e cpu-clock <fqdn>` — эквивалентно.

## Формат вывода

1. Строки прогресса `Profiling for N sec` — отбросить (всё до первого `f(`).
2. Далее — данные Pangin-flamegraph, по строке на фрейм:
   `f(level, left, width, type, 'title')`
   - `width` ∝ числу сэмплов (CPU-время); level 0 — корень `all`;
     **level 1 — процессы/треды** (главная группировка);
   - треды JVM обрезаны до 15 символов: `prometheus-http`, `data-plane-kafk`,
     `C2_CompilerThre`, `kafka-admin-cli` (= AdminClient-тред `kafka-admin-client-thread`);
   - имя процесса = comm главного треда: `java`, `python3`, `kafka_exporter`, `vector`.

## Парсинг (python)

```python
import re
data = open('perf.out').read()[open('perf.out').read().find('f('):]
frames = [(int(m[1]), int(m[3]), m[4]) for m in
          re.finditer(r"f\((\d+),(\d+),(\d+),\d+,'((?:[^'\\]|\\.)*)'\)", data)]
total = max(w for l, _, w, _ in frames if l == 0)          # сэмплов всего
cores = total * 0.01 / 30                                   # при interval 10ms: сэмплов/ядро/сек = 100
threads = sorted([f for f in frames if f[0] == 1], key=lambda x: -x[2])
for l, w, t in threads: print(f'{w/total*100:5.1f}%  {t}')
```

⚠️ Агрегация `Counter` по title **по всем уровням** двоит счёт (универсальные фреймы
`[unknown]`, `entry_SYSCALL_64` встречаются во многих ветках) — для процентов смотреть
level-1 и листья, не сумму по всем уровням.

## Грабли

- `[unknown]` / `[perf-NNN.map]` — JIT-код без карты символов: до ~30–70% сэмплов.
  Атрибуция по ТРЕДАМ (level 1) точна, внутренности java-стеков — приблизительны.
- `jstat`/`jstack` из контейнера могут не работать (нет jattach), но `jstat -compiler`
  читает hsperfdata напрямую и работает — валидация «JIT спит или жжёт».
- perf видит ВЕСЬ контейнер: короткоживущие процессы (спавны CLI-скриптов) попадают
  в профиль с их реальной ценой — часто это и есть разгадка «непонятной нагрузки».
- Профиль — окно 30с: редкие события (спавн раз в 60с) ловить несколькими прогонами
  или удлиннять `-d`.
