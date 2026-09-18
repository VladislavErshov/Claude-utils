# 2026-09-17 — mdb-data: подавление checkstyle LineLength для одной строки

## Правило (как надо)

Длинную строку НЕ переносить — подавлять чекер **комментарием на строку выше**:

```java
// CHECKSTYLE.SUPPRESS: LineLength for +1 lines
@Schema(description = "...") String resourceName,
```

## Почему именно так (грабли)

- В `config/checkstyle.xml` `LineLength` (max 120) объявлен **на уровне Checker, вне TreeWalker** —
  TreeWalker-фильтры его не видят.
- `// CHECKSTYLE.SUPPRESS: LineLength` (короткая форма) **не работает** — чекер не глушится:
  Checker-уровневый `SuppressWithNearbyTextFilter` матчит формат
  `CHECKSTYLE.SUPPRESS\: (\w+) for ([+-]\d+) lines` — хвост **« for +N lines» обязателен**.
- Рабочий фильтр — `SuppressWithNearbyTextFilter` (Checker): `checkPattern=$1`, `lineRange=$2`
  (относительное смещение строк, `+1` = только следующая строка).
- `@SuppressWarnings("checkstyle:linelength")` на классе тоже работает (RecognitionException:
  AlertGeneratorImpl), но это подавление на весь класс — для одной строки НЕ использовать.
- Внутри TreeWalker правила (стиль кода и т.п.) глушатся иначе: `SuppressionCommentFilter`
  с `// CHECKSTYLE.OFF: <Правило>` … `// CHECKSTYLE.ON: <Правило>`.

## Контекст

Кейс MDBDEV-3458, MR mdb-data !505: @Schema-описание resourceName для ACL CLUSTER
в KafkaUserPermissionDto — пользователь сам выставил финальный вид (аннотации над полями,
длинная строка без переноса + nearby-комментарий).
