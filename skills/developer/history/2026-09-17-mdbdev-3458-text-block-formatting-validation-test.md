# 2026-09-17 — mdb-*: форматирование тестов с text block и try-with-resources (правки пользователя)

Контекст: тест UpsertKafkaUserRequestTest (MDBDEV-3458, DTO-валидация `Set<@NotNull ...>`).
Пользователь переформатировал мою версию.

## Правила (как надо)

1. **Text block как аргумент метода — не инлайнить в одну строку вызова.**
   Было (моя версия):
   ```java
   final var dto = mapper.readValue("""
       {"username": "robot-indexer-test", "permissions": [null]}
       """, UpsertKafkaUserRequest.class);
   ```
   Стало (правка пользователя):
   ```java
   final var dto = mapper.readValue(
       """
           {"username": "robot-indexer-test", "permissions": [null]}
           """, UpsertKafkaUserRequest.class
   );
   ```
   Открывающая скобка → перенос; text block с отступом глубже; второй аргумент —
   на строке закрывающих кавычек; закрывающая скобка — на отдельной строке.

2. **try-with-resources — `final var` в ресурсах.**
   `try (final var factory = Validation.buildDefaultValidatorFactory())` — не `try (var ...)`.

3. **Грабля из этого же теста: сообщения bean-validation локализованы.**
   Дефолтное сообщение `@NotNull` на JVM с ru-локалью — «не должно равняться null»,
   а не "must not be null" — ассерт на дефолтное сообщение падает в зависимости от
   локали машины. Стабильные варианты: ассертить `propertyPath` (и/или
   `getMessageTemplate()`), либо задавать своё сообщение в аннотации, как в
   ClusterParamsMapperTest (`@NotNull(message = "Coordinator host is required")`).

## Запись в propertyPath для null-элемента коллекции

`Set<@NotNull X> permissions` с `[null]` → violation:
- propertyPath: `permissions[].<iterable element>` (toString содержит «permissions»);
- message: локализованный дефолт `jakarta.validation.constraints.NotNull.message`.
