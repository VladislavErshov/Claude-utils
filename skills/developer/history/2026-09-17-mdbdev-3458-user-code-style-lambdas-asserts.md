# 2026-09-17 — Стиль кода: правки пользователя в MDBDEV-3458 (mdb-data, валидация ACL)

Контекст: реализовал валидацию `validateUserPermissions` (KafkaValidationService/Impl + тесты).
Пользователь поправил мою версию — паттерны правок обязательны для будущих правок в mdb-*:

## Правила (как надо писать)

1. **Лямбда-параметры — полными словами, не одной буквой.**
   Не `p -> ...`, а `permission -> ...`; не `e -> ...`, а `error -> ...`.

2. **Сложная лямбда — блок с локальными `final var`, без inline-цепочек выражений внутри вызова.**
   Было (моя версия):
   ```java
   .forEach(p -> result.addError(PERMISSIONS,
       "Для ресурса типа CLUSTER допустимо только имя ресурса kafka-cluster (получено: %s)"
           .formatted(p.getResourceName())));
   ```
   Стало (правка пользователя):
   ```java
   .forEach(permission -> {
       final var resourceName = permission.getResourceName();
       final var message =
           "Для ресурса CLUSTER допустимо только имя kafka-cluster (получено: %s)".formatted(resourceName);
       result.addError(PERMISSIONS, message);
   });
   ```
   Значение для сообщения и само сообщение — в отдельных локальных переменных;
   вызов метода внутри лямбды получает готовую переменную, а не цепочку вызовов.

3. **Сложные предикаты в ассертах — именованная локальная переменная, не инлайн-лямбда.**
   ```java
   final Predicate<ValidationError> predicate =
       error -> error.field().equals("permissions") && error.message().contains("kafka-cluster");
   assertThat(result.errors()).anyMatch(predicate);
   ```

4. **Javadoc публичных методов — только контракт, без ссылок на тикеты и без «почему/истории».**
   Убрал `(MDBDEV-3458)` и фразу про то, что Kafka отвергает батч: осталась одна фраза —
   что валидируется и какое ограничение. Контекст инцидента живёт в Jira/history, не в Javadoc.

5. Сообщения об ошибках валидации — короче без потери смысла:
   «Для ресурса CLUSTER допустимо только имя kafka-cluster (получено: %s)»
   вместо «Для ресурса типа CLUSTER допустимо только имя ресурса kafka-cluster (получено: %s)».

## Состояние задачи MDBDEV-3458

- Правки пользователя в working tree (не терять!): interface + impl + тесты.
- Компиляция `compileJava compileTestJava` — зелёная.
- Тест-класс `KafkaValidationServiceTest` пока не прогнан: локальный Docker сломан
  (Testcontainers не поднимается), пользователь чинит сам — Docker не трогать, тесты
  прогнать, когда он скажет, что Docker готов.
