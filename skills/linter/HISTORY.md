# История правок пользователя linter

Журнал эталонных правок пользователя по качеству кода. Каждая правка переносится
в SKILL.md как правило.

---

## 2026-09-14 — AssertJ isSameAs с dissimilar types (mdb-data, MR MDBDEV-3301)

**Мой код:** `assertThat(patched.kafkaTopicSettings().config()).isSameAs(newConfig)` —
IDE подсветила «assertion arguments have dissimilar types», пользователь попросил
поменять аргументы ассерта.

**Правка:** `isSameAs` → `isEqualTo`. Для record'ов (KafkaTopicConfigDto) value-equality
корректна и без разыменовывания nullable-полей; isSameAs здесь был и типологически
неверен (сравнение ссылок вместо значений), и маскировал несоответствие типов.

**Урок (перенесён в SKILL.md → «Тестовые ассерты»):** isSameAs — только для ссылочной
идентичности однотипных объектов; значения сравнивать isEqualTo; nullable-поля не
разыменовывать в цепочке ассерта.
