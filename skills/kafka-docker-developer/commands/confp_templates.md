# Правка confp jinja2-шаблонов (TODO)

⚠️ **Заглушка.** Заполнится по мере использования скилла.

## Scope

Правка файлов в `rootfs/etc/confp/`:

- `templates.d/*.j2` — jinja2-шаблоны: `client.properties.j2`, `jaas.conf.j2`,
  `create_keystore.sh.j2`, `pre-start-kafka-*.sh.j2`, `vector-default.toml.j2`, `.pass.j2`,
  `.ssl_enabled.j2`.
- `resources.d/kafka.yml` — confp resource: описывает куда рендерить (путь, права, source,
  перезапуск сервиса после рендера).

Шаблоны рендерятся на хосте `confp` сервисом из PMS-переменных → попадают в
`/opt/kafka/config/`, `/etc/sysconfig/kafka`, `/opt/cruise-control/config/`.

## Цикл

В отличие от чекеров, hot-reload неприменим — после правки j2 нужен рендер через PMS modify-флоу
(mdb-processing). Возможно через локальный confp на хосте — TBD.

## Что задокументентировать когда понадобится

- Какой путь рендера у каждого шаблона (сверка с `resources.d/kafka.yml`).
- Чем тестировать локально (confp CLI?).
- Связка с `/kafka-config-inspector` для сверки PMS-API ↔ отрендеренный файл.
