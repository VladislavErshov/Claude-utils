# MDBSUP-4594 — Cruise Control не стартует: `getpwnam(): name not found: 'kafka'` после смены `serviceName` в `one_cloud_meta`

Дата: 2026-08-12
Кластер: `kafka-stats-conv-adtech-kafka` (KRaft, pc)
Хост: `1.cruise.kafka-stats-conv-adtech-kafka.pc.one-infra.ru`

## Симптомы

В onecloud сервис `cruise.kafka-stats-conv-adtech-kafka` циклически падает:
DEPLOYING → STARTING → FAILURE (Exit code = 1), 6 попыток, потом UNAVAILABLE.

```
mcc status cruise.kafka-stats-conv-adtech-kafka:
  state: UPDATING/STARTING
  instances:
    - 1.cruise...: FAILURE (Container 'main' is dead: Exit code = 1)
  image: dzen-external-registry.odkl.ru/ubuntu20-mdb-cruisecontrol-2.5.147:1.0.2
```

На хосте:
- `systemctl status cruise-control` → `inactive (dead)`.
- `/mnt/logs/dbms/cruise-control.{out,err}.log` — **не существуют**.
- `/etc/sysconfig/cruise-control` (EnvironmentFile для systemd unit) — **не существует**.
- `journalctl -u cruise-control` — «No journal files were found».
- Java в образе: OpenJDK 17.0.15 → версия Java НЕ причина (не `UnsupportedClassVersionError`).

## Корень проблемы

`confp --oneshot` обрывается на `vault-pki` и не доходит до рендера основных конфигов круиза:

```
confp.confp  INFO   Running restart command '/opt/pyvault-python/bin/vault-pki && systemctl start vault-pki.timer'
root         ERROR  Failed to run '... vault-pki && ...':
  vault_pki.py:286  write_file(os.path.join(args.dir, ca_name), vault_ca, owner=args.user, group=args.group, mode=args.mode)
  utils.py:204      uid = pwd.getpwnam(owner).pw_uid
  KeyError: "getpwnam(): name not found: 'kafka'"
confp.confp  ERROR  Failed to call restart_cmd '... vault-pki && ...'
confp.backends.pms  INFO  Close connection with conf service
=== EXIT: 0 ===
```

`vault-pki` (запускаемый без аргументов) читает параметры из pms app=`ok-pyvault` по ключу-хосту.
В этом конфиге лежат **брокерские** значения:

```yaml
dir: /opt/kafka/ssl        # нет в образе круиза
user: kafka                # нет в образе круиза — есть только cruisecontrol (uid 998) и www-data (uid 33)
group: kafka
reload-cmd: systemctl reload-or-try-restart kafka-controller ; systemctl reload-or-try-restart kafka-broker
```

В образе `ubuntu20-mdb-cruisecontrol-2.5.147:1.0.2` пользователя `kafka` нет → `KeyError` →
скрипт падает → confp прерывает рендер → `/etc/sysconfig/cruise-control` и
`/opt/cruise-control/config/*.properties` не создаются → systemd не может поднять
`cruise-control.service` → контейнер Exit code = 1.

⚠️ `confp` выходит с кодом 0, несмотря на `ERROR` в логе → systemd/onecloud считает, что confp
«успешно» отработал, и цикл рестартов не размыкается автоматикой.

## Почему сломалось именно сейчас

В `one_cloud_meta.params` сервиса поменяли поле `serviceName`:

```diff
- "serviceName": "cruise-control"
+ "serviceName": "cruise"
```

`GenerateCruiseControlCertificatesTaskProcessor.getPmsHostName()` = `${serviceName}.${queue}.clouds`
(`plugins/mdb-backend/src/task/manifest/GenerateCruiseControlCertificatesTaskProcessor.ts:14-16`).
Смена `serviceName` сменила pms-ключ:

- было: `cruise-control.kafka-stats-conv-adtech-kafka.clouds` (app=ok-pyvault)
- стало: `cruise.kafka-stats-conv-adtech-kafka.clouds` (app=ok-pyvault)

На старом ключе лежал рабочий конфиг (видимо, ранее поправили руками — поэтому «в прошлый раз
обошлись без правки шаблона»). На новом ключе backstage перегенерил конфиг из шаблона
`plugins/mdb-backend/src/task/manifest/templates/kafka-cruise-control-pyvault-conf`, а там —
брокерские значения (`user: kafka`, `dir: /opt/kafka/ssl`). Этот шаблон — копия брокерского
`kafka-pyvault-conf`, не адаптированная под образ круиза.

Ветвление в `TemplateBuilder.ts:1329-1349` (`buildPyvaultConfKafka`) есть — для
`OneCloudMetaParamsType.CRUISE_CONTROL_SERVICE` грузится `kafka-cruise-control-pyvault-conf`,
иначе `kafka-pyvault-conf`. Но содержимое cruise-шаблона идентично брокерскому → баг.

## Фикс

⚠️ **Важное уточнение про pms-приложение**: `vault-pki` читает своё свойство `vault-pki.certs`
из pms-приложения **`ok-pyvault`**, а НЕ из `mdb`. Это не путать — `confp.yml` на хосте
настроен на `app_name: mdb` (для рендера `kafka.cruisecontrol.*` шаблонов), но vault-pki —
отдельный скрипт, который ходит в `ok-pyvault` напрямую (`APP_NAME_PMS = "ok-pyvault"` в
`ok_pyvault/defaults.py:17`, имя свойства `PMS_PKI_PROPERTY = "vault-pki.certs"` в
`defaults.py:20`). Правки `kafka.cruisecontrol.*` в app=`mdb` эту проблему **не лечат** —
нужно именно свойство `vault-pki.certs` в app=`ok-pyvault`.

Web UI pms для правки:
`https://pms.cloud.vk.team/client/#/props-search?ns=infra&a=ok-pyvault&h=cruise.<queue>.clouds`

Чтение через скилл `kafka-config-inspector` (передаётся `application=ok-pyvault`):
```bash
~/.claude/skills/kafka-config-inspector/bin/pms-read.sh cruise.<queue>.clouds vault-pki.certs infra ok-pyvault
```

### Вариант 1 (быстрый, ручной) — создать pms-свойство на новом ключе

Создать в pms (app=`ok-pyvault`, host `cruise.kafka-stats-conv-adtech-kafka.clouds`) свойство
`vault-pki.certs` со значением, скопированным со старого рабочего ключа
`cruise-control.kafka-stats-conv-adtech-kafka.clouds`:

```yaml
pki-role: hostname.one-infra.ru
dir: /etc/security/ssl            # НЕ /opt/kafka/ssl (брокерский путь, в круиз-образе его нет)
cert-name: tls.crt
key-name: tls.key
ca-name: tls_ca.crt
ttl: 4320h
user: www-data                    # НЕ kafka (в круиз-образе есть только cruisecontrol и www-data)
group: www-data
mode: 400
reload-cmd: systemctl reload-or-try-restart nginx
alt-names: '{{ env(cloud_hostname) }}'
```

После правки — дождаться деплоя или `mcc restart`, `confp --oneshot` проходит чисто,
`cruise-control.service` поднимается, логи идут в `/mnt/logs/dbms/cruise-control.{out,err}.log`.

⚠️ **Если просто отредактировать существующий брокерский ключ `kafka-stats-conv-adtech-kafka.clouds`
в app=`mdb` — не поможет.** Проблема в app=`ok-pyvault`, свойстве `vault-pki.certs`.

### Вариант 2 (откат) — вернуть `serviceName: cruise-control` в `one_cloud_meta`

Тогда pms-ключ снова станет `cruise-control.<queue>.clouds` и возьмёт старый рабочий конфиг
(если он не перетёрт). Подходит, если переименование не было обязательным.

### Вариант 3 (долгий, правильный) — поправить шаблон в backstage

`plugins/mdb-backend/src/task/manifest/templates/kafka-cruise-control-pyvault-conf`: заменить
`user: kafka` → `cruisecontrol`, `dir: /opt/kafka/ssl` → `/etc/security/ssl`, `reload-cmd`
убрать broker/controller. Покроет будущие круизы, но требует деплоя нового backstage и
перегенерации pms-конфига.

## Что проверено на хосте

```bash
mcc sshexec -n infra 1.cruise.kafka-stats-conv-adtech-kafka.pc.one-infra.ru \
  "confp --oneshot 2>&1 | tail -30"            # ← видно KeyError и EXIT 0
mcc sshexec -n infra 1.cruise... \
  "cat /etc/systemd/system/cruise-control.service | grep -E 'Environment|ExecStart'"
# ExecStart=/opt/cruise-control/kafka-cruise-control-start.sh /opt/cruise-control/config/cruisecontrol.properties
# EnvironmentFile=/etc/sysconfig/cruise-control
mcc sshexec -n infra 1.cruise... \
  "grep -E 'www-data|cruisecontrol|kafka' /etc/passwd"
# www-data:x:33:33:...
# cruisecontrol:x:998:998:Cruise Control service
# (kafka — нет)
mcc sshexec -n infra 1.cruise... \
  "/opt/pyvault-python/bin/vault-pki --verbose -r test 2>&1 | head -20"
# Без явных --user/--dir работает, пишет в /one/conf/ssl/ — значит pms-конфиг подсовывает битые значения.
```

## Грабли

- **vault-pki читает pms app=`ok-pyvault`, а НЕ `mdb`.** Свойство называется `vault-pki.certs`
  (`PMS_PKI_PROPERTY` в `ok_pyvault/defaults.py:20`). `confp.yml` на хосте настроен на
  `app_name: mdb` — это для рендера `kafka.cruisecontrol.*` шаблонов, к vault-pki не имеет
  отношения. Если править `kafka.*` в app=`mdb` — `getpwnam('kafka')` не уходит. Лечить только
  свойство `vault-pki.certs` в app=`ok-pyvault`.
- **PMS делает fallback на родительский хост.** Если для `cruise.<queue>.clouds` свойство
  `vault-pki.certs` не задано — pms отдаёт значение с `kafka-stats-conv-adtech-kafka.clouds`
  (брокерского, без префикса cruise.). Брокерский конфиг содержит `user: kafka, dir: /opt/kafka/ssl`
  → в круиз-образе это падает. То есть «свойство не задано» ведёт себя не как пусто, а как
  «возьми с брокерского ключа» — молча и незаметно.
  при диагностике — надо читать лог `confp --oneshot` и смотреть `ERROR`/`Traceback`. Из-за
  этого onecloud не размыкает цикл рестартов автоматически.
- **Смена `serviceName` в `one_cloud_meta` молча мигрирует pms-ключ.** Старый конфиг
  остаётся в pms, но уже не используется; новый подтягивается из шаблона в backstage. Если
  шаблон битый — сервис падает. Перед сменой `serviceName` проверять, что в pms на новом
  ключе лежит корректный конфиг, или что шаблон в backstage генерит правильные значения.
- **Шаблон `kafka-cruise-control-pyvault-conf` в backstage — копия брокерского.** Любой
  новый круиз, разворачиваемый с нуля, будет наступать на эти же грабли. Нужен фикс
  шаблона (`user: cruisecontrol`, правильный `dir`, `reload-cmd` под `cruise-control.service`).
- **«В прошлый раз мы без этого правили»** — потому что тогда правили pms-конфиг на
  старом ключе `cruise-control.<queue>.clouds` руками. После смены `serviceName` этот
  конфиг больше не используется.
- **Java 17 в образе круиза** — значит, симптом НЕ относится к семейству
  `UnsupportedClassVersionError` (известная проблема CC vs Java 11). Проверять версию Java
  в первую очередь, чтобы не идти по ложному следу.
- **`/mnt/logs/dbms/` пустая и `journalctl` пусто** — маркер того, что systemd даже не
  пытался запустить ExecStart. Значит, проблема на уровне конфигурации (EnvironmentFile,
  конфп), а не на уровне самого cruise-control.
