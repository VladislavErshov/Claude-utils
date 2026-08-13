# Cruise не поднимается после включения vault-pki в pms — две независимые проблемы

Дата: 2026-08-13
Кластер: `kafka-antifraud-adtech-kafka` (KRaft, hc)
Хост: `1.cruise.kafka-antifraud-adtech-kafka.hc.one-infra.ru`

## Симптомы

После того, как в pms для cruise-хоста включили `vault-pki` (добавили/обновили свойство
`vault-pki.certs` в app=`ok-pyvault`), хост всё равно не поднимается:
- `cruise-control.service` — `inactive (dead)`
- `/etc/security/ssl/` — отсутствует
- `vault-login.service` / `vault-pki.service` — `inactive (dead)`
- На хосте active всего 3 systemd-юнита (`systemd-remount-fs`, `systemd-tmpfiles-setup`,
  и после ручного старта — `cruise-control`), а на эталоне 12.

Разбор распадается на **две независимые проблемы**:

1. **vault-pki падает на `permission denied` при issue сертификата** — pms-конфиг содержал
   опечатку в `pki-role`.
2. **systemd застрял на `sysinit.target`** — после старта контейнера не произошёл переход к
   `multi-user.target`, поэтому nginx/rscheck/vector/confp-init/dbus/journald/rsyslog не
   стартовали.

## Проблема 1: `permission denied` на `pki/issue/hostname.one-infra`

### Диагностика

`confp --oneshot` обрывается на `vault-pki`:
```
INFO: Generate certificate for 1.cruise.kafka-antifraud-adtech-kafka.hc.one-infra.ru based on hostname.one-infra role
ERROR: "You don't have permission to request certs from role 'hostname.one-infra': 1 error occurred:
* permission denied
, on post https://hc.vault.infra.one-infra.ru/v1/pki/issue/hostname.one-infra"
```

CA (`tls_ca.crt`) при этом успешно кладётся в `/etc/security/ssl/`, но `tls.crt`/`tls.key`
не выдаются.

### Проверка токена и capabilities

Токен в `/root/.vault-token` получен (JWT-роль `default`, TTL 24h, policies включают
`infracloud_cruise.kafka-antifraud-adtech-kafka.hc` и т.п.). Проверка через
`sys/capabilities-self`:

```bash
TOK=$(cat /root/.vault-token)
curl -sS -X POST -H "X-Vault-Token: $TOK" -H 'Content-Type: application/json' \
  -d '{"paths":["pki/issue/hostname.one-infra.ru","pki/issue/hostname.one-infra","pki/issue/hostname"]}' \
  https://hc.vault.infra.one-infra.ru/v1/sys/capabilities-self
```

Результат:
```json
{
  "pki/issue/hostname": ["update"],
  "pki/issue/hostname.one-infra.ru": ["update"],
  "pki/issue/hostname.one-infra": ["deny"]
}
```

У токена **есть** права на `hostname.one-infra.ru` (с `.ru`) и на `hostname`, но **нет** на
`hostname.one-infra` (без `.ru`).

### Корень

В pms (app=`ok-pyvault`, host=`cruise.kafka-antifraud-adtech-kafka.clouds`, свойство
`vault-pki.certs`) лежит:
```yaml
pki-role: hostname.one-infra      # ← БАГ: нет .ru
```

Чтение через скилл `kafka-config-inspector`:
```bash
~/.claude/skills/kafka-config-inspector/bin/pms-read.sh \
  cruise.kafka-antifraud-adtech-kafka.clouds vault-pki.certs infra ok-pyvault
```

Эталонное значение (из MDBSUP-4594, рабочий cruise-хост):
```yaml
pki-role: hostname.one-infra.ru   # ← с .ru
dir: /etc/security/ssl
cert-name: tls.crt
key-name: tls.key
ca-name: tls_ca.crt
ttl: 4320h
user: www-data
group: www-data
mode: 400
reload-cmd: systemctl reload-or-try-restart nginx
alt-names: '{{ env(cloud_hostname) }}'
```

### Фикс

В pms (web UI: `https://pms.cloud.vk.team/client/#/props-search?ns=infra&a=ok-pyvault&h=cruise.kafka-antifraud-adtech-kafka.clouds`)
поправить `pki-role`:
```diff
- pki-role: hostname.one-infra
+ pki-role: hostname.one-infra.ru
```

После правки на хосте:
```bash
confp --oneshot
# vault-pki выпускает серты, /etc/security/ssl/tls.crt появляется
```

## Проблема 2: systemd застрял на `sysinit.target`

### Диагностика

После фикса `pki-role` и `confp --oneshot` серты выдаются, `cruise-control.service`
стартует вручную, но в `systemctl list-units --type=service` active всего 3 юнита вместо
эталонных 12. Отсутствуют `nginx`, `rscheck@cruisecontrol`, `vector`, `confp-init`,
`dbus`, `journald`, `rsyslog`, `network-wait-online`, `import-environment`.

Маркеры:
```bash
ps -p 1 -o args
# /usr/lib/systemd/systemd --unit=sysinit.target          ← застрял на sysinit

systemctl is-active multi-user.target
# inactive                                                 ← multi-user не активирован

systemctl get-default
# graphical.target                                         ← не причина, норма для контейнера
```

PID 1 запущен с `--unit=sysinit.target` — systemd поднял только sysinit-фазу и не перешёл
к `basic.target` → `multi-user.target`. Поэтому все сервисы, привязанные к multi-user
(nginx, rscheck, vector, confp-init, dbus, journald, rsyslog), не стартовали.

### Корень: `cmd` в манифесте onecloud

Параметр `--unit=sysinit.target` передавался не onecloud-minion и не из docker-образа —
он был **явно прописан в манифесте сервиса** в onecloud:

```yaml
cmd:
  - /usr/lib/systemd/systemd
  - '--unit=sysinit.target'
```

Проверка: в docker-образах `ubuntu20-mdb-cruisecontrol-*` → `ubuntu20-mdb-base` →
`ubuntu20-base` нет ни `entrypoint.sh`, ни упоминаний `sysinit.target` / `--unit=` / `/sbin/init`
(grep по `docker-images/` пуст). Параметр передаёт onecloud-minion из `cmd` манифеста.

Когда systemd стартует с `--unit=sysinit.target`:
1. Достигает `sysinit.target` (`systemd-remount-fs`, `tmpfiles-setup`).
2. **Останавливается** — `--unit=` переопределяет default target, systemd **не идёт** к
   `basic.target` → `multi-user.target`.
3. Все сервисы в `multi-user.target.wants` (`cruise-control`, `nginx`, `rscheck@cruisecontrol`,
   `vector`, `host-check`) и их зависимости (`confp-init`, `dbus`, `journald`, `rsyslog`,
   `network-wait-online`, `import-environment`) — не стартуют.

Без `cmd` в манифесте onecloud-minion запускает `/bin/systemd` без параметра → systemd
стартует с default target (`graphical.target` → редуцируется к `multi-user.target` через
зависимости) → все 12 сервисов поднимаются сами (эталон: `1.cruise.test-43version-4-mdbdev-kafka.hc.one-infra.ru`).

### Фикс

**Убрать `cmd` из манифеста onecloud** — это и есть корневой фикс. После применения
манифеста контейнер пересоздаётся (container ID в `/proc/1/cgroup` меняется), PID 1 =
`/bin/systemd`, все 13 сервисов active (12 эталонных + `host-check`).

Временный workaround (если манифест нельзя поменять сразу):
```bash
systemctl start multi-user.target
```

После этого подтягиваются все эталонные сервисы:
```
confp-init.service             active exited
cruise-control.service         active running
dbus.service                   active running
host-check.service             active running
import-environment.service     active exited
network-wait-online.service    active exited
nginx.service                  active running
rscheck@cruisecontrol.service  active running
rsyslog.service                active running
systemd-journald.service       active running
systemd-remount-fs.service     active exited
systemd-tmpfiles-setup.service active exited
vector.service                 active running
```

Без `rscheck@cruisecontrol.service` mdb-data считает хост dead, даже если
`cruise-control.service` running.

## Полная последовательность фикса

1. Поправить в pms app=`ok-pyvault` свойство `vault-pki.certs` для хоста
   `cruise.<queue>.clouds`: `pki-role: hostname.one-infra.ru` (с `.ru`).
2. **Убрать `cmd: [/usr/lib/systemd/systemd, --unit=sysinit.target]` из манифеста
   onecloud** (если прописано) — без этого после рестарта контейнер застрянет на
   `sysinit.target` и не поднимет `multi-user.target`.
3. После применения манифеста (контейнер пересоздастся) на хосте:
   ```bash
   confp --oneshot                          # отрендерить конфиги + выпустить серты
   systemctl start cruise-control           # поднять CC (если не стартовал сам)
   systemctl enable --now vault-pki.timer vault-login.timer   # ротация сертов и токена
   ```
4. Если `cmd` убрать нельзя — временный workaround после каждого рестарта:
   ```bash
   systemctl start multi-user.target
   ```
3. Подождать ~30 минут — CC копит снапшоты, после `isProposalReady=true`.

## Грабли

- **`pki-role` без `.ru` — неочевидная опечатка.** Vault-policy выдаёт права на
  `pki/issue/hostname.one-infra.ru` и `pki/issue/hostname`, но не на
  `pki/issue/hostname.one-infra`. При чтении pms-свойства легко не заметить отсутствующий
  `.ru`. Маркер — `permission denied` на issue, но CA при этом выдаётся (CA берётся без
  привязки к роли, issue — с ролью).
- **Запуск `vault-login.timer` не чинит проблему 1.** Токен уже валиден (получен через
  confp-init / vault-pki при первом запуске), проблема не в токене, а в правах на роль.
  Проверять через `sys/capabilities-self`.
- **`systemctl start cruise-control` поднимает только CC, не всю инфру.** Без
  `multi-user.target` не поднимаются nginx, rscheck, vector, confp-init — mdb-data всё
  равно считает хост dead. После любого ручного старта CC проверять полный список
  сервисов.
- **`systemctl get-default = graphical.target` — не причина.** Это дефолт для контейнера,
  не влияет на запуск `multi-user.target` через `systemctl start`.
- **`/etc/sysconfig/vault-login` и `/etc/sysconfig/vault-pki` отсутствуют** — это норма,
  они не обязательны (EnvironmentFile=- с `-` = optional). vault-pki читает параметры из
  pms напрямую (`APP_NAME_PMS = "ok-pyvault"`).

## Связанные

- `MDBSUP-4594.md` — похожий кейс, но там баг был в `user: kafka` / `dir: /opt/kafka/ssl`
  (брокерский шаблон), а не в `pki-role`. Эталон `pki-role: hostname.one-infra.ru` оттуда же.
- `feedback_cruise_vault_pki_role.md` (auto-memory) — краткое правило про `.ru` в `pki-role`.
- `kafka-host-inspector/SKILL.md` — эталонный список 12 сервисов для cruise-хоста.
