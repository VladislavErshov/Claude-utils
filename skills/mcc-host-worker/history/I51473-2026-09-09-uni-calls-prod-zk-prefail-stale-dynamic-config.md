# I51473 — uni-calls-prod-zk: PREFAIL на всех нодах из-за стейл zoo.cfg.dynamic + deadlock downscale-таски (2026-09-09)

## Кластер

- `uni-calls-prod-zk` (calls-prod, ns infra), ZK 3.8.6, queue
  `uni-calls-prod-zk.calls-prod.db.production.mdb.prod`.
- 9 живых хостов: dc = id1 (1.zk), rc = id4,5 (1-2.zk), pc = id7,8,9 (1-3.zk),
  uc = id10,11,12 (1-3.zk). myid сквозной, не совпадает с номером в fqdn.
- Целевой состав (чейндж от дежурных): dc ужат с 3 до 1 хоста
  (операция downscale-replicas `--cloud=dc --replicas=1` от 05.08), uc перевести
  в observer.

## Симптом

- В облаке ВСЕ 9 хостов PREFAIL, при этом ZK полностью жив: лидер id12
  (3.zk.uc), все BROADCAST, ~750 клиентских коннектов на ноду, latency ~5ms.
- mcc instances: `availability: PREFAIL`, `availability_details: id N ... BROADCAST`.

## Root cause

На нодах рендерился устаревший `/opt/zookeeper/conf/zoo.cfg.dynamic`:
**12 участников** (все participant), включая 3 несуществующих хоста
`2.zk...dc`, `3.zk...dc`, `3.zk...rc` (удалены даунскейлом 05.08, DNS не
резолвится). PMS к тому моменту уже содержал правильный состав (9 серверов),
но на ноды не применён — ноды берут PMS только при confp/reload/рестарте.

Чекер `rscheck@checkzookeeper` (класс `CheckZookeeper.get_rank`):
`voting_view` → для каждого участника GET `:8080/commands/isro` → исключение
(DNS `Name or service not known`) = unavailable. Порог:
`unavailable_hosts >= min(len(hosts)-3, 3)` → при 12 участниках достаточно
**3** мёртвых → `RANK_PREFAIL` на каждой ноде.

## Deadlock оператора

Таска `zookeeper.downscale-replicas` (висела с 05.08) застряла на фазе
`reloadConfigsAfterDownscale`: `Waiting all instances to be RUNNING RESERVED
before reloading next host`. PREFAIL не даёт RESERVED, RESERVED появляется
только после прокатки конфига → взаимное ожидание навсегда. `Executed: []` —
не прокатила ни одну ноду. Флагов терпимости у таски нет (help: только
`--cloud`, `--replicas`).

`zookeeper.reload-configs` в **текущей версии оператора НЕ знает**
`--maxUnavailableHostsCount` (в дежурной доке он описан — дока свежее
оператора): `Unrecognized option`. Осторожно: передача незнакомых аргументов
в op_start вида `-- "?"` и `-- -h` НЕ валидируется — таска реально
стартует/рестартует (`started`/`restarted`). Обе случайные инкарнации снял
`op_stop` (после op_stop бэкенд видит исчезновение таски и закрывает
операцию в mdb-data).

## Грабля при прокатке: финальный конфиг без текущего лидера = LOOKING

Первая попытка: отрендерил на 1.dc финальный конфиг (uc=observer, состав
участников {1,4,5,7,8,9}) и рестартнул → нода зависла в **LOOKING**:
в её новом взгляде лидер id12 не участник →
`Ignoring notification for non-cluster member sid 12`, вступить в ансамбль
не может. Ансамбль при этом держал кворум старого взгляда (12 + фолловеры
4,5,7,8,9,10,11 = 8 ≥ 7), записи не встали, но узел не обслуживал клиентов.

**Вывод:** статический (не-reconfig) переход нельзя делать конфигом, в
котором нет текущего лидера среди участников. Нужен промежуточный состав,
признаваемый обеими сторонами.

## Решение (двухфазная прокатка)

1. **PMS → промежуточный конфиг**: 9 живых хостов, ВСЕ participant
   (uc остался participant; мёртвых хостов нет). Запись через
   `POST /api/conf/update.do` (ns infra, app mdb, host
   `uni-calls-prod-zk.clouds`, property `zookeeper.zoo.cfg.dynamic`).
2. Прокат по одной ноде: `confp --oneshot` → проверить
   `zoo.cfg.dynamic` (9 строк, 0 observer, server.12 на месте) →
   `systemctl restart zookeeper.service` → ждать
   `zabstate=broadcast` (+ srvr role), следующая нода. Порядок:
   1.dc(id1) → 4.rc → 5.rc → 7.pc → 8.pc → 9.pc → 10.uc → 11.uc →
   **лидер 12.uc последней** (короткий перевыбор, 8 из 9 живы, quorum 5 —
   штатно). Каждый рестарт ~15-30с до broadcast.
3. Результат: все 9 хостов RESERVED, voting_view = 9 живых, PREFAIL ушёл.
   Новый лидер после перевыбора — **id11 (2.zk.uc)**.

## Итоговое состояние и хвосты

- PMS `zookeeper.zoo.cfg.dynamic` = промежуточный (9 participant).
  **Ловушка peerType**: per-host `zookeeper.peerType` для uc = observer —
  любая операция, регенерирующая dynamic-конфиг (make-observer и любые
  resize/reload-флоу), отрендерит uc как observer и при прокатке уронит
  ноды в LOOKING (лидер id11 — на uc). До целевого перевода uc в observer
  ключи либо согласовать, либо делать перевод осознанно.
- **Перевод uc в observer — отдельная операция**: перенести лидера с uc
  (рестарт лидера / выборы), затем rolling uc→observer. Не делать «в лоб».
- `http`-чек в `checkzookeeper.conf` ходит на `http://127.0.0.1:8080/ruok`
  → вечный 404 (AdminServer 3.8 отдаёт только `/commands/*`; 4lw ruok не в
  whitelist). На availability не влияет (reporter рапортует только
  zk-availability), но шумит в journalctl. Чинить отдельно (простая смена
  URL не поможет: CheckURL ждёт тело `Ok.`, а `/commands/ruok` возвращает JSON).
- Дедлок downscale-replicas (wait RESERVED ↔ PREFAIL) — кандидат на репорт
  платформе; см. также похожий кейс plait-prefail-block в
  scale-kafka-hosts-tester/history/2026-09-03.
- Проверить, что августовская операция downscale в mdb-data закрылась после
  op_stop (бэкенд ждёт исчезновения таски → должен закрыть как DONE).
- Дежурную доку Zookeeper дополнить: у reload-configs в прод-версии
  оператора нет `--maxUnavailableHostsCount`; незнакомые аргументы op_start
  не валидируются; переход состава с удалением лидера требует
  промежуточного конфига.
