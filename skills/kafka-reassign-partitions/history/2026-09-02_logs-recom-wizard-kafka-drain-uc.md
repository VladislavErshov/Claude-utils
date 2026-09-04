# logs-recom-wizard-kafka: вывод всех партиций с uc (unclean election + reassign)

**Кластер:** `logs-recom-wizard-kafka` (dzen, idzn.ru, ДЦ hc/kc/pc/uc; 46 брокеров: hc=20001-20012, kc=21001-21012, pc=22001-22012, uc=23001-23010; контроллеры 11001@kc / 12001@pc / 13001@uc, порт 9093)
**Дата:** 2026-09-02
**Инцидент-предок:** [`kafka-cluster-inspector/history/2026-08-31_logs-recom-wizard-kafka-urp-uc-new-conn-broken.md`](../../kafka-cluster-inspector/history/2026-08-31_logs-recom-wizard-kafka-urp-uc-new-conn-broken.md)
(URP=9, все led by 23001, ISR=23001; сеть uc мертва для новых входящих TCP на брокеров)

## Итог

- URP: 0, unavailable: 0, **партиций с репликами на uc: 0 из 360**, RF=3 везде (единственная RF=4
  партиция подрезана). uc-ДЦ больше не держит данных; 23001 остановлен, 23002-23010 работают пустыми.
- Потери зафиксированы: `ok-games-user-item-events-log:9` — на 23001 существовало только там
  «новое поколение» лога (LEO=10,585,636 сообщений, 5,347,356,740 байт), после unclean election
  и вывода 23001 утеряно (осознанно, по решению пользователя).

## Хронология операций

1. **Замер**: 9 URP led by 23001 (ISR=23001). 8 из 9 пустые на лидере (LEO=0), данные только в P9.
   Фолловеры P9 заморожены на старом поколении LEO=1,647,429,291 (retention'ом почищено до
   1.2GB/423MB) — лог на 23001 был пересоздан с offset 0.
2. **Попытка ручного unclean election при живом лидере → ОТКАЗ**: все 9 вернули
   `Valid replica already elected` — контроллер не делает unclean, пока лидер жив и в ISR.
   ⇒ чтобы принудительно увести лидерство с «живого, но застрявшего» брокера — остановить
   kafka-broker на нём, потом выборы.
3. На 5 топиках обнаружен **topic-override `unclean.leader.election.enable=true`** (default false;
   кто-то ставил до нас). При этом **авто-unclean при graceful stop всё равно НЕ сработал** —
   9 партиций повисли `Leader: none`; спас ручной запуск.
4. `systemctl stop kafka-broker` на 23001 (перед этим проверили LeaderCount=9 — уводили ровно его
   партиции; PartitionCount=23). Порт ушёл, партиции leaderless.
5. **`kafka-leader-election.sh --admin.config ... --election-type unclean --all-topic-partitions`**
   — сработал мгновенно: все 9 получили лидеров в kc/pc, ISR стал достраиваться (реплики
   синхронизировались между собой ещё до stop). `--all-topic-partitions` безопасен: election
   идёт только там, где лидера нет.
6. **Reassign**: полная карта (`kafka-topics --describe` → /tmp/full_map.txt) — 360 партиций,
   с uc-репликами **только 23** (все = партиции 23001; 23002-23010 уже были пусты).
   Генерация /tmp/reassign.json (python на хосте): заменять uc-реплику в-place на брокера
   **недостающего ДЦ** из оставшихся двух, least-loaded, порядок/preferred leader сохранён.
   Спред после: hc 24 / kc 23 / pc 23.
7. `--execute --throttle 104857600` → **упал `TimeoutException: incrementalAlterConfigs`**
   (повтор грабли MDBSUP-4166). `--execute` **без throttle** — успешно; данные тянутся с
   живых лидеров kc/pc, объёмы малы. Сходимость ~20-30 мин (последней дотягивалась
   ok-games-features-proto-base64-log:2).
8. **RF=4 → RF=3** (единственная: `vk-games-user-item-events-log:79` [20011,23001,21009,22009]):
   target [20011,21009,22009]. Грабля: `--execute` нового json для партиции с незавершённым
   реассигном → `Cannot execute because there is an existing partition assignment. Use --additional`.
   Не гадать с `--additional` — **дождаться завершения текущего реассигна**, тогда обычный
   `--execute` проходит мгновенно (все 3 целевые реплики уже в ISR).
9. **Unclean выключен** (просьба пользователя): снят topic-override
   `unclean.leader.election.enable` у 5 топиков (`kafka-configs --alter --delete-config`),
   проверено «нет override» — действует DEFAULT false.

## Грабли/факты

- Опции конфига у тулзов разные (3.8-style): `kafka-topics.sh`/`kafka-reassign-partitions.sh` —
  `--command-config`; `kafka-leader-election.sh` — **`--admin.config`** (ни `--command-config`,
  ни `--admin-client-config` не принимаются). Проверять `--help`.
- Ручной UNCLEAN не работает при живом лидере в ISR — см. п.2.
- Auto-unclean (override=true) не сработал на graceful stop — рассчитывать только на ручной запуск.
- `mcc sshexec` на длинные kafka-команды рвётся — всё через `mcc ssh`+expect+base64-скрипты
  (canonical паттерн, см. `mcc-host-worker/commands/ssh.md`).
- Ответ Jolokia: значение вложено как `{"value":{"Value":N}}`.

## Сеть (для сетевиков, снято 02.09)

- С kc-контроллера: `ping 10.189.121.44` (VIP 2.broker.uc) — **OK (ICMP)**, но TCP
  `[10.189.121.44]:9092` и `[fd00:...18ab]:9092` — **FAIL**; контрольный 2.broker.hc:9092 — OK.
- На самом uc-брокере: kafka active, `localhost:9092` и **собственный FQDN через VIP — OK**.
  ⇒ Kafka слушает и принимает; ICMP отвечает VIP-балансир; рвётся именно кросс-ДЦ TCP-флоу
  до брокерных VIP uc (диапазон 10.189.121.x, брокерные адреса .43/.44/...; при этом VIP
  uc-контроллера `10.189.121.46:9093` кросс-ДЦ работает, а WAN-v6 брокеров `2a00:...` — тоже).
- Вывод: мёртвы конкретные broker-facing VIP-эндпоинты uc, не Kafka и не весь диапазон.

## Осталось вне скоупа

- Вывод/withdraw хостов uc (23001-23010) из кластера через mdb-data (сейчас просто пустые).
- Ремонт сети uc / эскалация по конкретным VIP-эндпоинтам.
- Preferred leader election / CC rebalance после стабилизации (лидерства раскиданы по факту выборов).

## Пост-дрейн 2026-09-02 (вечер): сеть uc починена, stray на 23001 вычищены

- TCP pc→uc:9092 по FQDN: uc-2/3/5/10 OK — сеть/VIP починили. uc-1 не отвечал: брокер был
  остановлен после drain.
- 23001 запущен повторно: при старте LogManager пометил все 23 партиции как stray
  (renamed `<topic>-<p>.<topicId>-stray`), т.к. реплик 23001 в кластере 0/360.
- Очистка: `cd /mnt/data/log && rm -rf -- *-stray` — 23 директории, 214 ГБ освобождено.
  Остались только `__cluster_metadata-0` + чекпоинты. Брокер active, пустой (0 партиций).
- Грабля: в expect нельзя `grep -c -- -stray` без `--`; Tcl ест части команд — сложные
  конвейеры гонять целиком (du|awk), sentinel после каждой команды.
