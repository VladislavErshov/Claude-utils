# extdbpr + extdbperf (oneme): хронический URP из-за мёртвых реплик выведенного ДЦ — 3-й и 4-й кейс семьи (MDBSUP-5159, MDBSUP-5158)

**Дата:** 2026-09-08
**Тикеты:** [MDBSUP-5159](https://jira.vk.team/browse/MDBSUP-5159) (extdbpr), [MDBSUP-5158](https://jira.vk.team/browse/MDBSUP-5158) (extdbperf)
**Кластеры (oneme, infra):**
- `extdbpr-oneme-kafka` `3f9cfb21-8818-474f-aaa0-0e0ca86b39e0` — 60 брокеров (kc=21xxx, pc=22xxx, ec=23xxx, по 20)
- `extdbperf-oneme-kafka` `6a9e8ad1-10c8-41f2-8012-5d559dfdc1d4` — 75 брокеров (по 25 на ДЦ)
- fullQueue обоих: `<cluster>.oneme.db.production.mdb.prod`

## Диагноз (одинаков на обоих)

- Тот же паттерн, что [MDBSUP-4994 (extdbpu)](2026-09-02_extdbpu-oneme-kafka-dead-dc-replicas.md) и
  MDBSUP-4166: в Replicas партиций остались broker id диапазона **200xx** выведенного ДЦ,
  не зарегистрированные в кластере → перманентный URP, ISR=2 при minISR=2 (партиции «на грани»).
- extdbpr: **131 партиция / 10 топиков** (oneme_sferum_* ×72, user_profile_update ×18,
  oneme_maxb2b_* ×36, oneme_business_* ×18), мёртвые id **20021–20030**.
- extdbperf: **54 партиции / 2 топика** (oneme_tech_ClientDownloadImageEvents ×36,
  oneme_tech_BackSessionEvents ×18), мёртвые id **20018–20044** (27 id).
- Названия тикетов «54 offline партиции» — показания UI-панели поверх URP; на уровне Kafka
  offline (`--unavailable-partitions`) = 0, лидеры у всех есть. Проверять факт, а не заголовок тикета.
- extdbpr бонус: массовый PREFAIL 39/60 брокеров с 2026-09-07 10:18 (rscheck «Has N partitions
  with min in-sync replicas») → облачная задача **ACL update застревала на 7/20** с текстом
  «Not enough running replicas/spare capacity is reported» — это её readiness-чек, а не
  отдельная поломка. После снятия URP PREFAIL ушёл, ACL update должен докатиться сам.
  В kafka-операторе (`mcc ops`) задачи ACL нет — это облачный таск, не операторный.

## Фикс (по прецеденту 4994, оба кластера за один заход)

1. Диагностика одним sshexec: unavailable/URP counts, dead-ids (`Replicas` ∩ `^200`),
   ISR-гистограмма, зарегистрированные id через `kafka-broker-api-versions.sh`.
2. Фоновый полный describe (для расчёта нагрузки): topics list → цикл
   `kafka-topics --describe --topic` → `/tmp/full_<TAG>.txt` (5159: 891, 5158: 1881 партиций).
   Запускать **script-файлом + `setsid nohup`** (см. грабли).
3. Генератор `/tmp/gen_reassign.py` (python3 на хосте): для каждой URP-партиции мёртвый 200xx
   заменяется **in-place** (позиция и preferred leader сохраняются) на наименее нагруженного
   живого брокера **недостающего ДЦ** (у живой пары реплик 2 ДЦ, третий — куда класть).
   Бегущий счётчик нагрузки → новые реплики равномерно: extdbpr kc 43/pc 44/ec 44,
   extdbperf 18/18/18. Санити: count, RF=3, нет 200xx, 3 разных ДЦ, 0 skipped.
4. `kafka-reassign-partitions.sh --execute` **без throttle** (с ним TimeoutException —
   4-й случай подряд) → `--verify`: 131/131 и 54/54 completed, URP=0.
5. Верификация: `--at-min-isr-partitions` = 0, unavailable = 0, rscheck `/getstatus` → `true`,
   `mcc ops` — PREFAIL-алерт ушёл, брокеры AVAILABLE.

## Грабли (новое/повторённое)

- **`nohup ... &` внутри sshexec умирает вместе с сессией** (первая попытка сбора describe
  дала 0 строк) — писать команду в script-файл на хосте и запускать
  `setsid nohup bash /tmp/script.sh </dev/null >/dev/null 2>&1 &`.
- `mcc scp` файла на хост молча не залил (дважды) — только base64-чанки через expect
  (`mcc-host-worker/commands/scp.md`).
- grep `'^Topic:'` не матчит партиционные строки (они с табуляции) — фильтровать по `Replicas:`.
- kafka-утилиты: `/opt/kafka/bin/`, admin-конфиг `/opt/kafka/config/client.properties`;
  bootstrap — **FQDN хоста**, не localhost (SSL hostname verification падает).
- Локальные циклы по хостам в zsh: `set -- $var` не вордрасплитит — явные переменные/функции.
- `mcc sshexec` рвёт долгие команды (`Connection closed by remote host`) в конце вывода —
  косметика, команда выполняется; тяжёлое — в фон через setsid.

## Вывод

Семейство extdbp*-oneme-kafka (extdbpu/extdbpr/extdbperf) теряло топики при выводе ДЦ —
4994, 5159, 5158. При следующем выводе ДЦ в этом семействе (или любом delete_hosts с
reassign) проверять после операции: URP=0 **и** отсутствие `^200` в Replicas полного
describe (URP-алерты не приходили — «тихий» недореплицированный статус ловится только
такой сверкой).

## Ссылки

- Прецедент: [2026-09-02_extdbpu-oneme-kafka-dead-dc-replicas.md](2026-09-02_extdbpu-oneme-kafka-dead-dc-replicas.md) (MDBSUP-4994)
- Заглушки: `jira-mdbsup-solver/history/MDBSUP-5159-2026-09-08.md`, `jira-mdbsup-solver/history/MDBSUP-5158-2026-09-08.md`
- Прецедент-паттерн: MDBSUP-4166 (known_issues.md «Offline partitions из-за удалённого брокера в Replicas»)
