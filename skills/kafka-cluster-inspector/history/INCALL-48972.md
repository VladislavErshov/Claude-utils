# INCALL-48972 — Kafka не пережила минус ДЦ KC: фантомный voter 10001@hc в controller.quorum.voters

**Кластер**: `maxb2b-console-notify-kafka` (ID `1ed074ae-bc98-4385-9015-89e22c394899`), prod, notify.
**Хосты**: 3 брокера + 3 контроллера — ec/kc/pc (по одному на ДЦ). Kafka 3.8.0, static KRaft voters.
**Инцидент**: 25.08.2026 учения «Георезерв — минус KC» (20:31–21:14). При отрыве KC лидер
контроллера не переизбрался, кластер лёг. Починили ручным рестартом брокеров и контроллеров.

## Симптом (логи ec-контроллера 13001, 25.08 20:36)

```
QuorumState - [RaftManager id=13001] voters=[10001, 12001, 11001, 13001]
QuorumController - In the new epoch 8625XX, the leader is (none).   ← по кругу
CandidateState(13001) voteStates={10001=UNRECORDED, 12001=GRANTED, 11001=UNRECORDED, 13001=GRANTED}
```

ec и pc голосуют друг за друга → 2 голоса из 4 → большинство (3) не достижимо → бесконечные
election'ы без лидера. 10001 (hc) — мёртвый фантом, 11001 (kc) — отрезанный ДЦ.

## Корень

`controller.quorum.voters` на хостах (конфиг отрендерен confp из PMS `kafka.controller.quorum`):

| Хост | voters в controller.properties | mtime |
|---|---|---|
| controller **ec** (13001) | 4: **10001@hc (фантом)** + kc + pc + ec | 2026-07-29 16:52 |
| controller **pc** (12001) | 4: **10001@hc (фантом)** + kc + pc + ec | 2026-07-29 16:55 |
| controller kc (11001) | 3: kc + pc + ec — корректно | 2026-08-25 21:35 (рестарт при инциденте → перерендер) |
| брокеры ec/pc/kc | 3: kc + pc + ec — корректно | 2026-08-17 |

PMS `kafka.controller.quorum` **сейчас правильный** (3 voter'а без hc). 29.07 при миграции
контроллера hc→ec в PMS временно было 4-voter значение — под него отрендерились ec/pc, затем
PMS поправили, но ec/pc контроллеры не перерендерили и не рестартили. Отсюда: пока все три ДЦ
живы — кворум 3/4 собирался; минус любого одного ДЦ → 2/4 → полный отказ кворума.

Дополнительно: PMS `kafka.layout=hc,kc,pc,ec,rc` — stale (hc/rc ДЦ нет), но voters из layout
не рендерятся (только `kafka.controller.quorum`), а править layout опасно (от него зависит
node.id по позиции ДЦ — см. I48592). Не трогать.

Тот же класс проблемы, что INCALL-42685 (рассинхрон voters при миграции ДЦ контроллеров).

## Проверка PMS (по запросу пользователя — «ошибка в файле кворума в PMS?»)

Проверены: брокерский ключ `maxb2b-console-notify-kafka.clouds` (полный дамп всех kafka.*),
controller-ключ `controller.<queue>.clouds`, per-host ключи всех трёх controller-FQDN.

| PMS-переменная | Значение | Вердикт |
|---|---|---|
| `kafka.controller.quorum` | 3 voter'а: kc + pc + ec | ✅ корректно, ошибки НЕТ |
| `kafka.layout` | `hc,kc,pc,ec,rc` | stale, но **load-bearing**: node.id = `10000 + dc_id*1000 + instance_id`, dc_id = позиция ДЦ в layout (hc=0…rc=4). Удалить hc → сдвинутся node.id всех живых контроллеров → кластер ляжет (см. I48592). НЕ ТРОГАТЬ. |
| `kafka.cruisecontrol.properties` → `bootstrap.servers` | `kc, hc, pc` — hc не существует, ec отсутствует | ❌ реальная stale-ошибка в PMS, но на кластере нет cruise-хоста → некому потреблять. Мина при будущем добавлении CC. |
| per-host `kafka.controller.quorum` на FQDN контроллеров | `<NOT_SET>` | оверрайдов нет |

Вывод: гипотеза «ошибка в PMS-кворуме» НЕ подтвердилась — кворум в PMS правильный.
4-voter рендер на ec/pc взят из СТАРОГО значения PMS (до фикса 29.07); файлы на хостах
просто ни разу не перерендеривались после этого (mtime 29.07 у ec/pc vs 17.08 у брокеров,
25.08 у kc — рестарт при инциденте).

## Фикс

На ec и pc (по одному, ec первым — он фолловер; pc — действующий лидер, вторым):
```bash
confp --oneshot                      # перерендер controller.properties из актуального PMS
grep ^controller.quorum.voters /opt/kafka/config/controller.properties   # проверить: 3 voter'а, без hc
systemctl restart kafka-controller
```
Верификация: `kafka-metadata-quorum.sh --bootstrap-controller <ctrl>:9093 --command-config
/opt/kafka/config/client.properties describe --replication` — 3 voter'а, 10001 исчез,
лидер есть, lag=0.

## Результат фикса (27.08, выполнен)

- ec: confp перерендерил voters 4→3, рестарт OK («Kafka Server started»), вернулся Follower'ом.
  Сразу после рестарта ec лидер переехал pc→kc (11001) и фантом 10001 исчез из вью кворума.
- pc: confp → voters 4→3, рестарт OK.
- Финал: LeaderId=11001 (kc), CurrentVoters=[12001,11001,13001], lag=0 на всех voter'ах и
  observer'ах. Потеря любого одного ДЦ теперь переживается (кворум 2/3).
- PMS не меняли; `kafka.cruisecontrol.properties` (stale bootstrap.servers с hc) осознанно
  не трогаем — перезапишется при добавлении cruise-control.

Грабли:
- `kafka-metadata-quorum` в 3.8: `--command-config` ставится ДО подкоманды describe,
  конфиг — `/opt/kafka/config/client.properties` (`/etc/kafka/` пуст).
- Скрипты Kafka не в PATH на контроллере — полный путь `/opt/kafka/bin/...`.
- Рестартовать по одному, дожидаясь возврата в кворум; после рестарта лидера (pc) будет
  кратковременный metadata unavailable (секунды) — брокеры это переживают.
