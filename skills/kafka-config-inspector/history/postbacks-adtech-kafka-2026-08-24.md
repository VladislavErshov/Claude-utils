# postbacks-adtech-kafka — stale controller.quorum.voters на pc (сирота I48592-паттерна)

Дата: 2026-08-24. Cluster id: `b6f2ad69-5580-481a-9875-6dc030ae0066`, namespace `infra`.

## Симптом

На pc-контроллере `kafka.controller.quorum` в PMS (3 voters) расходился с
`controller.properties` на хосте (4 voters — включал несуществующий `10001@hc`).
Похож на I48592 (eventstream-general), но там было сломано `kafka.layout` →
неверные node.id; здесь layout корректен, застрял сам рендер voters.

## Проверенное состояние

PMS (`<queue>.clouds`, brokers key — controller-настройки лежат там, НЕ на
`controller.<queue>.clouds`):

- `kafka.layout = hc,kc,pc,ec` → kc=11001, pc=12001, ec=13001 ✓
- `kafka.controller.quorum = 11001@kc, 12001@pc, 13001@ec` ✓

Хосты (config + /mnt/data/{log,metadata}/meta.properties + env cloud_name):

| Хост | node.id | voters в конфиге | вердикт |
|---|---|---|---|
| ec | 13001 | 3 | ✓ |
| kc | 11001 (leader) | 3 | ✓ |
| pc | 12001 | **4 (с 10001@hc)** | stale render |

Кластер при этом был AVAILABLE 3/3: pc считал кворум 3-из-4, majority сохранялся.
Риск: при падении любого одного контроллера pc видел бы 2/4 → потеря кворума.

## Фикс (сделал пользователь)

`confp --oneshot` + `systemctl restart kafka-controller` на pc:
конфиг перерендерен 15:41:33, рестарт 15:41:37 (после рендера — важно).
Итог: PMS = конфиг = meta.properties на всех трёх, kc=leader.

## Грабли метода

- `kafka-metadata-quorum.sh describe --replication` с хоста падает:
  OOM (heap CLI) / TimeoutException — порт 9093 под TLS/SASL, PLAINTEXT-клиент
  не коннектится. Live-статус кворума надёжнее смотреть в UI mdb-data.
- Проверять не только конфиг, но и **порядок**: рестарт сервиса должен быть
  ПОСЛЕ mtime controller.properties, иначе раннинг держит старых voters.
- `mcc instances -n infra "%.postbacks-adtech-kafka.%"` → EntityNotFound
  (кластер не находится по паттерну); состав хостов брать из voters/PMS.
