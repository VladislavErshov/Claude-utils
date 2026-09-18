# 2026-09-09/10: полный обход 816 Kafka-кластеров на фантомных voter'ов (MDBDEV-3381)

Валидация настроек контроллеров по всем кластерам: сверка PMS ↔ host_state ↔ конфиги
контроллеров ↔ живой кворум (+ MaxFollowerLag). Триггер — учения (POST-5275), тикет
[MDBDEV-3381](https://jira.vk.team/browse/MDBDEV-3381) (закрыт). Детектор:
`~/Documents/utils/scan_phantom.sh`, фикс — `fix_phantom.sh` (доработан под ns/domen).
Процедура и грабли — в аддендуме файла
`2026-09-04-mass-quorum-phantom-voters-cleanup.md` (обязательно читать перед повтором).

## Итоги

- **Починены фантомы (26 кластеров, процедура 5044)**: billing-prod-vkbilling,
  billing-test-adtech, callbackd-billing-adtech, deb-test-pg, extdbperf-oneme,
  external-prod-cloudtraining, internal-events-p-adblogger (фантом-дубль 10002),
  internal-events-s-mdb9772, kafka-stats-misc-adtech, trg-195741-dwh, kuadwhprod-cfs
  (конфиги расходились все три), ssp-requests-adtech, stat-events-p-adblogger,
  target-mrd, vkmyvkteamprod-cfs, maxb2b-notify, story-external-p-adblogger,
  moneyflowd-billing-adtech, target-4-dwh, target-6-dwh, vkmarket-events-p-adblogger,
  kafka-seo (dzen), one-flow-vh (dzen), spellrustore-spell-rustore (dzen),
  logs-smsapi (финал — миграцией хоста владельцем), ads-format-dev family нет — см. таски.
- **Правки PMS (2)**: spellreplies-spellchecker — убран dc-фантом 14001;
  blogger-import-video — кворум расширен 3→4 (kc жив, решено владельцем; при расширении
  voter-set меняется только после рестарта ЛИДЕРА — верифицировать поэтапно).
- **Семейства-рецидивисты**: adblogger (internal/stat/vkmarket events), cfs-кластеры
  (kuadwhprod, vkmyvkteamprod) — старые 5-ДЦ layouts после миграций ДЦ.
- **Ложные тревоги**: PMS rate-limit под параллельной нагрузкой (пустые pms-read);
  Kafka 4.x CurrentVoters в JSON; кластеры под операциями (add_hosts и т.п.).
- **Не по voter'ам, передано в таски**: MDBSUP-5284…5295 (10 облачных ремонтов
  LOST_MINION/мёртвых брокеров, исполнитель L1/L2), MDBSUP-5296 (14 призраков без облака
  — проверка и чистка БД), 18 mdbdev-тестовых призраков вычищены сразу (117 строк
  host_state, 13 кластеров deleted=true).
- **Хвост**: adb-users — LAG ~102K при живых хостах, причина не найдена.

## Инструменты

- `~/Documents/utils/scan_phantom.sh` — детектор (4 источника, таймауты, resume, ns/domen
  через env `NS`/`PMS_NS`; dzen = `.idzn.ru`, vkontakte = `.vkcl.ru`).
- `~/Documents/utils/phantom_driver.sh` — драйвер слайсов по 3 кластера.
- `~/Documents/utils/kafka_ctrl_hosts.tsv` — карта кластеров (перегенерация SQL'ем с
  фильтром `db_cluster.deleted=false`; там же колонка env).
- Отчёты: `phantom_mismatch.txt` (вердикты по каждому), `phantom_scan_done.txt` (resume).

## Грабли (полный список в аддендуме 2026-09-04)

pms-read без ретраев под нагрузкой = ложный PMS_EMPTY; mcc sshexec без таймаута висит
минутами (обёртка фон+kill 30с обязательна); fix_phantom при расширении кворума требует
поэтапной верификации (voter-set меняет только рестарт лидера); zsh не сплитит слова —
батчи запускать из bash-драйвера; dzen-хосты в mcc ищутся по `.idzn.ru`, а в host_state
лежит каноническое `wan.idzn.ru` (не править в БД); перед фиксом смотреть operations
кластера за 2 дня — транзиенты операций выглядят как фантомы.
