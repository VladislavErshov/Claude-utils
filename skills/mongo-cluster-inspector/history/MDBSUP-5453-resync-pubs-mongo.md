# MDBSUP-5453 (2026-09-17) — resync нод MongoDB publications-2/5 (dzen): грабли

Контекст: pub5 rc — mongod прибит SIGTERM 31.08 (юнит остался active(exited), автостарта нет); pub2 hidden rc — RECOVERING с марта. Oplog-окно primary ~25ч → полный resync (wipe + initial sync). Обе ноды MongoDB 6.0.19, ~550–600 ГБ.

## Грабли

1. **script.js инициализирует одиночный репликасет.** ExecStartPost `/etc/mongo_init/script.js` на пустой ноде: connect self с admin-кредами падает (юзеров нет) → localhost → replSetGetConfig fails → buildReplicaSet → `rs.initiate` одного себя → нода PRIMARY своего rs01, реальный кластер отвергает heartbeats ("replica set IDs do not match, ours: … remote node's: …"). НЕ стартовать пустую ноду через systemd-юнит. Правильный старт:
   `su -s /bin/bash mongodb -c 'ulimit -n 64000; exec /usr/bin/mongod --config /etc/mongod.conf'`
   Пустая нода, оставаясь в конфиге сета, получает конфиг от соседей и сама уходит в initial sync (STARTUP2).
2. **PAM-лимиты su = nofile 1024.** Прямой `su ... mongod` клампует maxConns до 819 и падает "Too many open files" в первые секунды initial sync (systemd-юнит LimitNOFILE не задаёт, лимит приходит из PAM юзера mongodb). Всегда `ulimit -n 64000` перед exec.
3. **Перед wipe проверить zen.mongodb.hosts** (PMS, ключ `<cluster>.clouds`, namespace dzen): почищенная нода, стоящая ПЕРВОЙ в списке, стартует инициализатором нового репликасета. Переставить в конец через update.do, верифицировать values.do. Hidden-хосты в списке обычно не фигурируют.
4. **`pkill -f 'bin/mongod --config'` убивает собственную ssh-сессию** — паттерн совпадает с cmdline самого шелла (mcc sshexec). После pkill проверять состояние ноды заново, команды после pkill не выполняются.
5. **systemd-юнит после ручного старта остаётся inactive** — это ок: при рестарте контейнера всё поднимется через юнит штатно, init-скрипт на ноде с данными идёт по ветке "replicaset is ready" (replSetGetConfig успешен → addSelfToRs) и ничего не ломает.
6. **Диагностика живости ноды**: `systemctl is-active mongod` может быть `active (exited)` при мёртвом процессе (unit oneshot/fork) — проверять `ps aux | grep bin/mongod` и конец `/mnt/logs/dbms/mongodb.log` (ищи "Received signal"/"Signal was sent by kill(2)").
7. PREFAIL/UNAVAILABLE хостов в mcc instances — plait-шум; mongod при этом жив. TCP-проба: `timeout 3 bash -c 'cat < /dev/null > /dev/tcp/<host>/27017'`. Репликасет смотреть только через rs.status (креды monitor — в /etc/mongo_init/script.js, у каждого кластера СВОИ пароли).

## Статус на 17.09 17:40

- Обе ноды STARTUP2/h:1, initial sync идёт. Операции в БД НЕ закрыты, тикет открыт, работа передана (next steps — в тикете, комментарий для Developers id 53333904).
- PMS pub5 zen.mongodb.hosts: rc переставлен в конец (write 17.09).
