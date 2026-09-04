#!/bin/zsh
# Watchdog v4 локального MDB-стека: автоподнятие УПАВШИХ сервисов.
# Ключевые отличия от v3:
#   - рестарт только после 3 ПОДРЯД неудачных проверок (не убивает медленно-но-живые)
#   - curl timeout 10с
#   - перед стартом: gradle --stop + зачистка процессов проекта (нет копления демонов)
# Лог: /tmp/mdb-watchdog.log. Запуск: nohup ~/.claude/skills/mdb-local-tester/scripts/watchdog.sh &
# Останов: pkill -f mdb-watchdog

LOG=/tmp/mdb-watchdog.log
INTERVAL=45
BOOT=200      # сек после старта сервиса, когда проверки пропускаются
FAILS_NEEDED=3 # подряд неудачных проверок для рестарта

MDATA=/Users/vl.ershov/Documents/Git/mdb-data
MHEALTH=/Users/vl.ershov/Documents/Git/mdb-health
MPROC=/Users/vl.ershov/Documents/Git/mdb-processing
MUI=/Users/vl.ershov/Documents/Git/mdb
BACKSTAGE=/Users/vl.ershov/Documents/Git/backstage

log() { echo "$(date '+%H:%M:%S') $*" >> $LOG; }

alive() { curl -s -m 10 "http://localhost:$1/actuator/health/liveness" 2>/dev/null | grep -q '"UP"'; }
http_up() { port=$1; path=$2; code=$(curl -s -m 12 -o /dev/null -w '%{http_code}' "http://localhost:$port$path" 2>/dev/null); [ -n "$code" ] && [ "$code" != "000" ]; }

kill_port() { lsof -tiTCP:$1 -sTCP:LISTEN 2>/dev/null | xargs -r kill -9; }
kill_project() {
  ps aux | grep -E "[G]it/$1" | awk '{print $2}' | xargs -r kill -9 2>/dev/null
  sleep 2
}
rotate() { [ -f "$1" ] && mv "$1" "$1.last"; }

start_mdb_data() {
  kill_port 8081
  (cd $MDATA && ./gradlew --stop >/dev/null 2>&1)
  kill_project mdb-data; rotate /tmp/mdb-data.log
  cd $MDATA && /usr/bin/nohup env BOOT_RUN_JVM_ARGS="--add-opens java.base/java.lang=ALL-UNNAMED -Xmx1500m" \
    ./gradlew bootRun --args='--spring.profiles.active=local --server.port=8081' > /tmp/mdb-data.log 2>&1 &
  log "mdb-data запущен (pid $!)"
}

start_mdb_health() {
  kill_port 8082
  (cd $MHEALTH && ./gradlew --stop >/dev/null 2>&1)
  kill_project mdb-health; rotate /tmp/mdb-health.log
  cd $MHEALTH && /usr/bin/nohup env BOOT_RUN_JVM_ARGS="-Xmx1500m" ./gradlew bootRun --args='--spring.profiles.active=local --server.port=8082 --rtconfig.local-config.path=src/main/resources/rtconfig/local.hjson --spring.ssl.bundle.pem.pms-client.keystore.certificate=/Users/vl.ershov/.mccloud/client.cert --spring.ssl.bundle.pem.pms-client.keystore.private-key=/Users/vl.ershov/.mccloud/client.key --spring.ssl.bundle.pem.pms-client.truststore.certificate=/Users/vl.ershov/.mccloud/ca.crt --redis.storage.ttl.host_info_ttl=24h --redis.storage.ttl.cluster_availability_ttl=1m --vault.vaults.infra.address=http://localhost:8200 --vault.vaults.infra.token=root' > /tmp/mdb-health.log 2>&1 &
  log "mdb-health запущен (pid $!)"
}

start_mdb_processing() {
  kill_port 8080
  (cd $MPROC && ./gradlew --stop >/dev/null 2>&1)
  kill_project mdb-processing; rotate /tmp/mdb-processing.log
  cd $MPROC && /usr/bin/nohup env BOOT_RUN_JVM_ARGS="-Xmx1500m" \
    ./gradlew bootRun --args='--spring.profiles.active=local' > /tmp/mdb-processing.log 2>&1 &
  log "mdb-processing запущен (pid $!)"
}

start_backstage() {
  kill_port 7007
  ps aux | grep "[m]db-start-backend" | awk '{print $2}' | xargs -r kill -9 2>/dev/null; sleep 2
  rotate /tmp/backstage.log
  cd $BACKSTAGE && /usr/bin/nohup /usr/bin/env PATH="/opt/homebrew/opt/node@18/bin:/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin" HOME="$HOME" yarn mdb-start-backend > /tmp/backstage.log 2>&1 &
  log "backstage запущен (pid $!, node18)"
}

start_vite() {
  kill_port 3012
  cd $MUI && /usr/bin/nohup /usr/bin/env PATH="$HOME/.nvm/versions/node/v22.23.2/bin:/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin" pnpm run dev > /tmp/mdb-ui.log 2>&1 &
  log "vite запущен (pid $!)"
}

start_vkone() {
  kill_port 8090
  /usr/bin/nohup node $HOME/.claude/skills/mdb-local-tester/scripts/vkone-stub.mjs > /tmp/vkone-stub.log 2>&1 &
  log "vkone-stub запущен (pid $!)"
}

lock=/tmp/mdb-watchdog.lock
if [ -f $lock ] && kill -0 $(cat $lock 2>/dev/null) 2>/dev/null; then echo "watchdog уже работает"; exit 1; fi
echo $$ > $lock

typeset -A GRACE FAILS
log "=== watchdog v4 стартовал (pid $$) ==="

check() {
  local port=$1 name=$2 cmd=$3 start_fn=$4
  local now=$EPOCHSECONDS
  if [ "${GRACE[$port]:-0}" -gt "$now" ]; then return; fi
  if eval "$cmd"; then
    FAILS[$port]=0
    return
  fi
  FAILS[$port]=$(( ${FAILS[$port]:-0} + 1 ))
  if [ "${FAILS[$port]}" -ge $FAILS_NEEDED ]; then
    log "$name (:$port) не отвечает ${FAILS[$port]} проверки подряд — перезапускаю"
    FAILS[$port]=0
    $start_fn
    GRACE[$port]=$(( now + BOOT ))
  fi
}

while true; do
  check 8081 mdb-data "alive 8081" start_mdb_data
  check 8082 mdb-health "alive 8082" start_mdb_health
  check 8080 mdb-processing "alive 8080" start_mdb_processing
  check 7007 backstage "http_up 7007 /api/mdb/projects" start_backstage
  check 3012 vite "http_up 3012 /" start_vite
  check 8090 vkone-stub "http_up 8090 /api/v1/user/info" start_vkone
  sleep $INTERVAL
done
