#!/usr/bin/env bash
# Фикс фантомного voter'а в KRaft-кворуме (процедура MDBSUP-5044).
# Usage: fix_phantom.sh <queue> <dc1> <dc2> <dc3> ...
# queue — имя PMS-очереди кластера (напр. actions-billing-adtech-kafka)
# dc*   — ДЦ живых контроллеров (по ним строится FQDN 1.controller.<queue>.<dc>.one-infra.ru)
set -u
Q="$1"; shift
DCS=("$@")
NS="mcc --local -n infra sshexec"
RETRY() { # RETRY <host> <cmd> — до 5 попыток
  local h="$1" cmd="$2" out="" i
  for i in 1 2 3 4 5; do
    out=$($NS "$h" "$cmd" 2>&1) && { echo "$out"; return 0; }
    sleep 4
  done
  echo "RETRY_FAILED: $out"; return 1
}
ctrl() { echo "1.controller.$Q.$1.one-infra.ru"; }
broker() { echo "1.broker.$Q.$1.one-infra.ru"; }

# 1. PMS-кворум → эталонный список ID
PMS_RAW=$(~/.claude/skills/pms-worker/bin/pms-read.sh "$(broker "${DCS[0]}")" "kafka.controller.quorum" infra mdb 2>/dev/null | grep -oE '[0-9]+@' | tr -d '@' | sort -n | tr '\n' ',' | sed 's/,$//')
[ -z "$PMS_RAW" ] && { echo "FAIL: PMS quorum не прочитан"; exit 1; }
echo "PMS voters: [$PMS_RAW]"

# 2. Конфиги контроллеров: кто в файле, confp для расходящихся
STALE=()
for dc in "${DCS[@]}"; do
  h=$(ctrl "$dc")
  ids=$(RETRY "$h" "grep 'controller.quorum.voters=' /opt/kafka/config/controller.properties" | sed 's/.*voters=//' | grep -oE '^[0-9]+|[0-9]+@' | tr -d '@' | sort -n | tr '\n' ',' | sed 's/,$//')
  if [ "$ids" = "$PMS_RAW" ]; then echo "$dc: файл уже верный"; else
    echo "$dc: файл [$ids] -> confp"
    RETRY "$h" "confp --oneshot >/dev/null 2>&1" >/dev/null
    ids2=$(RETRY "$h" "grep 'controller.quorum.voters=' /opt/kafka/config/controller.properties" | sed 's/.*voters=//' | grep -oE '^[0-9]+|[0-9]+@' | tr -d '@' | sort -n | tr '\n' ',' | sed 's/,$//')
    [ "$ids2" = "$PMS_RAW" ] || { echo "FAIL: $dc после confp [$ids2]"; exit 1; }
    STALE+=("$dc")
  fi
done
[ ${#STALE[@]} -eq 0 ] && { echo "Нет устаревших конфигов — выход"; exit 0; }

# 3. Лидер + порядок рестартов: follower'ы сначала, лидер последний
b=$(broker "${DCS[0]}")
for i in 1 2 3; do
  QOUT=$(RETRY "$b" "/opt/kafka/bin/kafka-metadata-quorum.sh --bootstrap-server $b:9092 --command-config /opt/kafka/config/client.properties describe --status 2>/dev/null | grep -E 'LeaderId|MaxFollowerLag:|CurrentVoters'")
  [ -n "$QOUT" ] && break; sleep 5
done
echo "Кворум сейчас: $(echo "$QOUT" | tr '\n' ' ')"
LEADER=$(echo "$QOUT" | grep LeaderId | grep -oE '[0-9]+')
ORDER=()
for dc in "${STALE[@]}"; do
  nid=$(RETRY "$(ctrl "$dc")" "grep -oE '^node.id=[0-9]+' /opt/kafka/config/controller.properties" | cut -d= -f2)
  [ "$nid" = "$LEADER" ] && LEADER_DC="$dc" || ORDER+=("$dc")
done
[ -n "${LEADER_DC:-}" ] && ORDER+=("$LEADER_DC")

# 4. Рестарты по одному с верификацией
for dc in "${ORDER[@]}"; do
  h=$(ctrl "$dc")
  echo "--- рестарт $dc ($h)"
  RETRY "$h" "systemctl restart kafka-controller && echo RESTART_OK" | grep -q RESTART_OK || { echo "FAIL: рестарт $dc"; exit 1; }
  sleep 45
  ok=0
  for i in 1 2 3 4; do
    QOUT=$(RETRY "$b" "/opt/kafka/bin/kafka-metadata-quorum.sh --bootstrap-server $b:9092 --command-config /opt/kafka/config/client.properties describe --status 2>/dev/null | grep -E 'LeaderId|MaxFollowerLag:|CurrentVoters'")
    cur=$(echo "$QOUT" | grep CurrentVoters | grep -oE '\[.*\]' | tr -d '[]' | tr ',' '\n' | sed 's/ //g' | sort -n | tr '\n' ',' | sed 's/,$//')
    lag=$(echo "$QOUT" | grep 'MaxFollowerLag:' | grep -oE '[0-9]+')
    if [ "$cur" = "$PMS_RAW" ] && [ "$lag" = "0" ]; then ok=1; break; fi
    sleep 20
  done
  [ $ok -eq 1 ] && echo "$dc OK: $(echo "$QOUT" | tr '\n' ' ')" || { echo "FAIL: кворум не собрался после $dc: $QOUT"; exit 1; }
  RETRY "$h" "systemctl restart rscheck@kafka" >/dev/null
done
echo "=== DONE $Q: voters=[$PMS_RAW] ==="
