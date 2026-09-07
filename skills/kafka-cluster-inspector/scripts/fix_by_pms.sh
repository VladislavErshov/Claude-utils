#!/usr/bin/env bash
# Обёртка: определяет ДЦ живых контроллеров из PMS-кворума (не host_state — там зомби)
# и вызывает fix_phantom.sh. Usage: fix_by_pms.sh <queue>
Q="$1"
RAW=$(~/.claude/skills/pms-worker/bin/pms-read.sh "1.broker.$Q.hc.one-infra.ru" "kafka.controller.quorum" infra mdb 2>/dev/null | grep -oE '[0-9]+@1\.controller\.[A-Za-z0-9.-]+' )
DCS=$(echo "$RAW" | sed "s/^[0-9]*@1\.controller\.$Q\.//; s/\.one-infra\.ru$//" | sort -u | tr '\n' ' ')
echo "##### $Q (DCs: $DCS)"
"$(dirname "$0")/fix_phantom.sh" "$Q" $DCS 2>&1 | grep -E "PMS voters|уже верный|confp|рестарт|OK:|FAIL|DONE|Кворум|RETRY_FAILED"
