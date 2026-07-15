#!/usr/bin/env bash
# Читать PMS-переменные для Kafka-хоста из реального pms.cloud.vk.team (mTLS).
#
# Usage:
#   pms-read.sh <host-or-fqdn> [property] [namespace] [application]
#
# Хост может быть как FQDN (1.broker.<queue>.<dc>.one-infra.ru), так и уже
# PMS-ключом (<queue>.clouds / controller.<queue>.clouds). FQDN автоматически
# конвертируется в PMS-ключ по логике MdbHostUtil.queueShortNameFromHost
# (mdb-processing/common/util/MdbHostUtil.java):
#   - 1.broker.<queue>.<dc>.one-infra.ru   → <queue>.clouds
#   - 2.broker.<queue>.<dc>.one-infra.ru   → <queue>.clouds
#   - 1.controller.<queue>.<dc>.one-infra.ru → controller.<queue>.clouds
#   - 1.cruise.<queue>.<dc>.one-infra.ru   → <queue>.clouds  (cruise делит ключ с broker)
#
# Примеры:
#   pms-read.sh 1.broker.test-resize-mdbdev-kafka.dc.one-infra.ru
#     — прочитать все известные Kafka PMS-переменные для broker-хоста
#   pms-read.sh 1.broker.test-resize-mdbdev-kafka.dc.one-infra.ru kafka.sysconfig
#     — прочитать одну переменную
#   pms-read.sh test-resize-mdbdev-kafka.clouds kafka.sysconfig
#     — явно указать PMS-ключ

set -euo pipefail

INPUT_HOST="${1:-}"
PROPERTY="${2:-}"
NAMESPACE="${3:-infra}"
APPLICATION="${4:-mdb}"

if [[ -z "$INPUT_HOST" ]]; then
  echo "Usage: pms-read.sh <host-or-fqdn> [property] [namespace] [application]" >&2
  exit 2
fi

CERT="${HOME}/.mccloud/client.cert"
KEY="${HOME}/.mccloud/client.key"
CA="${HOME}/.mccloud/ca.crt"

if [[ ! -f "$CERT" || ! -f "$KEY" || ! -f "$CA" ]]; then
  echo "Missing mccloud mTLS files in ~/.mccloud/ (expected client.cert, client.key, ca.crt)" >&2
  exit 3
fi

# Конвертация FQDN → PMS-ключ.
# FQDN формат: index.role.queueShortName.dc.one-infra.ru
# split(".") → [index, role, queueShortName, dc, "one-infra", "ru"]
# PMS-ключ: <queueShortName>.clouds (broker/cruise) или controller.<queueShortName>.clouds (controller)
host_to_pms_key() {
  local input="$1"
  if [[ "$input" == *.clouds ]]; then
    echo "$input"
    return
  fi
  local IFS='.'
  read -ra parts <<< "$input"
  if [[ ${#parts[@]} -lt 6 ]]; then
    # Не FQDN и не .clouds — передаём как есть (пусть PMS решит).
    echo "$input"
    return
  fi
  local role="${parts[1]}"
  local queue="${parts[2]}"
  if [[ "$role" == "controller" ]]; then
    echo "controller.${queue}.clouds"
  else
    echo "${queue}.clouds"
  fi
}

HOST=$(host_to_pms_key "$INPUT_HOST")

# Все известные Kafka PMS-переменные (из KafkaPmsProperty.java в mdb-processing)
KNOWN_PROPERTIES=(
  kafka.soc.audit.enabled
  kafka.sysconfig
  kafka.broker.properties
  kafka.controller.properties
  kafka.cruisecontrol.properties
  kafka.cruisecontrol.capacity.json
  kafka.cruisecontrol.sysconfig
  kafka.cruisecontrol.log4j.properties
  kafka.cruisecontrol.jaas.conf
  kafka.layout
  kafka.controller.quorum
  kafka.isWanCluster
  kafka.ssl.enabled
  kafka.keystore.password.vault.path
  kafka.truststore.password.vault.path
  kafka.hostInfo.pushUrl
  kafka.log4j.properties
  kafka.tools.log4j.properties
  zen.kafka.vaultRoot
)

fetch_one() {
  local prop="$1"
  local tmp_body
  tmp_body=$(mktemp)
  local http_code curl_exit
  http_code=$(curl -s --max-time 60 \
    --cert "$CERT" --key "$KEY" --cacert "$CA" \
    -H "x-namespace: $NAMESPACE" \
    "https://pms.cloud.vk.team/api/conf/values.do?application=${APPLICATION}&property=${prop}" \
    -o "$tmp_body" -w "%{http_code}" 2>/dev/null)
  curl_exit=$?

  if [[ $curl_exit -ne 0 || "$http_code" != "200" ]]; then
    printf "%-50s  <ERROR: HTTP=%s curl_exit=%d>\n" "$prop" "$http_code" "$curl_exit"
    rm -f "$tmp_body"
    return
  fi

  local value
  value=$(jq -r --arg h "$HOST" '.[$h] // "<NOT_SET>"' "$tmp_body" 2>/dev/null || echo "<PARSE_ERROR>")
  rm -f "$tmp_body"

  if [[ "$value" == "<NOT_SET>" ]]; then
    printf "%-50s  <NOT_SET>\n" "$prop"
  elif [[ "$value" == *$'\n'* ]]; then
    printf "%-50s  ┌─\n" "$prop"
    while IFS= read -r line; do
      printf "  %-50s│ %s\n" "" "$line"
    done <<< "$value"
    printf "  %-50s  └─\n" ""
  else
    printf "%-50s  %s\n" "$prop" "$value"
  fi
}

if [[ -n "$PROPERTY" ]]; then
  fetch_one "$PROPERTY"
else
  echo "Input:  $INPUT_HOST"
  echo "PMS key: $HOST   (namespace=$NAMESPACE, application=$APPLICATION)"
  echo "-------------------------------------------------------------------------"
  for prop in "${KNOWN_PROPERTIES[@]}"; do
    fetch_one "$prop"
  done
fi
