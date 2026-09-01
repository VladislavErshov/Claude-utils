#!/usr/bin/env bash
# Читать PMS-переменные из реального pms.cloud.vk.team (mTLS). Скилл pms-worker.
#
# Usage:
#   pms-read.sh <host-or-fqdn> [property|prop,prop,...] [namespace] [application]
#
# Хост может быть:
#   - FQDN Kafka-хоста (1.broker.<queue>.<dc>.one-infra.ru) — автоматически конвертируется
#     в PMS-ключ по логике MdbHostUtil.queueShortNameFromHost (mdb-processing):
#       1.broker.<queue>.<dc>.one-infra.ru     → <queue>.clouds
#       1.controller.<queue>.<dc>.one-infra.ru → controller.<queue>.clouds
#       1.cruise.<queue>.<dc>.one-infra.ru     → <queue>.clouds (делит ключ с broker)
#   - готовым PMS-ключом (<queue>.clouds / controller.<queue>.clouds / host-mdb)
#   - любым другим ключом — передаётся как есть.
#
# Свойство (второй аргумент):
#   - одно имя: kafka.sysconfig
#   - список через запятую: "health.prod.rtconfig.warnings,data.prod.rtconfig.warnings.cluster"
#   - пусто → дефолтный список известных Kafka-свойств (KNOWN_KAFKA_PROPERTIES ниже)
#
# Примеры:
#   pms-read.sh 1.broker.test-resize-mdbdev-kafka.dc.one-infra.ru kafka.sysconfig
#   pms-read.sh test-resize-mdbdev-kafka.clouds "" infra mdb
#   pms-read.sh events-front-kafka.clouds "" dzen mdb
#   pms-read.sh host-mdb "health.prod.rtconfig.warnings,data.prod.rtconfig.warnings.cluster" infra mdb

set -euo pipefail

INPUT_HOST="${1:-}"
PROPERTY="${2:-}"
NAMESPACE="${3:-infra}"
APPLICATION="${4:-mdb}"

if [[ -z "$INPUT_HOST" ]]; then
  echo "Usage: pms-read.sh <host-or-fqdn> [property|prop,prop,...] [namespace] [application]" >&2
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
host_to_pms_key() {
  local input="$1"
  if [[ "$input" == *.clouds || "$input" != *.*.* || "$input" == host-mdb ]]; then
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

# Дефолт: все известные Kafka PMS-переменные (из KafkaPmsProperty.java в mdb-processing).
# Используются, когда свойство не указано явно.
KNOWN_KAFKA_PROPERTIES=(
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

if [[ -n "$PROPERTY" && "$PROPERTY" == *,* ]]; then
  # Список свойств через запятую.
  IFS=',' read -r -a PROPERTIES <<< "$PROPERTY"
elif [[ -n "$PROPERTY" ]]; then
  PROPERTIES=("$PROPERTY")
else
  PROPERTIES=("${KNOWN_KAFKA_PROPERTIES[@]}")
fi

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

echo "Input:  $INPUT_HOST"
echo "PMS key: $HOST   (namespace=$NAMESPACE, application=$APPLICATION)"
echo "-------------------------------------------------------------------------"
for prop in "${PROPERTIES[@]}"; do
  fetch_one "$prop"
done
