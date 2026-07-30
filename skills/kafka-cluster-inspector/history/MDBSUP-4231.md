# MDBSUP-4231 — SSL handshake failed на cluster 2 при идентичных с cluster 1 CA/конфиге

Дата: 2026-07-30 (инцидент 2026-07-28)

## Симптомы

Клиент `portal-control-plane` на хосте `1.portal.us-rustore-test-main.rc.aicld.ru` пишет
в Kafka через librdkafka (`sasl_ssl://...`). Есть два кластера:

- **Cluster 1** (рабочий): `vk-video-kafka-prd-misearch-kafka`, брокеры в `dc`.
- **Cluster 2** (битый): `srch-rustore-spell-rustore-kafka`, брокеры в `dc`, `kc`, `rc`.

Конфиги клиента почти идентичны (разница в user/password, CA-файл — один и тот же на
обоих хостах-клиентах). Cluster 1 работает, cluster 2 — нет. Сеть до брокеров есть
(`telnet 2.broker.srch-rustore-spell-rustore-kafka.kc.one-infra.ru 9092` → `Connected`).

В логах клиента три типа ошибок:

```
{SSL} sasl_ssl://1.broker.srch-rustore-spell-rustore-kafka.dc.one-infra.ru:9092/22001:
  a_verify.c:213: error:0D0C50A1:lib(13):func(197):reason(161):  (err: 0)

SSL handshake failed: s3_clnt.c:1264: error:14090086:lib(20):func(144):reason(134):
  (after 8ms in state CONNECT) (err: -181)

Failed to connect to broker at [...rc.one-infra.ru]:9: Network is unreachable
  (after 1ms in state CONNECT) (err: -195)
```

Также: `Failed to deliver a message: Local: Message timed out (err: -192)` — следствие
отсутствия коннекта.

## Расшифровка кодов

| Код | OpenSSL | Что значит |
| --- | --- | --- |
| `0D0C50A1` | lib=ASN1(13), func=ASN1_verify(197), reason=UNKNOWN_MESSAGE_DIGEST_ALGORITHM(161) | Клиент не может проверить подпись **какого-то** сертификата в chain — нет подходящего digest-алгоритма. Часто — в truststore клиента лежит сертификат с устаревшим/отключённым alg (SHA1, gost, и т.п.), либо truststore вообще не от этого кластера. |
| `14090086` | lib=SSL(20), func=SSL3_READ_BYTES(144), reason=TLSV1_ALERT_CERTIFICATE_UNKNOWN(134) | Сервер прислал TLS alert `certificate_unknown` — **вторичная** реакция: клиент прервал handshake, сервер отвечает alert'ом. Не корневая причина. |
| `err: -181` | librdkafka `ERR_SSL` | SSL handshake failed. |
| `err: -192` | librdkafka `ERR__MSG_TIMED_OUT` | Producer не доставил сообщение — следствие. |
| `err: -195` | librdkafka `ERR__TRANSPORT` | Network is unreachable — на `rc`-брокере в момент лога. `telnet` позже прошёл → либо временно, либо IPv6 vs IPv4 (AAA-запись). Не корневая причина для `dc`/`kc`. |

Главная подсказка — **`0D0C50A1`**. Это клиентская верификация подписи, **не** сеть и **не**
mTLS-отказ сервером.

## Что проверено на брокере cluster 2 (`1.broker.srch-rustore-spell-rustore-kafka.dc`)

```bash
mcc --local sshexec -n infra 1.broker.srch-rustore-spell-rustore-kafka.dc.one-infra.ru \
  "openssl x509 -in /opt/kafka/ssl/tls_ca.crt -noout -subject -issuer -fingerprint -sha256"
```

- **CA** (`tls_ca.crt`): `O=VK LLC, CN=one-cloud Infrastructure Vault Certificate Authority`,
  fingerprint `67:76:D9:45:C7:76:C1:57:5F:E6:6C:85:AB:1D:7F:7C:AB:77:18:72:D0:2C:60:02:B8:C8:10:3B:F7:71:52:55`,
  notBefore=Jul 9 2024, notAfter=Jul 9 2034.
- **Server cert** (`tls.crt`): `CN=1.broker.srch-rustore-spell-rustore-kafka.dc.one-infra.ru`,
  issuer = тот же intermediate CA, `Signature Algorithm: sha256WithRSAEncryption`,
  SAN: `DNS:1.broker.srch-rustore-spell-rustore-kafka.dc.one-infra.ru`,
  notBefore=Jul 27 2026 (свежий, 3 дня назад).
- **Chain с сервера** (`openssl s_client -showcerts`): leaf → intermediate → root,
  `Verify return code: 0 (ok)`.
- **truststore** (`client.truststore.jks`, PKCS12): 1 entry `cakafka`, SHA-256 fingerprint
  совпадает с `tls_ca.crt` (intermediate). Дата изменения — Jul 30 2026 (сегодня, совпадает
  с рендером конфига).
- **broker.properties**: `listener.security.protocol.map=CONTROLLER:SASL_SSL,INTERNAL:SASL_SSL,WAN:SASL_SSL`,
  `sasl.enabled.mechanisms=PLAIN,SCRAM-SHA-256`, `ssl.client.auth` **не задан** (no mTLS).
  Конфиг **идентичен** cluster 1.

## Что проверено на брокере cluster 1 (`1.broker.vk-video-kafka-prd-misearch-kafka.dc`)

- **CA**: тот же subject/issuer/fingerprint `67:76:D9:45:...`, те же dates.
- **Server cert**: `sha256WithRSAEncryption`, issuer — тот же intermediate.
- **broker.properties**: идентичен cluster 2 (отличаются только `__SECRET_N__`).

**CA на обоих кластерах — одинаковый.** Серверная сторона cluster 2 полностью валидна.

## Вывод

Корень проблемы — **на клиенте** (`portal-control-plane` на `1.portal.us-rustore-test-main.rc.aicld.ru`),
не в инфраструктуре MDB Kafka. Серверный cert/CA/chain/config cluster 2 корректны и
верифицируются `openssl s_client` с `Verify return code: 0`.

Самые вероятные причины (проверять у клиента):

1. **В клиентском truststore лежит неправильный CA** — например, от третьего кластера,
   устаревший, или root вместо intermediate. Дима говорит "сертификат одинаковый", но
   `ssl.ca.location` / `ssl.truststore.location` может указывать на другой файл, или
   содержимое файла на этом хосте отличается. Сравнить байты с `/opt/kafka/ssl/tls_ca.crt`
   кластера 2 (fingerprint `67:76:D9:45:...`).
2. **Клиент использует system CA store** — если `ssl.ca.location` пустой и нет
   `ssl.truststore.location`, librdkafka может идти в `/etc/ssl/certs`, где нет
   `one-cloud Infrastructure Vault CA`.
3. **В truststore лежит root CA** (`CN=one-cloud Root Certificate Authority`), подписанный
   алгоритмом, отключённым в OpenSSL клиента (например, SHA1 при `SECLEVEL=3` или
   `OPENSSL_NO_SHA1`). Это прямо объясняет `ASN1_R_UNKNOWN_MESSAGE_DIGEST_ALGORITHM`:
   клиент не может проверить подпись root'а. На брокере в truststore лежит intermediate
   (SHA256) — у клиента может лежать root с legacy alg.
4. **Env vars** `SSL_CERT_DIR`/`SSL_CERT_FILE`/`LDAPTLS_REQCERT` подменяют CA store.
5. **Hostname verification** (`ssl.endpoint.identification.algorithm=https`) — менее
   вероятно: SAN содержит FQDN, и клиент ходит по FQDN. Но если в `bootstrap.servers`
   указан IP или CNAME-алиас — будет ошибка (правда, другой код — `X509_V_ERR_*`).

## Что попросить у клиента

1. Конфиг librdkafka (`ssl.ca.location`, `ssl.truststore.location`, `security.protocol`,
   `sasl.mechanism`, `ssl.endpoint.identification.algorithm`).
2. Содержимое CA-файла/truststore — fingerprint должен совпадать с
   `67:76:D9:45:C7:76:C1:57:5F:E6:6C:85:AB:1D:7F:7C:AB:77:18:72:D0:2C:60:02:B8:C8:10:3B:F7:71:52:55`.
3. Версию librdkafka и OpenSSL на хосте клиента.
4. Вывод `env | grep -E 'SSL|KAFKA'` на хосте клиента.
5. `openssl x509 -in <client_ca_file> -noout -text | grep -E 'Subject|Issuer|Signature Algorithm'`
   — посмотреть, что именно лежит у клиента.

## Грабли

- **Не ведитесь на "сертификат одинаковый"** — пока не сравните байты/fingerprint CA-файла
  на клиенте и на брокере, это предположение. УMDB-кластеров CA сейчас одинаковый
  (`one-cloud Infrastructure Vault CA`), но у клиента может лежать что угодно.
- **`ASN1_R_UNKNOWN_MESSAGE_DIGEST_ALGORITHM` — это про digest alg подписи, не про
  цепочку.** Если видите этот код — ищите в truststore клиента сертификат с необычным alg
  подписи (SHA1, ГОСТ, ECDSA-исторический).
- **`Network is unreachable` на `rc`-брокере** — отдельная проблема (или временная, или
  IPv6/IPv4). Не основная. Серверную верификацию чинить сначала.
- **Свежий server cert на cluster 2 (Jul 27 2026)** — совпадение, не причина: CA не
  менялся, и `openssl s_client` подтверждает валидность chain. Клиенту не нужно ничего
  обновлять при перевыпуске server cert, если CA тот же.
