---
date: 2026-08-21
ticket: MDBSUP-4737
related_ticket: MDBSUP-4649
cluster: auction-realtime-adtech-kafka
hosts:
  - 1.broker.auction-realtime-adtech-kafka.hc.one-infra.ru   # 20001
  - 2.broker.auction-realtime-adtech-kafka.hc.one-infra.ru   # 20002
  - 1.broker.auction-realtime-adtech-kafka.pc.one-infra.ru   # 22001
  - 2.broker.auction-realtime-adtech-kafka.pc.one-infra.ru
  - 1.broker.auction-realtime-adtech-kafka.uc.one-infra.ru   # 23001
  - 2.broker.auction-realtime-adtech-kafka.uc.one-infra.ru   # 23002
kafka_version: 3.8.0 (KRaft)
resolution: fixed  # num.replica.fetchers 1→4 помог частично, 8 — полностью
---

# Follower lag растёт «пилой» — num.replica.fetchers=4 не хватило из-за хэш-перекоса

## Симптомы

- На брокерах follower lag (Broker Max Lag) растёт, периодически резко падает, снова растёт —
  пилообразный график с периодом ≈ retention-интервалу.
- Хронические выпадения реплик из ISR (UnderReplicatedPartitions > 0) на жирном кластере
  (топик bannerd_potential_info, 3–4 Гбит/с записи).
- Ранее (MDBSUP-4649-кейс) диагностировано: num.replica.fetchers=1 — один fetcher-тред
  не тянет. Подняли до 4 — лаг ушёл не везде.

## Окружение

- Мульти-ДЦ кластер (hc/pc/uc/kc), брокеры перезапускаются волной при применении конфига.
- Конфиг живёт в `/opt/kafka/config/broker.properties` (НЕ server.properties).
- ⚠️ Похожие имена: `auction-realtime-spb-adtech-kafka` (ic/nc/zc) — **другой кластер**,
  не путать при поиске хостов.

## Диагностика

### 1. Конфиг применился — проверить трижды

```bash
# файл (broker.properties, не server.properties!)
grep num.replica.fetchers /opt/kafka/config/broker.properties

# dynamic config: synonyms показывают источник
kafka-configs.sh --bootstrap-server $(hostname -f):9092 --command-config ... \
  --describe --entity-type brokers --all | grep num.replica.fetchers
# → num.replica.fetchers=4 synonyms={STATIC_BROKER_CONFIG:...=4, DEFAULT_CONFIG:...=1}

# живые треды в JVM: 4 fetchers × N источников = 4N тредов
P=$(systemctl show -p MainPID --value kafka-broker.service)
cat /proc/$P/task/*/comm | grep -c ReplicaFetcher
```

Рестарты видны по `systemctl show kafka-broker -p ActiveEnterTimestamp`.

### 2. Кто именно лагает: per-partition + per-thread

```bash
curl -s --max-time 15 localhost:8080/metrics \
  | grep '^kafka_server_fetcherlagmetrics_consumerlag' | grep -v ' 0.0$' | sort -k2 -nr
# имя метрики: ..._clientid_replicafetcherthread_<T>_<SOURCE_BROKER_ID>{partition,topic}
```

Ключ к кейсу — **распределение партиций по тредам от одного источника**:

```bash
curl -s --max-time 20 localhost:8080/metrics \
  | grep "^kafka_server_fetcherlagmetrics_consumerlag_clientid_replicafetcherthread_._22001" \
  | awk '{split($1,a,"replicafetcherthread_"); split(a[2],b,"_"); t=b[1]; cnt[t]++; lag[t]+=$2}
        END {for (t in cnt) printf "thread_%s: partitions=%d sumlag=%.0f\n", t, cnt[t], lag[t]}'
```

Результат на лагающем брокере (23002, источник 22001):
`thread_0: partitions=6 sumlag=3.9M; thread_1: 2 / 0; thread_2: 3 / 0; thread_3: 2 / 0` —
**все 5 тяжёлых партиций в thread_0**, остальные треды idle (CPU по `ps -L -p $PID -o lwp,pcpu,comm`).

### 3. «Пила» = retention срезает хвост

В `/mnt/logs/dbms/kafka-broker.out.log` каждые ~5 мин (интервал проверки retention):

```
[ReplicaFetcherThread-0-22001] Current offset ... for partition bannerd_potential_info-10
is out of range, which typically implies a leader change. Reset fetch offset to ...
```

Это не leader change — это догоняющая реплика отстала дальше log start offset, лидер
отвечает OUT_OF_RANGE, фетчер прыгает на актуальный офсет. Лаг на графике падает в ноль
и снова растёт.

## Корневая причина

Kafka распределяет партиции по ReplicaFetcher-тредам **хэшем, per-source-broker, без учёта
нагрузки**. Если от одного брокера-лидера все тяжёлые партиции захэшировались в один тред —
это один TCP-стрим на все жирные партиции, и рост fetchers не помогает, пока тред один.
На 23002 так и случилось: 4 треда от 22001, но bannerd_potential_info p6/10/22/34 +
bidder2d p5 — все в thread_0 (суммарный лаг рос ~130K msg/мин). Соседний 23001 с ровным
распределением (20/24/11/13) полностью догнал.

## Фикс

`num.replica.fetchers=8` через настройки брокера в UI MDB (оркестратор сам рендерит
broker.properties и перезапускает волной). Партиции перехэшируются по 8 тредам на источник,
тяжёлые разъезжаются по разным стримам — лаг ушёл в 0.

## Ключевые уроки

1. **num.replica.fetchers хэширует без балансировки** — рост параметра помогает
   вероятностно: при неудачном хэше все тяжёлые партиции от одного лидера остаются в одном
   треде. Для жирных кластеров (Гбит/с) закладывать запас: 8 тредов хватило там, где 4 — нет.
2. **Пилообразный follower lag с периодом ~5 мин = retention-срез**: маркер в логе —
   `out of range ... Reset fetch offset` на одних и тех же партициях с ровным интервалом.
3. **Диагноз ставится per-thread распределением**: `fetcherlagmetrics_consumerlag` по
   `replicafetcherthread_<T>_<SRC>` — если весь лаг в одном треде, а остальные idle →
   хэш-перекос, лечится увеличением fetchers (перехэш при рестарте).
4. **Конфиг MDB Kafka — `broker.properties`**, не server.properties. Проверять и файл,
   и dynamic config synonyms (STATIC_BROKER_CONFIG vs DEFAULT_CONFIG), и живые треды в JVM.
5. **Мульти-ДЦ = один cloud-master на ДЦ**: `mcc instances` без `-c <dc>` покажет только
   свой ДЦ; перебирать `-c hc uc pc kc` и смотреть FQDN внимательно (adc/spb-префиксы —
   могут быть разными кластерами с похожими именами).
