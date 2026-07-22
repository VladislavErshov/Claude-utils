# Сборка образа (TODO)

⚠️ **Заглушка.** Заполнится по мере использования скилла.

## Scope

Сборка `ubuntu20-kafka-base` и `ubuntu20-kafka-3.8.0` через CI `docker-images`. Локально через
`docker build` — TBD.

## Файлы

- `ubuntu20-kafka-base-onecloud.dockerfile` — `FROM ubuntu20-mdb-base:stable`, `COPY rootfs /`,
  `RUN docker/build`.
- `ubuntu20-kafka-3.8.0-onecloud.dockerfile` — `ARG BASE_IMAGE=ubuntu20-kafka-base:stable`,
  `ENV KAFKA_VERSION 3.8.0`, `ENV SCALA_VERSION 2.13`.
- `rootfs/docker/build.d/*.sh` — source'ятся из `rootfs/docker/build` в алфавитном порядке.
- `rootfs/docker/build` — `for f in /docker/build.d/*.sh; do . "$f"; done; rm -rf /docker`.

## Что задокументентировать когда понадобится

- Как триггернуть CI-сборку (один образ vs зависимый).
- Где лежит registry (`dzen-external-registry.odkl.ru`).
- Версионирование: stable / latest / tag.
- Локальная сборка для воспроизведения бага.
- Связь с changelog (`rootfs/usr/changelog/changelog.md`).
