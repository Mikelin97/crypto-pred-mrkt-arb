#!/bin/bash

set -e

curl -X POST http://localhost:18083/connectors \
-H 'Content-Type: application/json' \
-d '{
  "name": "pg-cdc",
  "config": {
    "connector.class": "io.debezium.connector.postgresql.PostgresConnector",

    "database.hostname": "persistence_postgres",
    "database.port": "5432",
    "database.user": "debezium",
    "database.password": "debezium",
    "database.dbname": "postgres",

    "database.server.name": "postgres",
    "topic.prefix": "postgres",

    "publication.name": "dbz_publication",
    "slot.name": "dbz_slot",
    "plugin.name": "pgoutput",

    "slot.drop.on.stop": "false",
    "poll.interval.ms": "500",
    "max.batch.size": "8192",
    "max.queue.size": "65536",

    "include.schema.changes": "false",

    "snapshot.mode": "initial",
    "snapshot.fetch.size": "5000",
    "snapshot.max.threads": "1",

    "tombstones.on.delete": "false",

    "heartbeat.interval.ms": "10000",

    "time.precision.mode": "connect",

    "errors.tolerance": "all",
    "errors.log.enable": "true",
    "errors.deadletterqueue.topic.name": "postgres-dlq",
    "errors.deadletterqueue.context.headers.enable": "true",

    "transforms": "unwrap",
    "transforms.unwrap.type": "io.debezium.transforms.ExtractNewRecordState",
    "transforms.unwrap.drop.tombstones": "false",
    "transforms.unwrap.delete.handling.mode": "rewrite",
    "transforms.unwrap.add.fields": "op,ts_ms,source.ts_ms,source.lsn",
    "transforms.unwrap.add.fields.prefix": "__",

    "decimal.handling.mode": "double"
  }
}'
