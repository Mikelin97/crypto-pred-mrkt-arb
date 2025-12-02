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

    "include.schema.changes": "false",

    "snapshot.mode": "initial",
    "snapshot.fetch.size": "5000",
    "snapshot.max.threads": "1",

    "max.queue.size": "20480",
    "max.batch.size": "4096",
    "poll.interval.ms": "100",

    "tombstones.on.delete": "false",

    "heartbeat.interval.ms": "10000",

    "time.precision.mode": "connect",

    "errors.tolerance": "all",
    "errors.log.enable": "true",
    "errors.deadletterqueue.topic.name": "postgres-dlq",
    "errors.deadletterqueue.context.headers.enable": "true",

    "topic.creation.default.replication.factor": "1",
    "topic.creation.default.partitions": "3",
    "topic.creation.default.cleanup.policy": "delete",

    "transforms": "unwrap",
    "transforms.unwrap.type": "io.debezium.transforms.ExtractNewRecordState",
    "transforms.unwrap.drop.tombstones": "false",

    "decimal.handling.mode": "double"
  }
}'
