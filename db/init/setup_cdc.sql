-- Ensure a dedicated replication user for Debezium
DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'debezium') THEN
    CREATE ROLE debezium WITH LOGIN REPLICATION PASSWORD 'debezium';
  END IF;
END$$;

GRANT CONNECT ON DATABASE postgres TO debezium;
GRANT USAGE ON SCHEMA public TO debezium;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO debezium;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO debezium;

-- Publication for Debezium to stream changes
DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_publication WHERE pubname = 'dbz_publication') THEN
    CREATE PUBLICATION dbz_publication FOR ALL TABLES;
  END IF;
END$$;

-- Pre-create a logical replication slot Debezium can reuse
DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name = 'dbz_slot') THEN
    PERFORM pg_create_logical_replication_slot('dbz_slot', 'pgoutput');
  END IF;
END$$;
