DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'grafana_ro') THEN
        CREATE ROLE grafana_ro LOGIN PASSWORD 'grafana_ro';
    END IF;
END
$$;

GRANT CONNECT ON DATABASE frauddb TO grafana_ro;
GRANT USAGE ON SCHEMA public TO grafana_ro;
GRANT SELECT ON flagged_transactions, alerts, fraud_stats_by_minute TO grafana_ro;