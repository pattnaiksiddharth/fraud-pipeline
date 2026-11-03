DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'analyst_app') THEN
        CREATE ROLE analyst_app LOGIN PASSWORD 'siddharth@123';
    END IF;
END
$$;

GRANT CONNECT ON DATABASE frauddb TO analyst_app;
GRANT USAGE ON SCHEMA public TO analyst_app;

-- Column-level SELECT: the simulator labels are deliberately left out
GRANT SELECT (transaction_id, card_id, amount, currency, merchant, merchant_category,
              city, latitude, longitude, device_id, event_time, txn_count_5m,
              txn_count_10s, distance_km, speed_kmh, avg_amount_prior, amount_ratio,
              is_new_device, triggered_rules, risk_score, scored_at)
    ON flagged_transactions TO analyst_app;

GRANT SELECT ON alerts, fraud_stats_by_minute TO analyst_app;
GRANT UPDATE (case_status, assigned_to, updated_at) ON alerts TO analyst_app;