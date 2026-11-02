-- ============ Tables ============
CREATE TABLE IF NOT EXISTS flagged_transactions (
    transaction_id    TEXT PRIMARY KEY,
    card_id           TEXT NOT NULL,
    amount            NUMERIC(14,2) NOT NULL,
    currency          TEXT,
    merchant          TEXT,
    merchant_category TEXT,
    city              TEXT,
    latitude          DOUBLE PRECISION,
    longitude         DOUBLE PRECISION,
    device_id         TEXT,
    event_time        TIMESTAMPTZ NOT NULL,
    txn_count_5m      INT,
    txn_count_10s     INT,
    distance_km       DOUBLE PRECISION,
    speed_kmh         DOUBLE PRECISION,
    avg_amount_prior  DOUBLE PRECISION,
    amount_ratio      DOUBLE PRECISION,
    is_new_device     BOOLEAN,
    triggered_rules   TEXT NOT NULL,          -- comma-separated, e.g. 'BURST,IMPOSSIBLE_TRAVEL'
    risk_score        INT NOT NULL,
    scored_at         TIMESTAMPTZ,
    -- Ground-truth labels: for evaluation ONLY. Never send these to the LLM.
    label_is_fraud    BOOLEAN,
    label_fraud_type  TEXT
);

CREATE INDEX IF NOT EXISTS idx_flagged_card ON flagged_transactions (card_id, event_time DESC);
CREATE INDEX IF NOT EXISTS idx_flagged_time ON flagged_transactions (event_time DESC);

CREATE TABLE IF NOT EXISTS alerts (
    alert_id         BIGSERIAL PRIMARY KEY,
    transaction_id   TEXT NOT NULL UNIQUE REFERENCES flagged_transactions (transaction_id),
    llm_status       TEXT NOT NULL DEFAULT 'PENDING'
                     CHECK (llm_status IN ('PENDING', 'DONE', 'FAILED')),
    severity         TEXT,                    -- filled by the LLM service
    explanation      TEXT,
    suggested_action TEXT,
    llm_attempts     INT NOT NULL DEFAULT 0,
    last_error       TEXT,
    case_status      TEXT NOT NULL DEFAULT 'OPEN'
                     CHECK (case_status IN ('OPEN', 'IN_REVIEW', 'CONFIRMED_FRAUD', 'FALSE_POSITIVE')),
    assigned_to      TEXT,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_alerts_pending ON alerts (alert_id) WHERE llm_status = 'PENDING';
CREATE INDEX IF NOT EXISTS idx_alerts_case ON alerts (case_status);

CREATE TABLE IF NOT EXISTS fraud_stats_by_minute (
    minute         TIMESTAMPTZ PRIMARY KEY,
    txns           BIGINT NOT NULL,
    flagged_txns   BIGINT NOT NULL,
    total_amount   DOUBLE PRECISION,
    max_risk_score INT,
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- ============ Ingest views (Spark writes here) ============
CREATE OR REPLACE VIEW ingest_flagged AS SELECT * FROM flagged_transactions;
CREATE OR REPLACE VIEW ingest_stats   AS SELECT * FROM fraud_stats_by_minute;

CREATE OR REPLACE FUNCTION ingest_flagged_fn() RETURNS trigger AS $$
BEGIN
    INSERT INTO flagged_transactions
    SELECT NEW.*
    ON CONFLICT (transaction_id) DO NOTHING;

    INSERT INTO alerts (transaction_id)
    VALUES (NEW.transaction_id)
    ON CONFLICT (transaction_id) DO NOTHING;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION ingest_stats_fn() RETURNS trigger AS $$
BEGIN
    INSERT INTO fraud_stats_by_minute (minute, txns, flagged_txns, total_amount, max_risk_score)
    VALUES (NEW.minute, NEW.txns, NEW.flagged_txns, NEW.total_amount, NEW.max_risk_score)
    ON CONFLICT (minute) DO UPDATE SET
        txns           = EXCLUDED.txns,
        flagged_txns   = EXCLUDED.flagged_txns,
        total_amount   = EXCLUDED.total_amount,
        max_risk_score = EXCLUDED.max_risk_score,
        updated_at     = now();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS ingest_flagged_trg ON ingest_flagged;
CREATE TRIGGER ingest_flagged_trg INSTEAD OF INSERT ON ingest_flagged
    FOR EACH ROW EXECUTE FUNCTION ingest_flagged_fn();

DROP TRIGGER IF EXISTS ingest_stats_trg ON ingest_stats;
CREATE TRIGGER ingest_stats_trg INSTEAD OF INSERT ON ingest_stats
    FOR EACH ROW EXECUTE FUNCTION ingest_stats_fn();