from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from fraud_common import execute

CREATE_SQL = """
CREATE TABLE IF NOT EXISTS fraud_daily_summary (
    day             DATE PRIMARY KEY,
    txns            BIGINT,
    flagged_txns    BIGINT,
    flag_rate       DOUBLE PRECISION,
    total_amount    DOUBLE PRECISION,
    critical_alerts INT,
    high_alerts     INT,
    medium_alerts   INT,
    low_alerts      INT,
    open_cases      INT,
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
)
"""

# Recomputed from scratch each run, so reruns are safe (upsert, not append)
SUMMARY_SQL = """
INSERT INTO fraud_daily_summary
    (day, txns, flagged_txns, flag_rate, total_amount,
     critical_alerts, high_alerts, medium_alerts, low_alerts, open_cases)
SELECT s.day, s.txns, s.flagged, s.flagged::float / NULLIF(s.txns, 0), s.amount,
       COALESCE(al.critical, 0), COALESCE(al.high, 0), COALESCE(al.medium, 0),
       COALESCE(al.low, 0), COALESCE(al.open_cases, 0)
FROM (
    SELECT (minute AT TIME ZONE 'UTC')::date AS day,
           sum(txns) AS txns, sum(flagged_txns) AS flagged,
           sum(total_amount) AS amount
    FROM fraud_stats_by_minute
    GROUP BY 1
) s
LEFT JOIN (
    SELECT (f.event_time AT TIME ZONE 'UTC')::date AS day,
           count(*) FILTER (WHERE a.severity = 'CRITICAL') AS critical,
           count(*) FILTER (WHERE a.severity = 'HIGH')     AS high,
           count(*) FILTER (WHERE a.severity = 'MEDIUM')   AS medium,
           count(*) FILTER (WHERE a.severity = 'LOW')      AS low,
           count(*) FILTER (WHERE a.case_status = 'OPEN')  AS open_cases
    FROM alerts a
    JOIN flagged_transactions f USING (transaction_id)
    GROUP BY 1
) al ON al.day = s.day
ON CONFLICT (day) DO UPDATE SET
    txns = EXCLUDED.txns,
    flagged_txns = EXCLUDED.flagged_txns,
    flag_rate = EXCLUDED.flag_rate,
    total_amount = EXCLUDED.total_amount,
    critical_alerts = EXCLUDED.critical_alerts,
    high_alerts = EXCLUDED.high_alerts,
    medium_alerts = EXCLUDED.medium_alerts,
    low_alerts = EXCLUDED.low_alerts,
    open_cases = EXCLUDED.open_cases,
    updated_at = now()
"""

# (name, severity, SQL returning one value, predicate that must be True)
CHECKS = [
    ("flagged_equals_alerts", "error",
     "SELECT (SELECT count(*) FROM flagged_transactions) - (SELECT count(*) FROM alerts)",
     lambda v: v == 0),
    ("stats_match_flagged", "error",
     "SELECT COALESCE((SELECT sum(flagged_txns) FROM fraud_stats_by_minute), 0)"
     " - (SELECT count(*) FROM flagged_transactions)",
     lambda v: v == 0),
    ("risk_score_in_range", "error",
     "SELECT count(*) FROM flagged_transactions WHERE risk_score NOT BETWEEN 1 AND 100",
     lambda v: v == 0),
    ("done_alerts_complete", "error",
     "SELECT count(*) FROM alerts WHERE llm_status = 'DONE'"
     " AND (severity IS NULL OR explanation IS NULL OR suggested_action IS NULL)",
     lambda v: v == 0),
    ("no_stale_pending_alerts", "warn",
     "SELECT count(*) FROM alerts WHERE llm_status = 'PENDING'"
     " AND created_at < now() - interval '1 hour'",
     lambda v: v == 0),
    ("no_failed_alerts", "warn",
     "SELECT count(*) FROM alerts WHERE llm_status = 'FAILED'",
     lambda v: v == 0),
    ("data_age_hours", "warn",
     "SELECT COALESCE(extract(epoch FROM now() - max(event_time)) / 3600, 9999)"
     " FROM flagged_transactions",
     lambda v: v < 24),
]


def create_table(**_):
    execute(CREATE_SQL)


def build_summary(**_):
    n = execute(SUMMARY_SQL)
    print(f"Upserted {n} daily rows")


def run_checks(**_):
    errors = []
    for name, severity, sql, ok in CHECKS:
        value = execute(sql, fetch=True)[0][0]
        passed = ok(value)
        status = "PASS" if passed else ("FAIL" if severity == "error" else "WARN")
        print(f"[{status}] {name}: value={value}")
        if not passed and severity == "error":
            errors.append(name)
    if errors:
        raise ValueError(f"Data quality checks failed: {errors}")


with DAG(
    dag_id="fraud_daily",
    description="Daily summary table and data quality checks",
    start_date=datetime(2026, 10, 1),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=5)},
    tags=["fraud", "quality"],
) as dag:

    t1 = PythonOperator(task_id="create_summary_table", python_callable=create_table)
    t2 = PythonOperator(task_id="build_daily_summary", python_callable=build_summary)
    t3 = PythonOperator(task_id="quality_checks", python_callable=run_checks)

    t1 >> t2 >> t3