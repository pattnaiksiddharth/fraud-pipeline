from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from fraud_common import execute

DATA = "/opt/airflow/data"
TABLES = {
    "bronze": f"{DATA}/bronze/transactions",
    "silver": f"{DATA}/silver/transactions",
    "gold": f"{DATA}/gold/scored_transactions",
    "quarantine": f"{DATA}/silver/quarantine",
}
RETENTION_HOURS = 168          # 7 days. Do not lower this while streams are running.


def _open_tables():
    from deltalake import DeltaTable
    for name, path in TABLES.items():
        if DeltaTable.is_deltatable(path):
            yield name, path
        else:
            print(f"[skip] {name}: no Delta table at {path}")


def compact(**_):
    from deltalake import DeltaTable
    for name, path in _open_tables():
        dt = DeltaTable(path)
        before = len(dt.file_uris())
        metrics = dt.optimize.compact()
        after = len(DeltaTable(path).file_uris())
        print(f"[compact] {name}: files {before} -> {after}  metrics={metrics}")


def vacuum(**_):
    from deltalake import DeltaTable
    for name, path in _open_tables():
        removed = DeltaTable(path).vacuum(
            retention_hours=RETENTION_HOURS,
            dry_run=False,
            enforce_retention_duration=True,
        )
        print(f"[vacuum] {name}: deleted {len(removed)} unreferenced files")

def _row_count(path):
    import pyarrow as pa
    from deltalake import DeltaTable
    actions = pa.table(DeltaTable(path).get_add_actions(flatten=True))
    return sum(v or 0 for v in actions.column("num_records").to_pylist())


def check_layers(**_):
    counts = {name: _row_count(path) for name, path in _open_tables()}
    print("Row counts:", counts)

    errors, warnings = [], []
    bronze = counts.get("bronze", 0)
    silver = counts.get("silver", 0)
    gold = counts.get("gold", 0)
    quarantine = counts.get("quarantine", 0)

    # A downstream layer can never hold more rows than its source
    if silver > bronze:
        errors.append(f"silver ({silver}) > bronze ({bronze})")
    if gold > silver:
        errors.append(f"gold ({gold}) > silver ({silver})")

    # Lag or duplicates show up as a gap: warn, do not fail
    if bronze - silver - quarantine != 0:
        warnings.append(f"bronze - silver - quarantine = {bronze - silver - quarantine} "
                        f"(stream lag or deduplicated rows)")
    if silver != gold:
        warnings.append(f"silver ({silver}) != gold ({gold}): scoring is lagging")

    # Gold vs the serving layer (per-minute stats are computed from gold)
    stats_txns = execute(
        "SELECT COALESCE(sum(txns), 0) FROM fraud_stats_by_minute", fetch=True)[0][0]
    print(f"Gold rows={gold}, postgres stats txns={stats_txns}")
    if gold != stats_txns:
        warnings.append(f"gold ({gold}) != postgres stats ({stats_txns}): sink is lagging")

    for w in warnings:
        print(f"[WARN] {w}")
    if errors:
        raise ValueError(f"Layer checks failed: {errors}")
    print("[PASS] layer checks" if not warnings else "[PASS with warnings] layer checks")

with DAG(
    dag_id="fraud_delta_maintenance",
    description="Compact, vacuum and reconcile the bronze/silver/gold Delta tables",
    start_date=datetime(2026, 10, 1),
    schedule="0 3 * * *",
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=5)},
    tags=["fraud", "delta"],
) as dag:
    t1 = PythonOperator(task_id="compact_tables", python_callable=compact)
    t2 = PythonOperator(task_id="vacuum_tables", python_callable=vacuum)
    t3 = PythonOperator(task_id="check_layers", python_callable=check_layers)
    t1 >> t2 >> t3