import json
import os
from datetime import datetime, timedelta, timezone

from airflow import DAG
from airflow.operators.python import PythonOperator

DATA = "/opt/airflow/data"
SILVER = f"{DATA}/silver/transactions"
CONFIG = f"{DATA}/config/thresholds.json"

DEFAULTS = {
    "BURST_COUNT": 4,          # txns within 10 seconds
    "TRAVEL_MIN_KM": 100,
    "TRAVEL_MAX_SPEED": 900,   # km/h
    "AMOUNT_MULTIPLIER": 5,    # x the card's own average
    "MIN_PRIOR_TXNS": 3,
}
BURST_GRID = [3, 4, 5, 6, 7, 8]
AMOUNT_GRID = [3, 4, 5, 6, 7, 8, 10]
MIN_GAIN = 0.02        # change a threshold only if F1 improves by at least this
MIN_POSITIVES = 30     # not enough labeled examples -> keep the current value


def _f1(tp, fp, fn):
    p = tp / (tp + fp) if tp + fp else 0.0
    r = tp / (tp + fn) if tp + fn else 0.0
    return 2 * p * r / (p + r) if p + r else 0.0


def _load_current():
    current = dict(DEFAULTS)
    if os.path.exists(CONFIG):
        with open(CONFIG) as f:
            current.update(json.load(f).get("thresholds", {}))
    return current


def _choose(grid, current, score_fn, positives, name, evidence):
    if positives < MIN_POSITIVES:
        print(f"[{name}] only {positives} labeled positives: keeping {current}")
        evidence[name] = {"kept": current, "reason": "too few positives"}
        return current
    scores = {c: score_fn(c) for c in grid}
    cur_score = score_fn(current)
    best = max(scores, key=lambda c: (scores[c], -abs(c - current)))
    chosen = best if scores[best] - cur_score >= MIN_GAIN else current
    print(f"[{name}] current={current} (F1={cur_score:.3f}), best={best} "
          f"(F1={scores[best]:.3f}) -> chosen={chosen}")
    evidence[name] = {"current": current, "chosen": chosen,
                      "f1_by_candidate": {str(k): round(v, 3) for k, v in scores.items()}}
    return chosen


def refresh(**_):
    from deltalake import DeltaTable

    cols = ["txn_count_10s", "prior_txn_count", "amount", "avg_amount_prior",
            "label_fraud_type"]
    data = DeltaTable(SILVER).to_pyarrow_table(columns=cols).to_pydict()
    rows = list(zip(*(data[c] for c in cols)))
    print(f"Evaluating on {len(rows)} silver rows")

    current = _load_current()
    new = dict(current)
    evidence = {}

    # --- BURST_COUNT: a rule fires if txn_count_10s >= k ---
    burst_pos = sum(1 for r in rows if r[4] == "burst")

    def burst_score(k):
        tp = sum(1 for r in rows if r[0] is not None and r[0] >= k and r[4] == "burst")
        fp = sum(1 for r in rows if r[0] is not None and r[0] >= k and r[4] != "burst")
        return _f1(tp, fp, burst_pos - tp)

    new["BURST_COUNT"] = _choose(BURST_GRID, current["BURST_COUNT"], burst_score,
                                 burst_pos, "BURST_COUNT", evidence)

    # --- AMOUNT_MULTIPLIER: fires if amount > m * average of prior amounts ---
    min_prior = current["MIN_PRIOR_TXNS"]
    amount_pos = sum(1 for r in rows if r[4] == "large_amount")

    def amount_score(m):
        def fires(r):
            return (r[1] is not None and r[1] >= min_prior
                    and r[3] is not None and r[2] > m * r[3])
        tp = sum(1 for r in rows if fires(r) and r[4] == "large_amount")
        fp = sum(1 for r in rows if fires(r) and r[4] != "large_amount")
        return _f1(tp, fp, amount_pos - tp)

    new["AMOUNT_MULTIPLIER"] = _choose(AMOUNT_GRID, current["AMOUNT_MULTIPLIER"],
                                       amount_score, amount_pos,
                                       "AMOUNT_MULTIPLIER", evidence)

    os.makedirs(os.path.dirname(CONFIG), exist_ok=True)
    payload = {
        "computed_at": datetime.now(timezone.utc).isoformat(),
        "rows_evaluated": len(rows),
        "thresholds": new,
        "evidence": evidence,
    }
    tmp = CONFIG + ".tmp"
    with open(tmp, "w") as f:
        json.dump(payload, f, indent=2)
    os.replace(tmp, CONFIG)               # atomic: gold never reads a half-written file
    print(f"Wrote {CONFIG}: {new}")


with DAG(
    dag_id="fraud_thresholds",
    description="Tune rule thresholds against labeled history; gold picks them up at restart",
    start_date=datetime(2026, 10, 1),
    schedule="0 4 * * 0",
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=5)},
    tags=["fraud", "thresholds"],
) as dag:
    PythonOperator(task_id="refresh_thresholds", python_callable=refresh)