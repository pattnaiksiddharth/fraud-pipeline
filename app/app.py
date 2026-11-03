"""
Fraud Analyst Console (Streamlit + PostgreSQL 16)
Rules detect, the LLM explains, analysts decide.
All timestamps are displayed in UTC.
"""

import os

import pandas as pd
import psycopg2
import streamlit as st
from packaging.version import Version
from psycopg2.extras import RealDictCursor

# ---------------------------------------------------------------------------
# Page configuration and version check
# ---------------------------------------------------------------------------
st.set_page_config(page_title="Fraud Analyst Console", layout="wide")

MIN_STREAMLIT_VERSION = "1.37.0"
HAS_FRAGMENT = hasattr(st, "fragment") and Version(st.__version__) >= Version(MIN_STREAMLIT_VERSION)

TIME_WINDOWS = {
    "Last 15 min": "15 minutes",
    "Last 1 hour": "1 hour",
    "Last 24 hours": "24 hours",
    "Last 7 days": "7 days",
    "All": None,
}

SEVERITY_OPTIONS = ["CRITICAL", "HIGH", "MEDIUM", "LOW"]
CASE_STATUS_OPTIONS = ["OPEN", "IN_REVIEW", "CONFIRMED_FRAUD", "FALSE_POSITIVE"]
RULE_FALLBACK = ["BURST", "IMPOSSIBLE_TRAVEL", "LARGE_AMOUNT"]


# ---------------------------------------------------------------------------
# Database connection (one cached connection, reconnects if it drops)
# ---------------------------------------------------------------------------
@st.cache_resource
def _init_connection_holder():
    """Holds a mutable single-connection reference across Streamlit reruns."""
    return {"conn": None}


def _connect():
    host = os.environ.get("PGHOST", "127.0.0.1")
    port = os.environ.get("PGPORT", "5433")
    dbname = os.environ.get("PGDATABASE", "frauddb")
    user = os.environ.get("APP_DB_USER")
    password = os.environ.get("APP_DB_PASSWORD")

    if not user or not password:
        raise RuntimeError(
            "Database credentials missing. Set PGHOST, PGPORT, PGDATABASE, "
            "APP_DB_USER and APP_DB_PASSWORD."
        )

    conn = psycopg2.connect(
        host=host,
        port=port,
        dbname=dbname,
        user=user,
        password=password,
        connect_timeout=5,
    )
    conn.autocommit = True
    return conn


def get_connection():
    """Returns the cached connection, reconnecting transparently if it was dropped."""
    holder = _init_connection_holder()
    conn = holder.get("conn")

    if conn is not None and conn.closed == 0:
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT 1;")
            return conn
        except psycopg2.Error:
            try:
                conn.close()
            except Exception:
                pass

    conn = _connect()
    holder["conn"] = conn
    return conn


def run_query(sql: str, params: tuple = ()):
    """Runs a parameterized read query and returns a list of dicts."""
    conn = get_connection()
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(sql, params)
        return [dict(row) for row in cur.fetchall()]


def format_utc(series: pd.Series) -> pd.Series:
    """Formats a timestamp series explicitly in UTC."""
    if series.empty:
        return series
    return pd.to_datetime(series, utc=True).dt.strftime("%Y-%m-%d %H:%M:%S UTC")


def fmt_num(value, suffix: str = "", digits: int = 2) -> str:
    """Formats a number, showing n/a when the feature is missing (e.g. a card's first transaction)."""
    if value is None:
        return "n/a"
    return f"{float(value):,.{digits}f}{suffix}"


# ---------------------------------------------------------------------------
# Cached read queries (TTL 5 s). Ground-truth label columns are never selected.
# ---------------------------------------------------------------------------
@st.cache_data(ttl=5, show_spinner=False)
def fetch_kpis():
    sql = """
        SELECT
            COUNT(*) FILTER (WHERE case_status = 'OPEN') AS open_alerts,
            COUNT(*) FILTER (WHERE case_status = 'IN_REVIEW') AS in_review,
            COUNT(*) FILTER (WHERE case_status = 'CONFIRMED_FRAUD') AS confirmed_fraud,
            COUNT(*) FILTER (WHERE case_status = 'FALSE_POSITIVE') AS false_positives,
            COUNT(*) FILTER (WHERE llm_status IN ('PENDING', 'FAILED')) AS llm_pending_or_failed
        FROM public.alerts;
    """
    rows = run_query(sql)
    return rows[0] if rows else {
        "open_alerts": 0,
        "in_review": 0,
        "confirmed_fraud": 0,
        "false_positives": 0,
        "llm_pending_or_failed": 0,
    }


@st.cache_data(ttl=5, show_spinner=False)
def fetch_distinct_rules():
    sql = """
        SELECT DISTINCT trim(rule_token) AS rule_name
        FROM public.flagged_transactions ft,
             unnest(string_to_array(ft.triggered_rules, ',')) AS rule_token
        WHERE ft.triggered_rules IS NOT NULL AND trim(rule_token) <> ''
        ORDER BY rule_name;
    """
    rows = run_query(sql)
    return [r["rule_name"] for r in rows]


@st.cache_data(ttl=5, show_spinner=False)
def fetch_alert_queue(
    severities: tuple,
    case_statuses: tuple,
    rules: tuple,
    interval_str,
    card_search: str,
):
    # Columns are listed explicitly; label_is_fraud / label_fraud_type are never selected.
    sql = """
        SELECT
            a.alert_id,
            ft.transaction_id,
            ft.event_time,
            ft.card_id,
            ft.amount,
            ft.currency,
            ft.merchant,
            ft.city,
            ft.triggered_rules,
            ft.risk_score,
            a.severity,
            a.case_status,
            a.llm_status
        FROM public.alerts a
        JOIN public.flagged_transactions ft
          ON ft.transaction_id = a.transaction_id
        WHERE (%s = 0 OR a.severity = ANY(%s))
          AND (%s = 0 OR a.case_status = ANY(%s))
          AND (%s = 0 OR string_to_array(ft.triggered_rules, ',') && %s)
          AND (%s IS NULL OR ft.event_time >= now() - %s::interval)
          AND (%s = '' OR ft.card_id ILIKE %s)
        ORDER BY
            CASE a.severity
                WHEN 'CRITICAL' THEN 1
                WHEN 'HIGH' THEN 2
                WHEN 'MEDIUM' THEN 3
                WHEN 'LOW' THEN 4
                ELSE 5
            END ASC,
            ft.risk_score DESC,
            ft.event_time DESC
        LIMIT 200;
    """
    sev_list = list(severities)
    status_list = list(case_statuses)
    rule_list = list(rules)
    card_pattern = f"%{card_search}%" if card_search else ""

    params = (
        len(sev_list),
        sev_list,
        len(status_list),
        status_list,
        len(rule_list),
        rule_list,
        interval_str,
        interval_str,
        card_search,
        card_pattern,
    )
    return run_query(sql, params)


@st.cache_data(ttl=5, show_spinner=False)
def fetch_alert_detail(alert_id: int):
    sql = """
        SELECT
            a.alert_id,
            a.transaction_id,
            a.llm_status,
            a.severity,
            a.explanation,
            a.suggested_action,
            a.llm_attempts,
            a.last_error,
            a.case_status,
            a.assigned_to,
            a.created_at,
            a.updated_at,
            ft.card_id,
            ft.amount,
            ft.currency,
            ft.merchant,
            ft.merchant_category,
            ft.city,
            ft.latitude,
            ft.longitude,
            ft.device_id,
            ft.event_time,
            ft.txn_count_5m,
            ft.txn_count_10s,
            ft.distance_km,
            ft.speed_kmh,
            ft.avg_amount_prior,
            ft.amount_ratio,
            ft.is_new_device,
            ft.triggered_rules,
            ft.risk_score,
            ft.scored_at
        FROM public.alerts a
        JOIN public.flagged_transactions ft
          ON ft.transaction_id = a.transaction_id
        WHERE a.alert_id = %s;
    """
    rows = run_query(sql, (alert_id,))
    return rows[0] if rows else None


@st.cache_data(ttl=5, show_spinner=False)
def fetch_card_history(card_id: str):
    sql = """
        SELECT
            ft.event_time,
            ft.transaction_id,
            ft.amount,
            ft.currency,
            ft.merchant,
            ft.merchant_category,
            ft.city,
            ft.triggered_rules,
            ft.risk_score,
            ft.device_id,
            ft.is_new_device
        FROM public.flagged_transactions ft
        WHERE ft.card_id = %s
        ORDER BY ft.event_time DESC
        LIMIT 10;
    """
    return run_query(sql, (card_id,))


@st.cache_data(ttl=5, show_spinner=False)
def fetch_activity_data(interval_str):
    sev_sql = """
        SELECT
            COALESCE(a.severity, 'UNSCORED') AS severity,
            COUNT(*) AS alert_count
        FROM public.alerts a
        JOIN public.flagged_transactions ft
          ON ft.transaction_id = a.transaction_id
        WHERE (%s IS NULL OR ft.event_time >= now() - %s::interval)
        GROUP BY COALESCE(a.severity, 'UNSCORED')
        ORDER BY
            CASE COALESCE(a.severity, 'UNSCORED')
                WHEN 'CRITICAL' THEN 1
                WHEN 'HIGH' THEN 2
                WHEN 'MEDIUM' THEN 3
                WHEN 'LOW' THEN 4
                ELSE 5
            END;
    """
    stats_sql = """
        SELECT minute, txns, flagged_txns, total_amount, max_risk_score
        FROM public.fraud_stats_by_minute
        WHERE (%s IS NULL OR minute >= now() - %s::interval)
        ORDER BY minute ASC;
    """
    merchants_sql = """
        SELECT
            merchant,
            COUNT(*) AS flagged_count,
            ROUND(SUM(amount)::numeric, 2) AS total_flagged_amount
        FROM public.flagged_transactions
        WHERE (%s IS NULL OR event_time >= now() - %s::interval)
        GROUP BY merchant
        ORDER BY flagged_count DESC, total_flagged_amount DESC
        LIMIT 10;
    """
    cities_sql = """
        SELECT
            city,
            COUNT(*) AS flagged_count,
            ROUND(SUM(amount)::numeric, 2) AS total_flagged_amount
        FROM public.flagged_transactions
        WHERE (%s IS NULL OR event_time >= now() - %s::interval)
        GROUP BY city
        ORDER BY flagged_count DESC, total_flagged_amount DESC
        LIMIT 10;
    """
    params = (interval_str, interval_str)
    return (
        run_query(sev_sql, params),
        run_query(stats_sql, params),
        run_query(merchants_sql, params),
        run_query(cities_sql, params),
    )


# ---------------------------------------------------------------------------
# Uncached write (guarded by an optimistic concurrency check)
# ---------------------------------------------------------------------------
def update_case_decision(
    alert_id: int,
    expected_status: str,
    new_status: str,
    assigned_to: str,
) -> int:
    """
    Updates case_status, assigned_to and updated_at only if case_status still
    matches the status the analyst saw. Returns 1 on success, 0 on conflict.
    """
    sql = """
        UPDATE public.alerts
        SET case_status = %s,
            assigned_to = %s,
            updated_at = now()
        WHERE alert_id = %s
          AND case_status = %s;
    """
    conn = get_connection()
    with conn.cursor() as cur:
        cur.execute(sql, (new_status, assigned_to.strip() or None, alert_id, expected_status))
        return cur.rowcount


# ---------------------------------------------------------------------------
# Session state
# ---------------------------------------------------------------------------
if "selected_alert_id" not in st.session_state:
    st.session_state["selected_alert_id"] = None
if "analyst_name" not in st.session_state:
    st.session_state["analyst_name"] = ""
if "decision_feedback" not in st.session_state:
    st.session_state["decision_feedback"] = None
if "last_alert_ids" not in st.session_state:
    st.session_state["last_alert_ids"] = None


# ---------------------------------------------------------------------------
# Header
# ---------------------------------------------------------------------------
header_left, header_right = st.columns([4, 1])
with header_left:
    st.title("Fraud Analyst Console")
    st.caption("Rules detect, the LLM explains, analysts decide · All timestamps shown in UTC")
with header_right:
    if st.button("Refresh now", use_container_width=True):
        st.cache_data.clear()
        st.rerun()

if not HAS_FRAGMENT:
    st.warning(
        f"Installed Streamlit ({st.__version__}) is older than {MIN_STREAMLIT_VERSION}. "
        f"Upgrade to streamlit>={MIN_STREAMLIT_VERSION} for 10 s auto-refresh."
    )


# ---------------------------------------------------------------------------
# Sidebar filters
# ---------------------------------------------------------------------------
with st.sidebar:
    st.header("Queue Filters")
    try:
        available_rules = fetch_distinct_rules() or RULE_FALLBACK
    except Exception:
        available_rules = RULE_FALLBACK

    selected_severities = st.multiselect(
        "Severity",
        options=SEVERITY_OPTIONS,
        default=[],
        help="Leave empty to include all severities (including alerts not yet scored by the LLM).",
    )
    selected_statuses = st.multiselect(
        "Case Status",
        options=CASE_STATUS_OPTIONS,
        default=["OPEN", "IN_REVIEW"],
    )
    selected_rules = st.multiselect(
        "Triggered Rule",
        options=available_rules,
        default=[],
    )
    selected_window_label = st.selectbox(
        "Time Window (UTC)",
        options=list(TIME_WINDOWS.keys()),
        index=2,  # default: last 24 hours. Pick "All" to see older simulated data.
    )
    card_id_search = st.text_input("Card ID Search", placeholder="e.g. card-00011").strip()

selected_interval = TIME_WINDOWS[selected_window_label]


# ---------------------------------------------------------------------------
# Live fragment: KPIs and alert queue (refreshes every 10 s)
# ---------------------------------------------------------------------------
def _render_kpis_and_queue():
    try:
        kpis = fetch_kpis()
        k1, k2, k3, k4, k5 = st.columns(5)
        k1.metric("Open Alerts", int(kpis.get("open_alerts", 0)))
        k2.metric("In Review", int(kpis.get("in_review", 0)))
        k3.metric("Confirmed Fraud", int(kpis.get("confirmed_fraud", 0)))
        k4.metric("False Positives", int(kpis.get("false_positives", 0)))
        k5.metric("LLM Pending / Failed", int(kpis.get("llm_pending_or_failed", 0)))
    except Exception as exc:
        st.error(f"Unable to load KPI metrics from PostgreSQL: {exc}")
        return

    st.subheader("Alert Queue (max 200 rows · sorted by severity, risk score, event time)")

    try:
        rows = fetch_alert_queue(
            tuple(selected_severities),
            tuple(selected_statuses),
            tuple(selected_rules),
            selected_interval,
            card_id_search,
        )
    except Exception as exc:
        st.error(f"Unable to query alert queue: {exc}")
        return

    if not rows:
        st.session_state["last_alert_ids"] = None
        st.info("No alerts match the current filter criteria.")
        return

    df = pd.DataFrame(rows)
    df["event_time_utc"] = format_utc(df["event_time"])
    df["amount_display"] = df.apply(
        lambda r: f"{float(r['amount']):,.2f} {r['currency']}", axis=1
    )
    df["severity_display"] = df["severity"].fillna("not scored yet")

    display_df = df[
        [
            "event_time_utc",
            "card_id",
            "amount_display",
            "merchant",
            "city",
            "triggered_rules",
            "risk_score",
            "severity_display",
            "case_status",
        ]
    ].rename(
        columns={
            "event_time_utc": "event_time (UTC)",
            "amount_display": "amount + currency",
            "severity_display": "severity",
        }
    )

    alert_ids = df["alert_id"].tolist()

    event = st.dataframe(
        display_df,
        use_container_width=True,
        hide_index=True,
        on_select="rerun",
        selection_mode="single-row",
        key="alert_queue_table",
    )

    selected_rows = event.selection.rows if event and hasattr(event, "selection") else []

    # A selection is a row INDEX. Only trust it if the list is unchanged since the
    # previous render; otherwise a refresh that reorders rows could silently move
    # the selection to a different alert.
    prev_ids = st.session_state.get("last_alert_ids")
    st.session_state["last_alert_ids"] = alert_ids

    if selected_rows and prev_ids == alert_ids:
        idx = selected_rows[0]
        if 0 <= idx < len(alert_ids):
            new_id = int(alert_ids[idx])
            if st.session_state["selected_alert_id"] != new_id:
                st.session_state["selected_alert_id"] = new_id
                st.session_state["decision_feedback"] = None
                # Full-app rerun, so the detail panel (outside the fragment) updates.
                st.rerun()


if HAS_FRAGMENT:
    live_queue_fragment = st.fragment(run_every="10s")(_render_kpis_and_queue)
else:
    live_queue_fragment = _render_kpis_and_queue


# ---------------------------------------------------------------------------
# Detail panel
# ---------------------------------------------------------------------------
def render_alert_detail_panel():
    alert_id = st.session_state.get("selected_alert_id")
    st.divider()
    st.subheader("Alert Detail Panel")

    if not alert_id:
        st.info("Select a row in the Alert Queue above to inspect the transaction and record a case decision.")
        return

    try:
        detail = fetch_alert_detail(int(alert_id))
    except Exception as exc:
        st.error(f"Unable to load alert #{alert_id}: {exc}")
        return

    if not detail:
        st.warning(f"Alert #{alert_id} was not found.")
        return

    seen_status = detail["case_status"]
    event_time_utc = pd.to_datetime(detail["event_time"], utc=True).strftime("%Y-%m-%d %H:%M:%S UTC")
    updated_at_utc = pd.to_datetime(detail["updated_at"], utc=True).strftime("%Y-%m-%d %H:%M:%S UTC")

    st.markdown(
        f"**Alert ID:** `{detail['alert_id']}` · "
        f"**Transaction ID:** `{detail['transaction_id']}` · "
        f"**Event Time:** `{event_time_utc}` · "
        f"**Case Status:** `{seen_status}` · "
        f"**Assigned To:** `{detail.get('assigned_to') or 'Unassigned'}` · "
        f"**Last Updated:** `{updated_at_utc}`"
    )

    # 1. LLM explanation and suggested action
    st.markdown("#### LLM Triage Assessment")
    llm_status = detail.get("llm_status")
    if llm_status == "PENDING":
        st.info(
            "LLM analysis is PENDING for this alert. Severity, explanation and "
            "suggested action will appear once it completes."
        )
    elif llm_status == "FAILED":
        st.error(
            f"LLM analysis FAILED after {detail.get('llm_attempts', 0)} attempt(s). "
            f"Last error: {detail.get('last_error') or 'Unknown error'}"
        )
    else:
        llm_col1, llm_col2 = st.columns(2)
        with llm_col1:
            st.markdown(f"**Severity:** `{detail.get('severity') or 'n/a'}`")
            st.markdown("**Explanation**")
            st.write(detail.get("explanation") or "No explanation provided.")
        with llm_col2:
            st.markdown("**Suggested Action**")
            st.write(detail.get("suggested_action") or "No suggested action provided.")

    # 2. Transaction facts and features
    st.markdown("#### Transaction Facts and Risk Features")
    fact_col1, fact_col2 = st.columns(2)
    with fact_col1:
        st.markdown(
            f"- **Amount:** `{float(detail['amount']):,.2f} {detail['currency']}`\n"
            f"- **Merchant:** `{detail['merchant']}`\n"
            f"- **Category:** `{detail['merchant_category']}`\n"
            f"- **City:** `{detail['city']}`\n"
            f"- **Device ID:** `{detail['device_id']}`\n"
            f"- **Is New Device:** `{detail['is_new_device']}`\n"
            f"- **Triggered Rules:** `{detail['triggered_rules']}` (risk {detail['risk_score']})"
        )
    with fact_col2:
        st.markdown(
            f"- **Txn Count (10 s):** `{detail['txn_count_10s']}`\n"
            f"- **Txn Count (5 min):** `{detail['txn_count_5m']}`\n"
            f"- **Distance (km):** `{fmt_num(detail['distance_km'])}`\n"
            f"- **Speed (km/h):** `{fmt_num(detail['speed_kmh'])}`\n"
            f"- **Amount Ratio:** `{fmt_num(detail['amount_ratio'], 'x')}`\n"
            f"- **Avg Amount Prior:** `{fmt_num(detail['avg_amount_prior'])}`"
        )

    # 3. Case decision (optimistic concurrency guarded)
    st.markdown("#### Case Decision")
    st.text_input(
        "Analyst Name (saved to assigned_to)",
        key="analyst_name",
        placeholder="e.g. j.doe",
    )

    feedback = st.session_state.get("decision_feedback")
    if feedback:
        level, msg = feedback
        if level == "conflict":
            st.warning(msg)
        elif level == "error":
            st.error(msg)
        else:
            st.success(msg)

    b1, b2, b3, b4 = st.columns(4)
    decisions = [
        (b1, "Start review", "IN_REVIEW"),
        (b2, "Confirm fraud", "CONFIRMED_FRAUD"),
        (b3, "Mark false positive", "FALSE_POSITIVE"),
        (b4, "Reopen", "OPEN"),
    ]

    for col, label, target_status in decisions:
        if col.button(label, key=f"btn_{target_status}_{alert_id}", use_container_width=True):
            try:
                updated_rows = update_case_decision(
                    alert_id=int(alert_id),
                    expected_status=seen_status,
                    new_status=target_status,
                    assigned_to=st.session_state.get("analyst_name", ""),
                )
                st.cache_data.clear()
                if updated_rows == 0:
                    st.session_state["decision_feedback"] = (
                        "conflict",
                        "This alert was changed by someone else. Refresh.",
                    )
                else:
                    st.session_state["decision_feedback"] = (
                        "success",
                        f"Alert #{alert_id} updated to {target_status}.",
                    )
            except Exception as exc:
                st.session_state["decision_feedback"] = (
                    "error",
                    f"Database error while saving decision: {exc}",
                )
            # Always rerun so the message (success, conflict or error) is displayed.
            st.rerun()

    # 4. The card's last 10 flagged transactions
    st.markdown(f"#### Last 10 Flagged Transactions for Card `{detail['card_id']}` (UTC)")
    try:
        history_rows = fetch_card_history(detail["card_id"])
        if history_rows:
            hist_df = pd.DataFrame(history_rows)
            hist_df["event_time (UTC)"] = format_utc(hist_df["event_time"])
            hist_df["amount + currency"] = hist_df.apply(
                lambda r: f"{float(r['amount']):,.2f} {r['currency']}", axis=1
            )
            st.dataframe(
                hist_df[
                    [
                        "event_time (UTC)",
                        "transaction_id",
                        "amount + currency",
                        "merchant",
                        "merchant_category",
                        "city",
                        "triggered_rules",
                        "risk_score",
                        "device_id",
                        "is_new_device",
                    ]
                ],
                use_container_width=True,
                hide_index=True,
            )
        else:
            st.info("No prior flagged transactions found for this card.")
    except Exception as exc:
        st.error(f"Unable to load card history: {exc}")


# ---------------------------------------------------------------------------
# Activity tab
# ---------------------------------------------------------------------------
def render_activity_tab():
    st.subheader(f"Pipeline Activity · Window: {selected_window_label} (UTC)")
    try:
        sev_rows, stats_rows, merchant_rows, city_rows = fetch_activity_data(selected_interval)
    except Exception as exc:
        st.error(f"Unable to load activity analytics: {exc}")
        return

    chart_col1, chart_col2 = st.columns(2)

    with chart_col1:
        st.markdown("#### Alerts by Severity")
        if sev_rows:
            sev_df = pd.DataFrame(sev_rows).set_index("severity")
            st.bar_chart(sev_df["alert_count"])
        else:
            st.info("No alert data for the selected time window.")

    with chart_col2:
        st.markdown("#### Flagged vs Total Transactions per Minute (UTC)")
        if stats_rows:
            stats_df = pd.DataFrame(stats_rows)
            stats_df["minute_utc"] = pd.to_datetime(stats_df["minute"], utc=True)
            stats_df = stats_df.set_index("minute_utc")[["txns", "flagged_txns"]]
            st.line_chart(stats_df)
        else:
            st.info("No per-minute statistics for the selected time window.")

    table_col1, table_col2 = st.columns(2)
    with table_col1:
        st.markdown("#### Top 10 Merchants by Flagged Count")
        if merchant_rows:
            st.dataframe(pd.DataFrame(merchant_rows), use_container_width=True, hide_index=True)
        else:
            st.info("No flagged merchant data available.")

    with table_col2:
        st.markdown("#### Top 10 Cities by Flagged Count")
        if city_rows:
            st.dataframe(pd.DataFrame(city_rows), use_container_width=True, hide_index=True)
        else:
            st.info("No flagged city data available.")


# ---------------------------------------------------------------------------
# Main view
# ---------------------------------------------------------------------------
tab_queue, tab_activity = st.tabs(["Alert Queue and Triage", "Activity"])

with tab_queue:
    live_queue_fragment()
    render_alert_detail_panel()

with tab_activity:
    render_activity_tab()