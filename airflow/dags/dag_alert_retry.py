from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from fraud_common import execute

COOLDOWN_MINUTES = 10


def requeue_failed(**_):
    # Cool-down: do not hammer the API right after a failure
    n = execute(
        """
        UPDATE alerts
        SET llm_status = 'PENDING', llm_attempts = 0,
            last_error = NULL, updated_at = now()
        WHERE llm_status = 'FAILED'
          AND updated_at < now() - interval '%d minutes'
        """ % COOLDOWN_MINUTES
    )
    print(f"Requeued {n} failed alerts")


with DAG(
    dag_id="fraud_alert_retry",
    description="Requeue FAILED alerts and run one pass of the alert service",
    start_date=datetime(2026, 10, 1),
    schedule="*/15 * * * *",
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=2)},
    tags=["fraud", "alerts"],
) as dag:

    requeue = PythonOperator(task_id="requeue_failed", python_callable=requeue_failed)

    run_service = BashOperator(
        task_id="run_alert_service",
        bash_command="python /opt/airflow/alerts/alert_service.py",
        env={"MAX_ALERTS_PER_RUN": "50"},
        append_env=True,
    )

    requeue >> run_service