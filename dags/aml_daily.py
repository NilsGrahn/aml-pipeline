"""Daily AML screening: one run per day of transaction data."""
import json
import sys
from pathlib import Path

import pendulum
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG, task

PROJECT_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_DIR / "data"
PYTHON = sys.executable  # the Python in your virtual environment
MIN_ROWS_FOR_SHARE_CHECK = 5_000

with DAG(
    dag_id="aml_daily",
    description="Flag suspicious transactions with PySpark, one day at a time",
    schedule="@daily",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    end_date=pendulum.datetime(2025, 1, 31, tz="UTC"),
    catchup=True,          # create a run for every day in the range
    max_active_runs=1,     # one Spark job at a time on a laptop
    default_args={"retries": 1, "retry_delay": pendulum.duration(minutes=1)},
    tags=["aml", "pyspark"],
) as dag:

    # {{ ds }} is the run's date as YYYY-MM-DD, filled in by Airflow
    check_input = BashOperator(
        task_id="check_input",
        bash_command=f"test -d '{DATA_DIR}/transactions/txn_date={{{{ ds }}}}'",
    )

    flag_transactions = BashOperator(
        task_id="flag_transactions",
        bash_command=f"'{PYTHON}' '{PROJECT_DIR}/jobs/flag_transactions.py' --date {{{{ ds }}}}",
    )

    @task
    def validate_output(ds=None):
        summary = json.loads((DATA_DIR / "summary" / f"{ds}.json").read_text())
        print(summary)

        if summary["transactions"] == 0:
            raise ValueError(f"No transactions read for {ds}")

        # The flagged share only says something on days with enough volume.
        # PaySim has a few very small days where fraud dominates.
        share = summary["flagged"] / summary["transactions"]
        if share > 0.20:
            message = f"{share:.0%} of transactions flagged for {ds}"
            if summary["transactions"] >= MIN_ROWS_FOR_SHARE_CHECK:
                raise ValueError(f"{message}, rules are too wide")
            print(f"WARNING: {message} (low-volume day, not failing the run)")

    check_input >> flag_transactions >> validate_output()