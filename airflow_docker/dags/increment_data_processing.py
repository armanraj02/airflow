from datetime import datetime
from pathlib import Path
import pandas as pd

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator


def _calculate_stats(input_path, output_path):
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)

    events = pd.read_json(input_path)

    stats = (
        events
        .groupby(["date", "user"])
        .size()
        .reset_index(name="count")
    )

    stats.to_csv(output_path, index=False)
    print(f"Processed file saved to {output_path}")


with DAG(
    dag_id="pre_incremental_data_processing",
    start_date=datetime(2026, 2, 24),
    schedule="@daily",
    catchup=False,
) as dag:

    fetch_events = BashOperator(
        task_id="fetch_events",
        bash_command=(
            "mkdir -p /tmp/data && "
            "echo 'START={{ data_interval_start }} END={{ data_interval_end }}' && "
            "curl -s -o /tmp/data/events_{{ ds }}.json "
            "'http://events_api:5000/events?"
            "start_date=2026-02-24&"
            "end_date=2026-02-26'"
        ),
    )

    calculate_stats = PythonOperator(
        task_id="calculate_stats",
        python_callable=_calculate_stats,
        op_kwargs={
            "input_path": "/tmp/data/events_{{ ds }}.json",
            "output_path": "/tmp/data/output_{{ ds }}.csv",
        },
    )

    fetch_events >> calculate_stats