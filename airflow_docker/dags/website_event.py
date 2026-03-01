from datetime import datetime
from pathlib import Path

import pandas as pd
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


dag = DAG(
    dag_id="01_unscheduled",
  schedule=None,
    catchup=False,
)

fetch_events = BashOperator(
    task_id="fetch_events",
    bash_command="curl -o /tmp/events.csv -L 'https://people.sc.fsu.edu/~jburkardt/data/csv/airtravel.csv'",
    dag=dag,
)


def _calculate_stats(input_path, output_path):
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)

    # Read CSV correctly
    events = pd.read_csv(input_path)

    # Simple summary
    summary = events.describe()

    summary.to_csv(output_path)


calculate_stats = PythonOperator(
    task_id="calculate_stats",
    python_callable=_calculate_stats,
    op_kwargs={
        "input_path": "/tmp/events.csv",
        "output_path": "/tmp/event_stats.csv",
    },
    dag=dag,
)

fetch_events >> calculate_stats