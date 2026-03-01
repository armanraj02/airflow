from datetime import timedelta

from airflow import DAG
from airflow.utils import timezone
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator

START_DATE = timezone.datetime(2026, 2, 23)
ERP_CHANGE_DATE = timezone.datetime(2026, 2, 25)


def _pick_erp_system(**context):
    logical_date = context["logical_date"]

    if logical_date < ERP_CHANGE_DATE:
        return "fetch_sales_old"
    else:
        return "fetch_sales_new"


def _fetch_sales_old():
    print("Fetching sales data from the old ERP system")


def _fetch_sales_new():
    print("Fetching sales data from the new ERP system")


def _clean_sales_old():
    print("Cleaning sales data from the old ERP system")


def _clean_sales_new():
    print("Cleaning sales data from the new ERP system")


with DAG(
    dag_id="03_branching_dags",
    start_date=START_DATE,
    schedule="@daily",
    catchup=False,
    default_args={
        "owner": "airflow",
        "retries": 1,
        "retry_delay": timedelta(minutes=1),
    },
    tags=["branching", "example"],
) as dag:

    start = EmptyOperator(task_id="start")

    pick_erp_system = BranchPythonOperator(
        task_id="pick_erp_system",
        python_callable=_pick_erp_system,
    )

    fetch_sales_old = PythonOperator(
        task_id="fetch_sales_old",
        python_callable=_fetch_sales_old,
    )

    clean_sales_old = PythonOperator(
        task_id="clean_sales_old",
        python_callable=_clean_sales_old,
    )

    fetch_sales_new = PythonOperator(
        task_id="fetch_sales_new",
        python_callable=_fetch_sales_new,
    )

    clean_sales_new = PythonOperator(
        task_id="clean_sales_new",
        python_callable=_clean_sales_new,
    )

    fetch_weather = EmptyOperator(task_id="fetch_weather")
    clean_weather = EmptyOperator(task_id="clean_weather")

    join_datasets = EmptyOperator(
        task_id="join_datasets",

    )

    train_model = EmptyOperator(task_id="train_model")
    deploy_model = EmptyOperator(task_id="deploy_model")

    # Dependencies
    start >> [pick_erp_system, fetch_weather]

    pick_erp_system >> [fetch_sales_old, fetch_sales_new]

    fetch_sales_old >> clean_sales_old
    fetch_sales_new >> clean_sales_new

    fetch_weather >> clean_weather

    [clean_sales_old, clean_sales_new, clean_weather] >> join_datasets

    join_datasets >> train_model >> deploy_model