import json
import pathlib
from datetime import datetime, timedelta

import requests
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


# -----------------------------
# Python Function (Task B)
# -----------------------------
def _get_pictures():
    pathlib.Path("/tmp/images").mkdir(parents=True, exist_ok=True)

    with open("/tmp/launches.json") as f:
        launches = json.load(f)
        image_urls = [launch["image"] for launch in launches["results"]]

        for image_url in image_urls:
            try:
                response = requests.get(image_url)
                image_filename = image_url.split("/")[-1]
                target_file = f"/tmp/images/{image_filename}"

                with open(target_file, "wb") as img_file:
                    img_file.write(response.content)

                print(f"Downloaded {image_url} to {target_file}")

            except requests.exceptions.MissingSchema:
                print(f"{image_url} appears to be an invalid URL.")
            except requests.exceptions.ConnectionError:
                print(f"Could not connect to {image_url}.")


# -----------------------------
# DAG Definition
# -----------------------------
with DAG(
    dag_id="download_rocket_launcher_parallel",
    start_date=datetime.now() - timedelta(days=1),
    schedule=None,
    catchup=False,
) as dag:

    # A
    download_launches = BashOperator(
        task_id="download_launches",
        bash_command="curl -o /tmp/launches.json -L 'https://ll.thespacedevs.com/2.0.0/launch/upcoming'",
    )

    # B
    get_pictures = PythonOperator(
        task_id="get_pictures",
        python_callable=_get_pictures,
    )

    # C
    task_c = BashOperator(
        task_id="task_c",
        bash_command="echo 'Running parallel branch C'",
    )

    # D
    task_d = BashOperator(
        task_id="task_d",
        bash_command="echo 'Task D after get_pictures'",
    )

    # E
    task_e = BashOperator(
        task_id="task_e",
        bash_command="echo 'Task E after task_c'",
    )

    # F
    notify = BashOperator(
        task_id="notify",
        bash_command='echo "Pipeline finished. Total images: $(ls /tmp/images | wc -l)"',
    )

    # -----------------------------
    # Dependencies (Parallel Flow)
    # -----------------------------
    download_launches >> [get_pictures, task_c]

    get_pictures >> task_d
    task_c >> task_e

    [task_d, task_e] >> notify