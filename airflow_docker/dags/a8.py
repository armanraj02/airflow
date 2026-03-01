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
    dag_id="parallel_dag",
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
        bash_command="echo 'Running branch C'",
    )

    # D
    task_d = BashOperator(
        task_id="task_d",
        bash_command="echo 'Task D completed'",
    )

    # E
    task_e = BashOperator(
        task_id="task_e",
        bash_command="echo 'Task E completed'",
    )

    # G
    task_g = BashOperator(
        task_id="task_g",
        bash_command="echo 'Running branch G'",
    )

    # H
    task_h = BashOperator(
        task_id="task_h",
        bash_command="echo 'Task H completed'",
    )

    # F (Final Join)
    notify = BashOperator(
        task_id="notify",
        bash_command='echo "All branches completed. Total images: $(ls /tmp/images | wc -l)"',
    )

    # -----------------------------
    # Dependencies
    # -----------------------------

    # A splits into 3 parallel branches
    download_launches >> [get_pictures, task_c, task_g]

    # Branch 1
    get_pictures >> task_d

    # Branch 2
    task_c >> task_e

    # Branch 3
    task_g >> task_h

    # Join all branches into F
    [task_d, task_e, task_h] >> notify