import os
import tempfile
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.models import Connection
from airflow.operators.bash import BashOperator
from airflow.operators.docker_operator import DockerOperator
from docker.types import Mount

with DAG(
    'run_dbt_cleaning',
    description='Check if dataset exist and create if not',
    schedule_interval='@daily',
    start_date=datetime.now(),
    catchup=False,
) as dag:

    run_dbt_docker = DockerOperator(
        task_id='run_dbt_docker',
        image='ghcr.io/dbt-labs/dbt-bigquery:1.8.2',
        command='run',
        docker_url='unix://var/run/docker.sock',
        network_mode='bridge',
        mounts = [
            Mount(source="/home/yohann.bacquey/code/BacqueyYohann/stay_informed/dbt_project", target="/usr/app", type="bind"),
            Mount(source="/home/yohann.bacquey/code/BacqueyYohann/stay_informed/dbt_project/profiles.yaml",target="/root/.dbt/profiles.yml",type="bind"),
            Mount(source="/home/yohann.bacquey/.gcp_keys/stay_informed.json", target="/dbt/stayinformed.json",type="bind")
        ],
        dag=dag,
    )

#    run_docker_compose >> down_docker_compose
    run_dbt_docker
