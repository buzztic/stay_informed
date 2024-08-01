import os
import tempfile
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.models import Connection
from airflow.operators.bash import BashOperator
from airflow.operators.docker_operator import DockerOperator
from docker.types import Mount
from airflow.operators.python import PythonOperator
import logging

def print_debut():
    logging.info(f"sa path variable : {os.getenv('sapath')}")

with DAG(
    'transform_raw_to_silver',
    description='Check if dataset exist and create if not',
    start_date=datetime(2024, 7, 18),
    schedule_interval='@daily',
    catchup = False
) as dag:
    #print(f"sa path variable : {os.getenv('sapath')}")
    run_dbt_docker = DockerOperator(
        task_id='run_dbt_docker',
        image='ghcr.io/dbt-labs/dbt-bigquery:1.8.2',
        command='run',
        docker_url='unix://var/run/docker.sock',
        network_mode='bridge',
        mounts = [
            Mount(source=os.getenv('dbtpath'), target="/usr/app", type="bind"),
            Mount(source=os.getenv('pfpath'),target="/root/.dbt/profiles.yml",type="bind"),
            Mount(source=os.getenv('sapath'), target="/dbt/stayinformed.json",type="bind")
        ],
        dag=dag,
    )
    debug_print = PythonOperator(           #This task filters the file names to only keep the one uploaded on the execution date
        task_id='debut_print',
        python_callable=print_debut
    )

#    run_docker_compose >> down_docker_compose
    run_dbt_docker
