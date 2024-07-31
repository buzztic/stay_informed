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

    run_docker_compose = DockerOperator(
        task_id = "run_docker_compose",
        image= "ghcr.io/dbt-labs/dbt-bigquery:1.8.2",
        container_name= "dbt_one_run",
        api_version='auto',
        auto_remove=True,
        #command="bash -c 'dbt build'",
        command="run",
        #docker_url="tcp://docker-proxy:2375",
        docker_url='unix://var/run/docker.sock',
        network_mode="bridge",
        mounts = [Mount(source="/home/yohann.bacquey/code/BacqueyYohann/stay_informed/dbt_project", target="/usr/app", type="bind"),
                Mount(source="/home/yohann.bacquey/code/BacqueyYohann/stay_informed/dbt_project/profiles.yaml",target="/root/.dbt/profiles.yml",type="bind"),
                Mount(source="/home/yohann.bacquey/.gcp_keys/stay_informed.json", target="/dbt/stayinformed.json",type="bind")
                ],
        mount_tmp_dir = False


    )

#    run_docker_compose >> down_docker_compose
    run_docker_compose
