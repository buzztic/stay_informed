# Just run one time at the begining
docker compose up airflow-init
mkdir -p ./dags ./logs ./plugins ./config
echo "AIRFLOW_UID=$(id -u)" > .env