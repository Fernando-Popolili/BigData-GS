#!/usr/bin/env bash
# Sobe o Airflow 3 com Docker Compose (Linux). Rode a partir da raiz do repositório.
set -euo pipefail

mkdir -p dags logs plugins config chegada

if [ ! -f docker-compose.yaml ]; then
  curl -LfO 'https://airflow.apache.org/docs/apache-airflow/stable/docker-compose.yaml'
fi

# Monta a pasta chegada/ dentro dos contêineres.
if ! grep -q '/opt/airflow/chegada' docker-compose.yaml; then
  sed -i '/:\/opt\/airflow\/dags$/a\    - ${AIRFLOW_PROJ_DIR:-.}/chegada:/opt/airflow/chegada' docker-compose.yaml
fi

# Sem DAGs de exemplo, para a interface mostrar só a gs_coleta.
sed -i "s/AIRFLOW__CORE__LOAD_EXAMPLES: 'true'/AIRFLOW__CORE__LOAD_EXAMPLES: 'false'/" docker-compose.yaml

echo "AIRFLOW_UID=$(id -u)" > .env

docker compose up airflow-init
docker compose up -d

echo "Airflow em http://localhost:8080 (usuário airflow, senha airflow)."
echo "Ative a DAG gs_coleta; com catchup ela gera um run por dia desde 01/11/2026."
