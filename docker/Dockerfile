# Imagem oficial do Airflow estendida com as dependências do pacote `anp`.
# As versões vêm das constraints oficiais (as mesmas do CI): nada é fixado à mão.
ARG AIRFLOW_VERSION=3.4.0
ARG PYTHON_VERSION=3.12
FROM apache/airflow:${AIRFLOW_VERSION}-python${PYTHON_VERSION}

ARG AIRFLOW_VERSION
ARG PYTHON_VERSION

COPY requirements.txt /requirements.txt

# Reinstalar o próprio apache-airflow na mesma versão impede o pip de trocá-la.
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" -r /requirements.txt \
      --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

# Falha o build se faltar dependência do DAG ou o auth manager FAB usado no compose.
RUN python -c "import boto3, pyarrow, requests; \
from airflow.providers.fab.auth_manager.fab_auth_manager import FabAuthManager"
