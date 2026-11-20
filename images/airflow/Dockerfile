# Image Airflow de la plateforme : l'image officielle « slim » (1,1 Go au lieu de 3,2 Go pour l'image
# complète), les seuls modules utiles, puis les DAG et les contrats de data products.
# Construite depuis la racine du dépôt : docker build -f images/airflow/Dockerfile .
ARG AIRFLOW_VERSION=3.2.2
ARG PYTHON_VERSION=3.12
FROM apache/airflow:slim-${AIRFLOW_VERSION}-python${PYTHON_VERSION}

ARG AIRFLOW_VERSION
ARG PYTHON_VERSION
COPY images/airflow/requirements.txt /tmp/requirements.txt
# postgres : base de métadonnées ; statsd : métriques ; le reste est listé dans requirements.txt
RUN pip install --no-cache-dir "apache-airflow[postgres,statsd]==${AIRFLOW_VERSION}" -r /tmp/requirements.txt \
      --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

COPY --chown=airflow:0 dags/ /opt/airflow/dags/
COPY --chown=airflow:0 data-products/ /opt/airflow/data-products/
ENV DATA_PRODUCTS_DIR=/opt/airflow/data-products \
    PYTHONPATH=/opt/airflow/dags
