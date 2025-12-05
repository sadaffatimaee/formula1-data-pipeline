# Start from the official Airflow image with Python 3.11
FROM apache/airflow:2.10.1-python3.11

#############################

#############################

USER root

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        git \
        build-essential \
        libpq-dev \
        python3-dev \
        openssl \
    && rm -rf /var/lib/apt/lists/*

#############################
# 2. Install Airflow provider + ML libs
#############################

USER airflow

RUN pip install --no-cache-dir \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-3.11.txt" \
    yfinance \
    apache-airflow-providers-snowflake \
    sentence-transformers \
    pinecone-client

#############################
# 3. Install dbt (NO constraints)
#############################

RUN pip install --no-cache-dir \
    dbt-core==1.10.15 \
    dbt-snowflake==1.10.3

#############################
# 4. Copy your local plugins + dbt project
#############################

COPY plugins /opt/airflow/plugins
COPY dbt /opt/airflow/dbt

#############################
# 5. Environment variables
#############################

ENV AIRFLOW_HOME=/opt/airflow
ENV PYTHONPATH="${PYTHONPATH}:/opt/airflow"
