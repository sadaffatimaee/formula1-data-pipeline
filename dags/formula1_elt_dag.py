"""
A working dbt DAG that runs dbt commands via BashOperator
"""

from pendulum import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.hooks.base import BaseHook


DBT_PROJECT_DIR = "/opt/airflow/dbt"
DBT_BIN = "/home/airflow/.local/bin/dbt"   


def load_env_vars():
    """
    Load Snowflake credentials safely at runtime.
    """
    conn = BaseHook.get_connection("my_snowflake_conn")
    return {
        "DBT_USER": conn.login,
        "DBT_PASSWORD": conn.password,
        "DBT_ACCOUNT": conn.extra_dejson.get("account"),
        "DBT_WAREHOUSE": conn.extra_dejson.get("warehouse"),
        "DBT_ROLE": conn.extra_dejson.get("role"),
        "DBT_SCHEMA": conn.schema,
        "DBT_DATABASE": conn.extra_dejson.get("database"),
        "DBT_TYPE": "snowflake",
    }


with DAG(
    dag_id="Formula1_ELT_dbt",
    description="Runs dbt models inside the Airflow container",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False
) as dbt_dag:

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"{DBT_BIN} run --project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROJECT_DIR}",
        env=load_env_vars(),
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"{DBT_BIN} test --project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROJECT_DIR}",
        env=load_env_vars(),
    )

    dbt_snapshot = BashOperator(
        task_id="dbt_snapshot",
        bash_command=f"{DBT_BIN} snapshot --project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROJECT_DIR}",
        env=load_env_vars(),
    )

    dbt_run >> dbt_test >> dbt_snapshot
