from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.models.baseoperator import chain
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator


create_cost_table = """
    CREATE OR REPLACE TABLE customers
        (
            id INT,
            name VARCHAR(255)
        );
"""


with DAG("snowflake_dag", start_date=datetime(2023,1,1), schedule=None) as dag:
    begin = BashOperator(
        task_id="begin",
        bash_command="pip freeze && python -c 'from snowflake import connector\nprint(connector.__dict__)'"
        )

    create_table = SnowflakeOperator(
        snowflake_conn_id="snowflake_default",
        task_id="create_table_table",
        sql=create_cost_table
    )

    end = EmptyOperator(task_id="end")
    chain(
        begin,
        [create_table],
        end
    )
