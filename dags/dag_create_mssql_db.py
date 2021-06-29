from airflow import DAG
from airflow.models.variable import Variable
from airflow.models.base import SQL_ALCHEMY_SCHEMA
from airflow.providers.microsoft.mssql.operators.mssql import MsSqlOperator
from airflow.operators.dummy import DummyOperator
from datetime import datetime

# VARS
DAG_ID = "create_mssql_db_v1"
DAG_SCHEDULE = None
DAG_START_DATE = datetime(2021, 6, 26)

DAG_DEFAULT_ARGS = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
}

SQL_CHECK_DB = """
SELECT name, database_id, create_date FROM sys.databases
"""

SQL_DB_PATH = Variable.get("db_create_sql_path")

with DAG(
    dag_id=DAG_ID,
    schedule_interval=DAG_SCHEDULE,
    default_args=DAG_DEFAULT_ARGS,
    start_date=DAG_START_DATE,
) as dag:

    start = DummyOperator(
        task_id="start"
    )

    check_dbs = MsSqlOperator(
        task_id="check_databaes",
        mssql_conn_id="mssql_local",
        sql=SQL_CHECK_DB
    )

    create_db = MsSqlOperator(
        task_id="create_database",
        mssql_conn_id="mssql_local",
        sql=SQL_DB_PATH
    )

    start >> check_dbs >> create_db
