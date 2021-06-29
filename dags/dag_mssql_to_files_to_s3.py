# Airflow imports
from airflow import DAG
from airflow.models.variable import Variable
from airflow.models.base import SQL_ALCHEMY_SCHEMA
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook, T
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
# Builtin import
from datetime import datetime
import os, logging
# Extra packages imports
from redshift_auto_schema import RedshiftAutoSchema
from sqlalchemy import schema
# Owner modules imports
from functions.aux_functions import mssql_get_table, table_file_to_s3, create_redshift_auto_schema, query_postgres

# Logging
logger = logging.getLogger(__name__)

# VARS
DAG_ID = "mssql_files_to_s3_v1"
DAG_SCHEDULE = None
DAG_START_DATE = datetime(2021, 6, 26)

DAG_DEFAULT_ARGS = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
}

# Execution vars
configs = Variable.get('lgc_vars', deserialize_json=True)
file_out_path = '/'.join([os.environ['AIRFLOW_HOME'], 'data'])
target_bucket = configs['s3_bucket']
tables_to_process = configs['tables_to_process']
s3_key_path = configs['s3_key_path']
out_format = configs['output_format']
redshift_conn_id = configs['postgres_redshift_conn_id']

# Functions - spicific to DAG
def create_redshift_table(schema_: str, table_: str, conn_id: str, **context):
    """Creates a table in AWS Redshift.

    Args:
        file_path (str): path to local file
        schema (str): schema where the table will reside
        table (str): table name
        conn_id (str): connection ID for Redshift (expects a postgres connection)

    Returns:
        str: SQL with table dll
    """
    file_path = context['templates_dict']['file_path']
    logger.info("Generating table DLL")
    dll = create_redshift_auto_schema(file_path, schema_, table_)
    logger.info("Querying with connection ID: %s" % conn_id)
    query_postgres(query=dll, conn_id=conn_id, return_df=False)
    return dll

# DAG definition
with DAG(
    dag_id=DAG_ID,
    schedule_interval=DAG_SCHEDULE,
    default_args=DAG_DEFAULT_ARGS,
    start_date=DAG_START_DATE,
) as dag:

    start = DummyOperator(
        task_id="start"
    )
    ##
    # table = tables_to_process[0] ## TO CHANGE TO CYCLE
    for table in tables_to_process[0:1]:
        # Create schenma and table names
        tb_schema = table.split('.')[1]
        tb = table.split('.')[-1]
        id_name = '_'.join([tb_schema, tb])
        # Tasks
        query_mssql_id = 'mssql_table_%s_to_%s' % (id_name, out_format)
        query_mssql = PythonOperator(
            task_id=query_mssql_id,
            python_callable=mssql_get_table,
            op_kwargs={
                'output_path': file_out_path,
                'table_name': table,
                'output_format': out_format
            }
        )

        send_file_s3_id = 'send_file_s3_%s_%s' % (id_name, out_format)
        send_file_s3 = PythonOperator(
            task_id=send_file_s3_id,
            python_callable=table_file_to_s3,
            op_kwargs={
                'task_id': query_mssql_id, 
                's3_key_path': s3_key_path, 
                'bucket_name': target_bucket,
                'output_path': file_out_path,
                'table_name': table,
                'output_format': out_format
            }
        )

        create_tb_reshift = PythonOperator(
            task_id='create_redshift_table_%s_%s' % (id_name, out_format),
            python_callable=create_redshift_table,
            op_kwargs={
                'schema_': tb_schema,
                'table_': tb,
                'conn_id': redshift_conn_id
            },
            templates_dict={
                'file_path': "{{ task_instance.xcom_pull(task_ids='" + query_mssql_id + "', key='return_value') }}"
            }    
        )

        load_s3_to_redshift = S3ToRedshiftOperator(
            task_id='s3_%s_%s_to_redshift' % (id_name, out_format),
            s3_bucket=target_bucket,
            s3_key="{{ ti.xcom_pull(task_ids=\""+ send_file_s3_id +"\") }}",
            schema=tb_schema,
            table=tb,
            copy_options=[out_format, "IGNOREHEADER 1"],
            redshift_conn_id=redshift_conn_id,
            aws_conn_id=configs['aws_conn_id'],

        )

        start >> query_mssql >> send_file_s3 >> load_s3_to_redshift
        query_mssql >> create_tb_reshift >> load_s3_to_redshift
