# Airflow imports
from airflow import DAG
from airflow.models.variable import Variable
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
# Builtin import
from datetime import datetime
import os, logging, csv
# Owner modules imports
from functions.aux_functions import query_postgres, file_to_s3, query_mssql, get_schema_df, create_sql_create_statment, create_redshift_auto_schema, sql_table_to_file
# from functions.aux_functions import * 

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
mssql_conn_id = configs['mssql_conn_id']

# Functions - spicific to DAG
def create_redshift_table(db_: str, schema_: str, table_: str, redshift_conn_id: str, src_conn_id=None, **context):
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
    if src_conn_id is None:
        dll = create_redshift_auto_schema(file_path, schema_, table_)
    else:
        # Getting schema from connection
        # df_schema = get_schema_df_mssql(db=db_, schema=schema_, table=table_, conn_id=src_conn_id)
        df_schema = get_schema_df(db=db_, schema=schema_, table=table_, conn_id=src_conn_id, query_func=query_mssql)
        # df_schema = query_mssql(SQL_DESCRIBE_TABLES.format(db=db_, tb=table_, sch=schema_), conn_id=src_conn_id)
        # Create schema
        dll = create_sql_create_statment(table_, schema_, df_schema)
    logger.info("Querying with connection ID: %s" % redshift_conn_id)
    query_postgres(query=dll, conn_id=redshift_conn_id, return_df=False)
    return dll


def table_file_to_s3(task_id: str, s3_key_path: str, bucket_name: str, local_file_path=None, **context):
    """Send table file to S3 bucket.

    Args:
        task_id (str): task id to get path from XCom
        s3_key_path (str): key path for the file on the bucket
        bucket_name (str): s3 bucket name
        local_file_path (str): local file path to load. Defaults to None
    """
    # Get data from XCOm to get path of local file
    if local_file_path is None:
        local_path = context['ti'].xcom_pull(task_ids=task_id)
        logger.info("Got path from XCom: %s" % local_path)
    else:
        local_path = local_file_path
        logger.info("Using provided path: %s" % local_path)
    # Create S3 key from file name in local path
    s3_key = '/'.join([s3_key_path, local_path.split('/')[-1]])
    # Send file to S3
    return file_to_s3(local_path, s3_key, bucket_name)


def mssql_table_to_file(table_name: str, output_format: str, output_path: str, conn_id: str) -> str:
    """Creates CSV or Parquet file from MSSQL table.

    Args:
        table_name (str): full table name: <db>.<schema>.<table> 
        output_format (str): file format, either 'csv' or 'parquet'
        output_path (str): Output file path where files will be stored
        conn_id (str): connection id to MSSQL

    Returns:
        str: path of the file created (is pushed to xcom)
    """
    return sql_table_to_file(table_name, output_format, output_path, query_mssql, conn_id)

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
    for table in tables_to_process:
        # Create schenma and table names
        db = table.split('.')[0]
        tb_schema = table.split('.')[1]
        tb = table.split('.')[-1]
        id_name = '_'.join([tb_schema, tb])
        # Tasks
        query_mssql_id = 'mssql_table_%s_to_%s' % (id_name, out_format)
        query_mssql_table = PythonOperator(
            task_id=query_mssql_id,
            python_callable=mssql_table_to_file,
            # python_callable=sql_table_to_file,
            op_kwargs={
                'output_path': file_out_path,
                'table_name': table,
                'output_format': out_format,
                'conn_id': mssql_conn_id
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
                'db_': db,
                'schema_': tb_schema,
                'table_': tb,
                'redshift_conn_id': redshift_conn_id,
                'src_conn_id': mssql_conn_id
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

        start >> query_mssql_table >> send_file_s3 >> load_s3_to_redshift
        query_mssql_table >> create_tb_reshift >> load_s3_to_redshift