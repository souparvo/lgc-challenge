# Airflow imports
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook, T
from airflow.providers.jdbc.hooks.jdbc import JdbcHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
# Builtin import
from datetime import datetime
from typing import Optional
import os, logging, csv
# Extra packages imports
from redshift_auto_schema import RedshiftAutoSchema
from pandas import DataFrame

# LOGGING
logger = logging.getLogger(__name__)

# VARS
CSV_DELIMITER = ','

## Functions
### MSSQL functions
def mssq_query_df(cuery: str, conn_id='mssql_local'):
    """Executes a SQL query on a MSSQL and returns a pandas df with the results.

    Args:
        cuery (str): SQL query to execute

    Returns:
        DataFrame: pandas dataframe with result
    """
    mssqlsh = MsSqlHook(mssql_conn_id=conn_id)
    df = mssqlsh.get_pandas_df(sql=cuery)
    return df


def mssql_get_table(table_name: str, output_format: str, output_path: str):
    """Creates CSV or Parquet file from MSSQL table.

    Args:
        table_name (str): full table name: <db>.<schema>.<table> 
        output_format (str): file format, either 'csv' or 'parquet'
        output_path (str): Output file path where files will be stored

    Returns:
        str: path of the file (is pushed to xcom)
    """
    # Create hook 
    df = mssq_query_df("SELECT * FROM %s" % table_name)
    # Create CSV file
    final_path = output_path + '/' + table_name + '.' + output_format
    if output_format == "csv":
        df.to_csv(final_path, index=False, sep=CSV_DELIMITER, quoting=csv.QUOTE_NONNUMERIC, header=True)
    elif output_format == "parquet":
        df.to_parquet(final_path, index=False)
    else:
        raise
    logger.info("Saved %s format in path: %s" % (output_format, final_path))
    return final_path

### AWS functions
def file_to_s3(local_path: str, s3_key: str, bucket_name: str):
    """Loads file to S3 using S3Hook.

    Args:
        local_path (str): local path of the file to load to s3
        s3_key (str): full key for s3
        bucket_name (str): s3 bucket name
    """
    # Create S3Hook
    awsh = S3Hook(aws_conn_id='aws_airflow')
    logger.info("Sending file to S3 with key: %s" % s3_key)
    # Send file to S3
    awsh.load_file(local_path, s3_key, bucket_name, replace=True)
    return s3_key


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


def create_redshift_auto_schema(file_path: str, schema: str, table: str) -> str:
    """Creates a Redshift schema from file.

    Args:
        file_path (str): local file path to generate schema
        schema (str): schema name to generate
        table (str): table name to generate

    Returns:
        str: query dll for table creation
    """
    if file_path.split('.')[-1] == 'csv':
        # ensure the same delimiter is applied when creating and reading the CSV generated files
        new_table = RedshiftAutoSchema(file=file_path, schema=schema, table=table, delimiter=CSV_DELIMITER)
    else:
        new_table = RedshiftAutoSchema(file=file_path, schema=schema, table=table)

    dll = new_table.generate_table_ddl()
    # add if not exists to avoid error
    dll_if_exists = dll[:13] + "IF NOT EXISTS" + dll[12:]
    # @TODO - add option to remove table
    dll_drop = "DROP TABLE IF EXISTS " + schema + "." + table + ";\n\n"

    return dll_drop + dll_if_exists


# def create_redshift_table(file_path: str, schema: str, table: str, conn_id: str):
#     """Creates a table in AWS Redshift.

#     Args:
#         file_path (str): path to local file
#         schema (str): schema where the table will reside
#         table (str): table name
#         conn_id (str): connection ID for Redshift (expects a postgres connection)

#     Returns:
#         str: SQL with table dll
#     """
#     logger.info("Generating table DLL")
#     dll = create_redshift_auto_schema(file_path, schema, table)
#     logger.info("Querying with connection ID: %s" % conn_id)
#     query_postgres(dll, conn_id, False)
#     return dll


### Aux functions
def translate_dict(item: str, trans: dict) -> str:
    """Auxiliar function to translate using a dictionary.

    Args:
        item (str): key item requiring translation
        trans (dict): dictionary with values to translate from key

    Returns:
        str: translated value
    """

    return trans[item.lower()]

### JDBC Functions
def query_jdbc(query: str, conn_id='aws_redshift_awsuser', return_df=True):
    """Executes a query using JDBC hook.

    Args:
        query (str): query string
        conn_id (str, optional): connection ID of to query. Defaults to 'aws_redshift_awsuser'.
        return_df (bool, optional): if True requrns a df, otherwise a list of results. Defaults to True.

    Returns:
        tuple | DataFrame: when return_df ir True returns a DataFrame, otherwise None or a list of results
    """
    jh = JdbcHook(jdbc_conn_id=conn_id)
    if return_df:
        return jh.get_pandas_df(sql=query)
    else:
        return jh.run(sql=query)

def query_postgres(query: str, conn_id: str, return_df=True):
    """Executes a query using JDBC hook.

    Args:
        query (str): query string
        conn_id (str, optional): connection ID of to query. Defaults to 'postgres_redshift_awsuser_testdb'.
        return_df (bool, optional): if True requrns a df, otherwise a list of results. Defaults to True.

    Returns:
        tuple | DataFrame: when return_df ir True returns a DataFrame, otherwise None or a list of results
    """
    ph = PostgresHook(postgres_conn_id=conn_id)
    if return_df:
        return ph.get_pandas_df(sql=query)
    else:
        return ph.run(sql=query)