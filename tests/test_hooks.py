# Airflow imports
from airflow import DAG
from airflow.models.variable import Variable
from airflow.models.base import SQL_ALCHEMY_SCHEMA
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook, T
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.jdbc.hooks.jdbc import JdbcHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
# Builtin import
from datetime import datetime
from typing import Optional
import os, logging
# Extra packages imports
from redshift_auto_schema import RedshiftAutoSchema
from pandas import DataFrame

# LOGGING
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

# Queries
SQL_CHECK_DB = """
SELECT name, database_id, create_date FROM sys.databases
"""

SQL_DESCRIBE_TABLES = """
USE AdventureWorks;
SELECT
    schema_name(tab.schema_id) AS schema_name,
    tab.name AS table_name, 
    col.column_id,
    col.name AS column_name, 
    t.name AS data_type,    
    col.max_length,
    col.precision
FROM 
    sys.tables AS tab
    INNER JOIN sys.columns AS col
        ON tab.object_id = col.object_id
    LEFT JOIN sys.types AS t
        ON col.user_type_id = t.user_type_id
ORDER BY 
    schema_name,
    table_name, 
    column_id
"""

CSV_DELIMITER = ','

REDSHIFT_TRANSLATE = {
  "bigint": "bigint",
  "bit": "boolean",
  "decimal": "decimal",
  "int": "integer",
  "money": "real",
  "numeric": "numeric",
  "smallint": "smallint",
  "smallmoney": "real",
  "tinyint": "smallint",
  "float": "float",
  "real": "real",
  "date": "date",
  "datetime2": "timestamp",
  "datetime": "timestamp",
  "datetimeoffset": "text",
  "smalldatetime": "text",
  "time": "time",
  "char": "char",
  "text": "text",
  "varchar": "varchar",
  "nchar": "char",
  "ntext": "text",
  "nvarchar": "varchar",
  "binary": "text",
  "image": "text",
  "varbinary": "text",
  "cursor": "text",
  "hierarchyid": "text",
  "sql_variant": "text",
  "table": "text",
  "rowversion": "text",
  "uniqueidentifier": "text",
  "xml": "text",
  "flag": "boolean",
  "name": "text"
}

## Functions
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
    logger.info("DataFrame types: %s" % str(df.dtypes))
    final_path = output_path + '/' + table_name + '.' + output_format
    logger.info("infer: %s" % str(df.infer_objects().dtypes))
    dll = RedshiftAutoSchema(schema='Purchasing', table='ProductVendor', file_df=df).generate_table_ddl()
    if output_format == "csv":
        df.to_csv(final_path, index=False, sep=CSV_DELIMITER, na_rep=None)
    elif output_format == "parquet":
        df.to_parquet(final_path, index=False)
    else:
        raise
    logger.info("Saved %s format in path: %s" % (output_format, final_path))
    return final_path


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
    return True


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
    file_to_s3(local_path, s3_key, bucket_name)
    return True

# def create_redshift_schema(tb_name: str, translator: dict):
#     """Creates an AWS Redshift schema from a MSSQL table, with the help of a translator dict.

#     Args:
#         tb_name (str): name of the table in format db.schema.table
#         translator (dict): dictionary with the <key,value> pair <mssql type,redshifh type>
#     """

#     # get table schemas from mssql
#     df = mssq_query_df(SQL_DESCRIBE_TABLES)
#     # separate elements from name
#     logger.info("Got data from DB")
#     aux_list = tb_name.split('.')
#     db = aux_list[0]
#     schema = aux_list[1]
#     table = aux_list[2]
#     # apply schema dict
#     schema_text = "CREATE TABLE {sch}.{tb} (\n".format(sch=schema, tb=table)
#     # Iterate over columns
#     tb_cols_list = []
#     for row_item in df[(df.schema_name == schema) & (df.table_name == table)].itertuples():
#         tb_cols_list.append('\t' + row_item.column_name + ' ' + translate_dict(row_item.data_type, translator).upper() + ',\n')
#     # remove comma from last
#     last = tb_cols_list.pop()
#     tb_cols_list.append(last.replace(',', ')'))
#     logger.info(tb_cols_list)
#     # create sql text
#     for item in tb_cols_list:
#         schema_text = schema_text + item
#     logger.info("Final query: %s")
#     logger.info("\n" + schema_text)


def translate_dict(item: str, trans: dict) -> str:
    """Auxiliar function to translate using a dictionary.

    Args:
        item (str): key item requiring translation
        trans (dict): dictionary with values to translate from key

    Returns:
        str: translated value
    """

    return trans[item.lower()]

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

    return dll_if_exists


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

def query_postgres(query: str, conn_id='redshift_awsuser_testdb', return_df=True):
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

### TESTING AREA ###
# TST_TABLE = 'AdventureWorks.Production.TransactionHistory_csv'
TST_TABLE = 'AdventureWorks.Purchasing.ProductVendor'
TST_FILE_PATH = '/home/fvcamelo/dev/aws/lgc-challenge/airflow/data/AdventureWorks.Production.TransactionHistory.parquet'
print("AIRFLOW_HOME: %s" % os.getenv('AIRFLOW_HOME'))

mssql_get_table(TST_TABLE, 'csv', '/'.join([os.environ['AIRFLOW_HOME'], 'data']))

# create_redshift_schema(TST_TABLE, REDSHIFT_TRANSLATE)
dll = create_redshift_auto_schema(TST_FILE_PATH, TST_TABLE.split('.')[1], TST_TABLE.split('.')[2])

# res = query_postgres(query=dll, return_df=False)

# print(res)