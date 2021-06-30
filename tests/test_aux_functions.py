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

# Gets schema data from SQL Server db
SQL_DESCRIBE_TABLES = """
USE {db};
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
WHERE
	tab.name = '{tb}' AND schema_name(tab.schema_id) = '{sch}'
ORDER BY 
    schema_name,
    table_name, 
    column_id
"""

# Translates SQL Server types to DataFrame
DATAFRAME_TRANSLATE = {
    "bigint": "Int64",
    "bit": "Boolean",
    "decimal": "Float64",
    "int": "Int64",
    "money": "Float64",
    "numeric": "Float64",
    "smallint": "Int8",
    "smallmoney": "Float64",
    "tinyint": "Int8",
    "float": "Float64",
    "real": "Float64",
    "date": "datetime64[ns]",
    "datetime2": "datetime64[ns]",
    "datetime": "datetime64[ns]",
    "datetimeoffset": "str",
    "smalldatetime": "str",
    "time": "datetime64[ns]",
    "char": "str",
    "text": "str",
    "varchar": "str",
    "nchar": "str",
    "ntext": "str",
    "nvarchar": "str",
    "binary": "str",
    "image": "str",
    "varbinary": "str",
    "cursor": "str",
    "hierarchyid": "str",
    "sql_variant": "str",
    "table": "str",
    "rowversion": "str",
    "uniqueidentifier": "str",
    "xml": "str",
    "flag": "Boolean",
    "name": "text"
}

SQL_TRANSLATE = {
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

SQL_SIZABLES = ['char', 'varchar', 'nvarchar', 'nchar', 'decimal', 'numeric']

## Functions

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


def create_sql_create_statment(table: str, schema: str, schema_df: DataFrame, translator=SQL_TRANSLATE, sizables=SQL_SIZABLES) -> str:
    """Creates an SQL statement to create a new table. ATTENTION: Drops table if it exists.

    Args:
        table (str): table name
        schema (str): schema name
        schema_df (DataFrame): data frame with table schema from src
        translator (dict): dictionary with <key,value> pair to translate between src and target types

    Returns:
        str: text with sql create statement
    """
    # apply schema dict
    schema_text = "DROP TABLE IF EXISTS {sch}.{tb};\nCREATE TABLE IF NOT EXISTS {sch}.{tb} (\n".format(sch=schema, tb=table)
    # Iterate over columns
    tb_cols_list = []
    # for row_item in df[(df.schema_name == schema) & (df.table_name == table)].itertuples():
    for row_item in schema_df.itertuples():
        if translate_dict(row_item.data_type, translator) in SQL_SIZABLES:
            tb_cols_list.append('\t' + row_item.column_name + ' ' + translate_dict(row_item.data_type, translator).upper() + '(%d),\n' % row_item.max_length)
        else:
            tb_cols_list.append('\t' + row_item.column_name + ' ' + translate_dict(row_item.data_type, translator).upper() + ',\n')
    # remove comma from last
    last = tb_cols_list.pop()
    tb_cols_list.append(last.replace(',', ')'))
    logger.info(tb_cols_list)
    # create sql text
    for item in tb_cols_list:
        schema_text = schema_text + item
    logger.info("Final query:\n%s" % schema_text)
    return schema_text


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


def convert_df_dtypes(df_in: DataFrame, df_sch: DataFrame, trans_dict=DATAFRAME_TRANSLATE) -> DataFrame:
    """Converts dataframe schemas to with custom translation dict.

    Args:
        df_in (DataFrame): df to transform dtypes
        df_sch (DataFrame): data frame with schema
        trans_dict (dict): [description]

    Returns:
        DataFrame: df with converted types
    """

    # for each column, convert
    for df_row in df_sch.itertuples(index=False):
        col_name = df_row.column_name
        sql_dtype = df_row.data_type
        logger.debug("Converted column %s from %s to %s" % (col_name, sql_dtype, trans_dict[sql_dtype]))
        try:
            df_in[col_name] = df_in[col_name].astype(trans_dict[sql_dtype])
        except Exception as e:
            logger.error("Error converting type %s, assuming string type for column: %s" % (sql_dtype, col_name))
            df_in[col_name] = df_in[col_name].astype(trans_dict[sql_dtype])
        logger.debug("Updated types:\n%s" % df_in.dtypes)

    return df_in


def get_schema_df(db: str, schema: str, table: str, query_func_df: callable, conn_id: str, sql_desc=SQL_DESCRIBE_TABLES) -> DataFrame:

    # query for schemas
    return query_func_df(sql_desc.format(db=db, tb=table, sch=schema), conn_id)


### JDBC Functions
def query_jdbc(query: str, conn_id: str, return_df=True):
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

### Postgres Functions
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

### MSSQL functions
def query_mssql(cuery: str, conn_id='mssql_local'):
    """Executes a SQL query on a MSSQL and returns a pandas df with the results.

    Args:
        cuery (str): SQL query to execute

    Returns:
        DataFrame: pandas dataframe with result
    """
    mssqlsh = MsSqlHook(mssql_conn_id=conn_id)
    df = mssqlsh.get_pandas_df(sql=cuery)
    return df


def sql_table_to_file(table_name: str, output_format: str, output_path: str, query_func: callable, conn_id: str) -> str:
    """Creates CSV or Parquet file from MSSQL table.

    Args:
        table_name (str): full table name: <db>.<schema>.<table> 
        output_format (str): file format, either 'csv' or 'parquet'
        output_path (str): Output file path where files will be stored

    Returns:
        str: path of the file (is pushed to xcom)
    """
    # Create query schema
    db_name = table_name.split('.')[0]
    tb_name = table_name.split('.')[-1]
    tb_schema = table_name.split('.')[1]
    df_schema = get_schema_df(db_name, tb_name, tb_schema, query_func, conn_id)
    df_ori = query_func("SELECT * FROM %s" % table_name)
    logger.info("DataFrame ORIGINAL types:\n%s" % str(df_ori.dtypes))
    df = convert_df_dtypes(df_ori, df_schema, DATAFRAME_TRANSLATE)
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


def mssql_table_to_file(table_name: str, output_format: str, output_path: str, conn_id: str) -> str:
    """[summary]

    Args:
        table_name (str): name of the table to store as file
        output_format (str): format of the file to be stored
        output_path (str): output path of the file
        conn_id (str): connection ID for the query function

    Returns:
        str: return file path stored
    """
    logger.info("Executing function...")
    return sql_table_to_file(table_name, output_format, output_path, query_mssql, conn_id)



### TESTING

out = mssql_table_to_file('AdventureWorks.Purchasing.ProductVendor', 'csv', os.environ['AIRFLOW_HOME'] + '/data', 'mssql_local')

print(out)