# LGC Challenge

## Description and approach

[Apache Airflow](https://airflow.apache.org/) is an open source orchestration tool for batch data pipelines. Airflow API: Operators and Hooks is used as the programming foundation for the ingestion of data from a MSSQL Server database to a Redshift cluster. The pipeline deployed follows the steps:

__load MSSQL table to CSV/Parquet file --> load CSV/Parquet file to S3 bucket --> create redshift table from schema downloaded from MSSQL --> load S3 bucket file to redshift table__

The DAG file used is `dags/dag_mssql_to_files_to_s3.py`. The DAG can be configured to load a tables, using the Variables feature of Airflow. 

## Environment

WSL2 Ubuntu 20.04 LTS

## Installation of Airflow

Airflow is installed on a virtual environment using python3. It is configured as a development environment with a single process for webserver and sync executor, and a separate process for the scheduler. The metadata database is a SQLlite. There is no concurrency in this deployment, tasks are executed synchronously.

Installation of Airflow will create a `airflow` folder with the required files on the root of the repository.

### Step 1 - Install Airflow instance

In the root of the repository folder, use the `airflow.sh` script to install an instance of Airflow.

```bash
cd /path/to/repo

# Install Airflow
bash airflow.sh install

# After start the service (in WSL systemd is not started
bash airflow.sh start
```

The script will configure and create the required folders and symlinks to the dags in the `dags/`. The password and user to enter the UI is admin:admin.

Process will start in the background. In order to stop the service run:

```bash
bash airflow.sh stop
```

### Step 2 - Create connections and load variables on Airflow

__Variables__:
1. Login to Airflow
2. On the top tab, go to Admin --> Variables --> click on the "Import Variables" and select the `variables.json` file on the root of the repo

__Connections__:
1. Create the connections to MSSQL, AWS and Redshift instance: Admin --> Connections --> `+` button
2. Add the connection ids in the variables

### Step 3 - Change variables to your own config

Each variable loaded needs to be changed to reflect the installed environment.
```json
{
    "lgc_vars": {
        "aws_conn_id": "aws_airflow", // connection to AWS, for S3
        "mssql_conn_id": "mssql_local", // connection to MSSQL instance with source data
        "output_format": "csv", // can be 'csv' or 'parquet'
        "postgres_redshift_conn_id": "redshift_awsuser_testdb", // Connection ID for Redshift cluster
        "s3_bucket": "lgc-challenge", // name of the bucket in S3
        "s3_key_path": "redshift/data", // S3 bucket key where table files will be sent
        "tables_to_process": [ // Tables list to load to Redshift always in format <db>.<schema>.<table>
            "AdventureWorks.Purchasing.ProductVendor",
            "AdventureWorks.Purchasing.PurchaseOrderDetail",
            "AdventureWorks.Purchasing.PurchaseOrderHeader",
            "AdventureWorks.Purchasing.ShipMethod",
            "AdventureWorks.Purchasing.Vendor"
        ]
    }
}
```

## Installation of MSSQL Server

MSSQL Server is used as the source of the data. The repo includes scripts to install and load data.

### Step 1 - Installs MS SQL Server (choose express version) 
Use the scritps in the `mssql/` folder. Execute the following commands, assuming being in the root of the repo:

```bash
cd mssql

# Install and select version and SA password
bash mssql.sh install

# Start the database
bash mssql.sh start
```

Process will start in the foreground, to continue to load the database keep the WSL on and use another WSL window.

### Step 2 - Load the AdventureWorks database

To load the databse use the `load_adventureworks.sh` script:

```bash
bash load_adventureworks.sh
```

The database should be successfully loaded from a backup file.

## Running the DAG

The Airflow DAG is not configured to be automatically scheduled, therefore it should be manually triggered. 

Configuring the `tables_to_process` key in the `lgc_vars` variable with the tables to be sent to redshift.