#!/bin/bash

##
#
#   Use:
#       airflow.sh [install|start|stop]
##

## LOGGING FUNCTION
logging() {
    echo "$(date '+%Y-%m-%d %H:%M:%S') -- $1 --"
}

## FUNCTIONS
create_dir() {
    if [ -d "$1" ]; then
        logging "$1 already exists"
    else
        mkdir -p $1
        logging "Creating $1 dir..."
    fi
}

## VARS

# export AIRFLOW_HOME=${0%/*}/airflow
AIRFLOW_VENV=$AIRFLOW_HOME/venv
OPTION=$1

logging "AIRFLOW_HOME: ${AIRFLOW_HOME}"
# logging "chdir to "

# Install air
if [ "$OPTION" = "install" ]; then
    logging "Installing airflow..."
    # Create folders
    create_dir $AIRFLOW_HOME
    create_dir $AIRFLOW_HOME/logs
    create_dir $AIRFLOW_HOME/dags
    create_dir $AIRFLOW_HOME/run
    # Install airflow on venv
    logging "Install venv"
    if [ -d "$AIRFLOW_VENV/bin" ]; then
        logging "Not creating venv, already exists"
    else
        python3 -m venv $AIRFLOW_VENV
    fi
    # activate environment
    source $AIRFLOW_VENV/bin/activate
    $AIRFLOW_VENV/bin/pip install wheel apache-airflow['amazon','mssql','presto']
    # init db
    $AIRFLOW_VENV/bin/airflow db init
    # create user
    $AIRFLOW_VENV/bin/airflow users create --username admin --firstname user --lastname admin --role Admin --email admin@airflow.com

elif [ "$OPTION" = "start" ]; then
    source $AIRFLOW_VENV/bin/activate
    export AIRFLOW__CORE__LOAD_EXAMPLES="false"
    $AIRFLOW_VENV/bin/airflow webserver -D -L "$AIRFLOW_HOME/logs/webserver.log" -E "$AIRFLOW_HOME/logs/webserver.log" -l "$AIRFLOW_HOME/logs/webserver.log" --pid "$AIRFLOW_HOME/run/webserver.pid" --stdout "$AIRFLOW_HOME/logs/webserver.log" --stderr "$AIRFLOW_HOME/logs/webserver.log" &
    # $AIRFLOW_VENV/bin/airflow webserver -D -l ${AIRFLOW_HOME}/logs/webserver.log --pid ${AIRFLOW_HOME}/run/webserver.pid >> ${AIRFLOW_HOME}/logs/webserver.log 2>&1
    $AIRFLOW_VENV/bin/airflow scheduler -D -l "$AIRFLOW_HOME/logs/scheduler.log" --pid "$AIRFLOW_HOME/run/scheduler.pid" --stdout "$AIRFLOW_HOME/logs/scheduler.log" --stderr "$AIRFLOW_HOME/logs/scheduler.log" &

elif [ "$OPTION" = "stop" ]; then
    logging "Removing pid files..."
    rm -rf $AIRFLOW_HOME/run/*.pid
    kill -9 $(ps aux | grep 'airflow' | awk '{print $2}')
fi

logging "END"