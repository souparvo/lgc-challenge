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

# Set AIRFLOW_HOME to the repo /airflow folder
export AIRFLOW_HOME=$(cd $(dirname "${BASH_SOURCE[0]}") && pwd)/airflow
export AIRFLOW_VENV=$AIRFLOW_HOME/venv
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
    N_AIRFLOW_PROCESSES=$(ps aux | grep 'airflow' | grep -Ev 'grep|airflow.sh' | wc -l)
    logging "Airflow processes remaining: "
    logging "Sending SIGTERM to airflow processes"
    if [ ${N_AIRFLOW_PROCESSES} -gt 0 ]; then
        kill -9 $(ps aux | grep 'airflow' | grep -Ev 'grep|airflow.sh' | awk '{print $2}')ç
        logging "Airflow processes remaining: $(wc -l $(ps aux | grep 'airflow' | grep -Ev 'grep|airflow.sh'))"
    else
        logging "No processes to kill"
    fi
fi
logging "END"