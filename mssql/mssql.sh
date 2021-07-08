#!/bin/bash

##
#
#   Use:
#       sudo bash mssql.sh [install|start]
##

## LOGGING FUNCTION
logging() {
    echo "$(date '+%Y-%m-%d %H:%M:%S') -- $1 --"
}

OPTION=$1

if [ "$OPTION" = "install" ]; then
    logging "Installing MSSQL Server"
    # get file to install mssql
    wget -qO- https://packages.microsoft.com/keys/microsoft.asc | apt-key add -

    add-apt-repository "$(wget -qO- https://packages.microsoft.com/config/ubuntu/20.04/mssql-server-2019.list)"

    apt-get update
    apt-get install -y mssql-server

    # Setup database and create password
    /opt/mssql/bin/mssql-conf setup

    # Intall tools to install DB
    curl https://packages.microsoft.com/keys/microsoft.asc | sudo apt-key add -
    curl https://packages.microsoft.com/config/ubuntu/20.04/prod.list | sudo tee /etc/apt/sources.list.d/msprod.list

    sudo apt-get update 
    sudo apt-get install mssql-tools unixodbc-dev

elif [ "$OPTION" = "start" ]; then
    logging "Starting MSSQL Server, to stop press Ctrl + C"
    # If WSL linux systemctl is not turned ON
    # systemctl status mssql-server --no-pager
    # use this command
    su - mssql -c "/opt/mssql/bin/sqlservr"
else
    logging "Not valid option"
fi

logging "END"