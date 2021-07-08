#!/bin/bash

##
#
#   Use:
#       bash load_adventureworkers.sh
##

## LOGGING FUNCTION
logging() {
    echo "$(date '+%Y-%m-%d %H:%M:%S') -- $1 --"
}

# Install database - AdventureWorks
# Link to source: https://github.com/Microsoft/sql-server-samples/releases/download/adventureworks/AdventureWorks-oltp-install-script.zip

logging "Unzip AdventureWorks backup file database"
unzip bak/AdventureWorks.bak.zip -d /tmp/
logging "Sending backup file to MSSQL repo"
sudo su - -c "mkdir -p /var/opt/mssql/backup && cp /tmp/AdventureWorks.bak /var/opt/mssql/backup/ && chown mssql:mssql /var/opt/mssql/backup/ -R"

# Execute command
logging "Load to Database with sqlcmd and restore script"
echo "Please insert password: "
read -s password

/opt/mssql-tools/bin/sqlcmd -S localhost -U SA -P ${password} -i restore_db.sql

logging "Removing files tmp files"
rm -f /tmp/AdventureWorks.bak
# rm -f AdventureWorks-oltp-install-script.zip
# rm -rf ./tmp
logging "END"