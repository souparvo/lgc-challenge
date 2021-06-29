#!/bin/bash

# get file to install mssql
wget -qO- https://packages.microsoft.com/keys/microsoft.asc | apt-key add -

add-apt-repository "$(wget -qO- https://packages.microsoft.com/config/ubuntu/20.04/mssql-server-2019.list)"

apt-get update
apt-get install -y mssql-server

