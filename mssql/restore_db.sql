:setvar DatabaseName "AdventureWorks"

RESTORE DATABASE $(DatabaseName)
FROM DISK = '/var/opt/mssql/backup/$(DatabaseName).bak'
WITH MOVE '$(DatabaseName)' TO '/var/opt/mssql/data/$(DatabaseName).mdf',
MOVE '$(DatabaseName)_Log' TO '/var/opt/mssql/data/$(DatabaseName)_Log.ldf'
GO