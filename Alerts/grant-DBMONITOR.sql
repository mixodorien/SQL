select 'use [' + name + ']
go
' + 'grant view definition to DBMONITOR
go'
from sys.databases


select 'use [' + name + ']
go
' + 'if not exists(select 1 from sys.database_principals where type = ''S'' and name = ''DBMONITOR'')
	create user [DBMONITOR] for login [DBMONITOR] with default_schema = [dbo]
go'
from sys.databases


