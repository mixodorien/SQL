use msdb
go

create table dbo.DBM_DB_Status_Last(
       name sysname not null,
       state_desc nvarchar(60) not null
)
go 
 
-- proc
drop procedure dbo.DBM_DB_Status_alert
go
create procedure dbo.DBM_DB_Status_alert
as
declare @DBM_DB_Status table(
       name sysname not null,
       state_desc nvarchar(60) not null,
       [state] tinyint not null
)
 
insert into @DBM_DB_Status(name, state_desc, [state])
select name, state_desc, [state]
from sys.databases
where state_desc not in ('ONLINE')
 
select d.name as 'Database name', d.state_desc as 'Database status'
from @DBM_DB_Status d
where not exists(
       select 1
       from dbo.DBM_DB_Status_Last l
       where l.name = d.name
       and l.state_desc = d.state_desc
)
order by d.[state] desc, d.name
 
delete dbo.DBM_DB_Status_Last
 
insert into dbo.DBM_DB_Status_Last(name, state_desc)
select name, state_desc
from @DBM_DB_Status
go

grant execute on dbo.DBM_DB_Status_alert to DBMONITOR
go

--exec dbo.DBM_DB_Status_alert
