use msdb
go

/*
select *
from sys.procedures
order by create_date desc
*/

--drop table dbo.DBM_logspace_Last
create table dbo.DBM_logspace_Last(
	DBName sysname not null,
	logSize bigint not null,
	logPercentUsed smallint not null,
	lastUpdate datetime not null
)
go
create unique clustered index idx_DBName on dbo.DBM_logspace_Last(DBName)
go

drop procedure dbo.DBM_logspace_alert
go
create procedure dbo.DBM_logspace_alert
as

declare @logspace table(
	DBName sysname not null,
	logSize bigint not null, --decimal(18, 9) not null,
	logPercentUsed smallint not null, --(decimal(18, 9) not null,
	[status] int not null
)

insert into @logspace(DBName, logSize, logPercentUsed, [status])
exec('dbcc sqlperf(logspace)')

delete @logspace
where logSize < 100
or logPercentUsed < 25
or DBName in ('tempdb')

select
	c.DBName as 'Database Name',
	c.logSize as 'Log Size (MB)',
	c.logPercentUsed as 'Log Space Used (%)',
	'Log Space Used > 25% (' + case when l.DBName is null then 'new' else 'increased ' + convert(varchar(2), c.logPercentUsed - l.logPercentUsed) + '%' end + ')' as 'Alert'
from @logspace c
left join dbo.DBM_logspace_Last l on l.DBName = c.DBName
where 1 = 1
--and c.logSize > 100 and c.logPercentUsed > 25 and c.DBName not in ('tempdb')
and (l.DBName is null or c.logPercentUsed >= l.logPercentUsed + 25)
order by case when l.DBName is null then 0 else 1 end, c.DBName

delete l
from dbo.DBM_logspace_Last l
where not exists(
	select 1
	from @logspace c
	where c.DBName = l.DBName
)

update l
set l.logPercentUsed = c.logPercentUsed, l.lastUpdate = getdate()
from dbo.DBM_logspace_Last l
join @logspace c on c.DBName = l.DBName
where c.logPercentUsed >= l.logPercentUsed + 25

insert into dbo.DBM_logspace_Last(DBName, logSize, logPercentUsed, lastUpdate)
select DBName, logSize, logPercentUsed, getdate()
from @logspace c
where not exists(
	select 1
	from dbo.DBM_logspace_Last l
	where l.DBName = c.DBName
)

go

grant execute on dbo.DBM_logspace_alert to DBMONITOR
go


exec dbo.DBM_logspace_alert


/*
select @@SERVERNAME

select 'use [' + name + ']
go
CREATE USER [DBMONITOR] FOR LOGIN [DBMONITOR]
go'
from sys.databases
where database_id > 4
order by database_id

select * from dbo.DBM_logspace_Last -- 2016-05-26 14:49:29.820
delete dbo.DBM_logspace_Last
update dbo.DBM_logspace_Last set logPercentUsed = logPercentUsed - 5

sp_helptext DBM_logspace_alert

*/

