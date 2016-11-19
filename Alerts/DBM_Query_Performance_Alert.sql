use msdb
go

drop procedure dbo.DBM_Query_Performance_Alert
go
create procedure dbo.DBM_Query_Performance_Alert(
       @thresholdDuration bigint = 5000000, -- default 5 s
	   @thresholdNbQueries int = 20 -- default nb queries > 20
)
as
declare
@currentFilename nvarchar(260), @startDate datetime, @nbQueries int, @totalDuration bigint

set @startDate = dateadd(mi, -5, getdate())

if datepart(hh, @startDate) not between 8 and 20
	return

select @currentFilename = [path]
from sys.traces
where [path] like '%DBM_Queries_And_Locks%'

if object_id('tempdb..#t') is not null
       drop table #t

select db_name(DatabaseID) as DB, *
into #t
from sys.fn_trace_gettable(@currentFilename, 1)
where 1 = 1
and EventClass in (10, 12, 41, 45, 24)
and Duration > @thresholdDuration
and ApplicationName not like 'SQLAgent%'
and ApplicationName not like 'Microsoft SQL Server%' -- + management ?
and ApplicationName not like 'Time Navigator Enterprise Edition%'
and EndTime > @startDate

set @nbQueries = 0
select top 1 @nbQueries = count(*)
from #t
where EventClass in (10, 12)
group by hostname
order by count(*) desc

if @nbQueries > @thresholdNbQueries
begin
	select @@SERVERNAME as 'Server', hostname, min(EndTime) as 'Start time', max(EndTime) as 'End Time',
	'queries > ' + convert(varchar(10), @thresholdDuration/1000000) + ' s' as 'Counter',
	'Nb queries > ' + convert(varchar(10), @thresholdNbQueries) as 'Event',
	count(*) as 'Nb queries'
	from #t
	where EventClass in (10, 12)
	group by hostname
	having count(*) > @thresholdNbQueries
	order by count(*) desc

	select @totalDuration = sum(Duration)/1000000
	from #t
	where EventClass in (10, 12)

	-- Retrieve
	select top 50 qry.DB, min(qry.StartTime) as 'Start Time', qry.ObjectName, convert(nvarchar(4000), qry.textdata) as 'T-SQL',
	 qry.LoginName, qry.ApplicationName, max(qry.EndTime) as 'End time', count(*) as 'Nb queries', avg(qry.Duration)/1000000 as 'Avg Duration (s)',
	 str(100.*(sum(qry.Duration)/1000000)/@totalDuration, 5, 0) as '% total duration',
	 isnull(convert(varchar(30), avg(lck.Duration)/1000000), 'Unknown') as 'Avg Lock wait (s)',
	 avg(qry.CPU)/1000 as 'Avg CPU (s)', avg(qry.Reads) as 'Avg logical reads', avg(qry.Writes) as 'Avg Nb writes', avg(qry.RowCounts) as 'Avg Row counts', qry.HostName
	from #t qry
	left join #t lck on lck.spid = qry.spid and lck.EndTime > qry.StartTime and lck.StartTime < qry.EndTime and lck.EventClass = 24 -- lock
	where qry.EventClass in (41, 45) -- query statment
	and not exists(
		-- display a query only once 
		select 1
		from #t nest
		where nest.spid = qry.spid
		and nest.EndTime > qry.StartTime and nest.StartTime < qry.EndTime
		and nest.EventClass in (41, 45)
		and nest.NestLevel > qry.NestLevel
	)
	group by qry.DB, qry.ObjectName, qry.ApplicationName, qry.HostName, qry.LoginName, convert(nvarchar(4000), qry.textdata)
	order by sum(qry.Duration) desc
end
go
 
grant execute on dbo.DBM_Query_Performance_Alert to DBMONITOR
go

use master
go
grant alter trace to DBMONITOR
go

/*
exec dbo.DBM_Query_Performance_Alert 5000000, 0
exec dbo.DBM_Query_Performance_Alert 2000000, 0
exec dbo.DBM_Query_Performance_Alert 0, 0

*/

--exec dbo.DBM_Query_Performance_Alert 0, 0
