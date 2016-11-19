use msdb
go
create table dbo.DBM_SQLTrace_last(
       lastFilename nvarchar(260) not null,
       lastEventSequence bigint not null
)
go
create unique clustered index idx_lastFilename on dbo.DBM_SQLTrace_last(lastFilename)
go
 
drop procedure dbo.DBM_Long_Running_Query_Alert
go
create procedure dbo.DBM_Long_Running_Query_Alert(
       @thresholdQuery bigint = 15000000, -- default 15 s
       @thresholdTransact bigint = 300000000 -- default 300 s = 5 mn
)
as
declare
@lastFilename nvarchar(260),
@lastEventSequence bigint,
@currentFilename nvarchar(260),
@nextEventSequence bigint
 
select @lastFilename = lastFilename, @lastEventSequence = lastEventSequence
from dbo.DBM_SQLTrace_last
 
select @currentFilename = [path]
from sys.traces
where [path] like '%DBM_Queries_And_Locks%'
 
if @lastFilename <> @currentFilename or @lastFilename is null
       set @lastEventSequence = 0
 
if object_id('tempdb..#t') is not null
       drop table #t
 
select db_name(DatabaseID) as DB, *
into #t
from sys.fn_trace_gettable(@currentFilename, 1)
where 1 = 1
and (EventClass in (10, 12, 24) and Duration > @thresholdQuery
	or
	EventClass = 50 and Duration > @thresholdTransact
	)
and ApplicationName not like 'SQLAgent%'
and ApplicationName not like 'Microsoft SQL Server%' -- + management ?
and ApplicationName not like 'Time Navigator Enterprise Edition%'
and EventSequence > @lastEventSequence

if @@ROWCOUNT > 0
	select @nextEventSequence = isnull(max(EventSequence) + 1, 0) from #t
else
	set @nextEventSequence = @lastEventSequence
 
-- Retrieve
select top 50 case when EventClass = 50 then 'Transaction' else 'Query' end as [Event type], DB, SPID, StartTime, ObjectName, textdata as 'T-SQL',
 LoginName, ApplicationName, EndTime, Duration/1000000 as 'Duration (s)',
 Duration/(1000000*60) as 'Duration (mn)',
isnull(convert(varchar(30), lockDuration/1000000), 'Unknown') as 'Lock wait (s)',
CPU/1000 as 'CPU (s)', Reads as 'Nb logical reads', Writes as 'Nb writes', RowCounts, HostName, ClientProcessID
from (
       select trs.*, NULL as lockDuration
       from #t trs
       where trs.EventClass = 50 -- transaction
       and not exists(
             select 1
             from #t qry
             where qry.spid = trs.spid and qry.EndTime > trs.StartTime and qry.StartTime < trs.EndTime
             and qry.EventClass in (10, 12) -- query
             and qry.Duration > 0.25 * trs.Duration
       )
       union all
       select qry.*, lck.Duration as lockDuration
       from #t qry
       left join #t lck on lck.spid = qry.spid and lck.EndTime > qry.StartTime and lck.StartTime < qry.EndTime and
             lck.EventClass = 24 -- lock
       where qry.EventClass in (10, 12) -- query
) as t
order by endTime desc

-- fin
delete dbo.DBM_SQLTrace_last
insert into dbo.DBM_SQLTrace_last(lastFilename, lastEventSequence) values (@currentFilename, @nextEventSequence)
go
 
grant execute on dbo.DBM_Long_Running_Query_Alert to DBMONITOR
go

use master
go
grant alter trace to DBMONITOR
go

/*
select * from sys.traces

exec dbo.DBM_Long_Running_Query_Alert --5000000
delete dbo.DBM_SQLTrace_last
select * from dbo.DBM_SQLTrace_last

*/
