use msdb
go

create table dbo.DBM_exec_query_stats_REF(
	[query_hash] [binary](8) NOT NULL,
	[query_plan_hash] [binary](8) NOT NULL,
	[DB] sysname NOT NULL,
	[objectName] sysname NOT NULL,
	[objectDB] sysname NOT NULL,
	[statement_start_offset] [int] NOT NULL,
	[statement_end_offset] [int] NOT NULL,
	[creation_time] [datetime] NOT NULL,
	[last_execution_time] [datetime] NOT NULL,
	[execution_count] [bigint] NOT NULL,
	[stmt] [nvarchar](max) NULL,
	[max_logical_reads] [bigint] NOT NULL,
	[max_worker_time] [bigint] NOT NULL
)
go


--sp_rename 'DBM_exec_query_stats_REF', 'DBM_exec_query_stats_REF_old'



insert into dbo.DBM_exec_query_stats_REF
select
 s.query_hash, s.query_plan_hash,
 case when a.value = 32767 then 'resourceDB' else db_name(convert(int, a.value)) end as 'DB',
 isnull(object_name(t.objectid, t.dbid), '') as 'objectName',
 case when t.dbid = 32767 then 'resourceDB' else db_name(t.dbid) end as 'objectDB',
s.statement_start_offset, statement_end_offset, s.creation_time as creation_time, s.last_execution_time as last_execution_time, s.execution_count,
substring(t.text, s.statement_start_offset/2 + 1, case when s.statement_end_offset = -1 then datalength(t.text) else s.statement_end_offset/2  - s.statement_start_offset/2 + 1 end) as stmt,
s.max_logical_reads, s.max_worker_time
from sys.dm_exec_query_stats s
cross apply sys.dm_exec_sql_text(s.sql_handle) as t
cross apply sys.dm_exec_plan_attributes(s.plan_handle) as a
where 1 = 1
and a.attribute = 'dbid'
go


--insert into dbo.DBM_exec_query_stats_REF select * from dbo.DBM_exec_query_stats_REF_old

create index ix_stats on dbo.DBM_exec_query_stats_REF(query_hash, statement_start_offset, statement_end_offset, objectName, DB, query_plan_hash)
go


drop procedure dbo.DBM_New_Exec_plan_alert
go
create procedure dbo.DBM_New_Exec_plan_alert
as
if object_id('tempdb..#tmp') is not null
	drop table #tmp

select 
 s.query_hash, s.query_plan_hash,
 case when a.value = 32767 then 'resourceDB' else db_name(convert(int, a.value)) end as 'DB',
 isnull(object_name(t.objectid, t.dbid), '') as 'objectName',
 case when t.dbid = 32767 then 'resourceDB' else isnull(db_name(t.dbid), '') end as 'objectDB',
s.statement_start_offset, statement_end_offset, s.creation_time as creation_time, s.last_execution_time as last_execution_time, s.execution_count,
substring(t.text, s.statement_start_offset/2 + 1, case when s.statement_end_offset = -1 then datalength(t.text) else s.statement_end_offset/2  - s.statement_start_offset/2 + 1 end) as stmt,
s.max_logical_reads, s.max_worker_time
into #tmp
from sys.dm_exec_query_stats s
cross apply sys.dm_exec_sql_text(s.sql_handle) as t
cross apply sys.dm_exec_plan_attributes(s.plan_handle) as a
where 1 = 1
and a.attribute = 'dbid'
and creation_time > dateadd(mi, -5, getdate())
--and s.max_logical_reads > 9999

-- remove queries on change tracking table 
delete #tmp
where stmt like '%CHANGETABLE%'
or stmt like '%change_tracking%'

-- remove specific queries
delete #tmp
where query_hash in (0x2E368CAFA58A50AC, 0x639AD98F2E92D89B, 0x87EE343610EE5727, 0x2AB7606933584CA8, 0x8ACCAF14CB756196, 0x0C63801BED04A2A3, 0x128AA48150F7BD9F)

select convert(varchar(18), t2.query_hash, 1) as 'query_hash', convert(varchar(18), t2.query_plan_hash, 1) as query_plan_hash, t2.DB, t2.objectName, t2.objectDB, min(t2.creation_time) as 'Plan creation',
 max(t2.last_execution_time) as 'Last execution', sum(t2.execution_count) as 'Execution count', min(t2.stmt) as 'Statement',
 max(t2.max_logical_reads) as 'Max logical reads', max(t2.max_worker_time)/1000 as 'Max worker_time (ms)'
from #tmp t2
where 1 = 1
and exists(
	select 1
	from dbo.DBM_exec_query_stats_REF t1
	where t1.DB = t2.DB and t1.objectName = t2.objectName
	 and t1.statement_start_offset = t2.statement_start_offset
	 and t1.statement_end_offset = t2.statement_end_offset
	 and t1.query_hash = t2.query_hash
)
and not exists(
	select 1
	from dbo.DBM_exec_query_stats_REF t1
	where t1.DB = t2.DB and t1.objectName = t2.objectName
	 and t1.statement_start_offset = t2.statement_start_offset
	 and t1.statement_end_offset = t2.statement_end_offset
	 and t1.query_hash = t2.query_hash
	 and t1.query_plan_hash = t2.query_plan_hash
)
and t2.max_logical_reads > 9999
and t2.db not in ('msdb')
group by t2.query_hash, t2.query_plan_hash, t2.DB, t2.objectName, t2.objectDB, t2.statement_start_offset, t2.statement_end_offset
order by max(t2.max_logical_reads) desc

-- add new queries
insert into dbo.DBM_exec_query_stats_REF(query_hash, query_plan_hash, DB, objectName, objectDB, statement_start_offset, statement_end_offset, creation_time,
 last_execution_time, execution_count, stmt, max_logical_reads, max_worker_time)
select t2.query_hash, t2.query_plan_hash, t2.DB, t2.objectName, t2.objectDB, t2.statement_start_offset, t2.statement_end_offset, min(t2.creation_time),
 min(t2.last_execution_time), sum(t2.execution_count), min(t2.stmt), max(t2.max_logical_reads), max(t2.max_worker_time)
from #tmp t2
where not exists(
	select 1
	from dbo.DBM_exec_query_stats_REF t1
	where t1.DB = t2.DB and t1.objectName = t2.objectName
	and t1.statement_start_offset = t2.statement_start_offset
	and t1.statement_end_offset = t2.statement_end_offset
	and t1.query_hash = t2.query_hash
	and t1.query_plan_hash = t2.query_plan_hash
)
group by t2.query_hash, t2.query_plan_hash, t2.DB, t2.objectName, t2.objectDB, t2.statement_start_offset, t2.statement_end_offset
go

grant execute on dbo.DBM_New_Exec_plan_alert to DBMONITOR
go

/*
exec dbo.DBM_New_Exec_plan_alert

delete DBM_exec_query_stats_REF
where query_hash = 0xC16B13AF7CFACEDE

select * from DBM_exec_query_stats_REF
where query_hash = 0x0DD6AEC2300193AF
and DB = 'Roule_online-PROD'
where query_hash = 0x118B4B4C64043FB6

select convert(varbi(16), 0x049028EDA462E3E6)
select convert(varchar(16), 0x049028EDA462E3E6)
0x278B785992169FD7




sp_helptext DBM_New_Exec_plan_alert

select len('0x049028EDA462E3E6')

select convert(varchar(18), 0x049028EDA462E3E6, 1)

exec sp_spaceused DBM_exec_query_stats_REF

select top 10 * from dbo.DBM_exec_query_stats_REF
where creation_time > dateadd(mi, -10, getdate())


*/



