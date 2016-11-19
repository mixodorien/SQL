use msdb
go

--drop table dbo.DBM_Resource_Wait_collect
create table dbo.DBM_Resource_Wait_collect_generator(
	collect_id int not null
)
delete dbo.DBM_Resource_Wait_collect_generator

insert into dbo.DBM_Resource_Wait_collect_generator(collect_id) values (1)

declare @collect_id int
update dbo.dbo.DBM_Resource_Wait_collect_generator set collect_id = collect_id + 1, @collect_id = collect_id
select @collect_id




go

drop procedure dbo.DBM_Resource_Wait_Alert
go
create procedure dbo.DBM_Resource_Wait_Alert(
	@threshold int = 15000 -- default 15s
)
as

if object_id('tempdb..#temp') is not null
	drop table #temp

select
	r.session_id as 'request_session_id', db_name(r.database_id) as 'request_db_name',
	substring(t.text, r.statement_start_offset/2, case when r.statement_end_offset = -1 then datalength(t.text) else r.statement_end_offset/2  -  r.statement_start_offset/2 + 1 end) as 'request_query',
	r.command as 'request_command',
	s.login_name as 'request_login_name', s.host_name as 'request_host_name', s.program_name as 'request_program_name',
	r.wait_type, r.wait_resource, r.wait_time/1000 as 'wait_time_s', r.start_time as 'request_start_time',
	nullif(r.blocking_session_id, 0) as 'blocking_session_id', db_name(rb.database_id) as 'blocking_db_name',
	substring(tb.text, rb.statement_start_offset/2, case when rb.statement_end_offset = -1 then datalength(t.text) else rb.statement_end_offset/2  - rb.statement_start_offset/2 + 1 end) as 'blocking_query',
	rb.command as 'blocking_command',
	sb.login_name as 'blocking_login_name', sb.host_name as 'blocking_host_name', sb.program_name as 'blocking_program_name',
	isnull(rb.start_time, sb.last_request_start_time) as 'blocking_start_time',
	sb.[status] as 'blocking_status', sb.host_process_id  as 'blocking_host_process_id'
into #temp
from sys.dm_exec_requests r
join sys.dm_exec_sessions s on s.session_id = r.session_id
outer apply sys.dm_exec_sql_text(r.sql_handle) t
left join sys.dm_exec_requests rb on rb.session_id = r.blocking_session_id
left join sys.dm_exec_sessions sb on sb.session_id = r.blocking_session_id
outer apply sys.dm_exec_sql_text(rb.sql_handle) tb
where 1 = 1
and s.is_user_process = 1
and r.wait_type NOT IN ('ASYNC_IO_COMPLETION',
 'CLR_SEMAPHORE', 'LAZYWRITER_SLEEP', 'RESOURCE_QUEUE', 'SLEEP_TASK',
 'SLEEP_SYSTEMTASK', 'SQLTRACE_BUFFER_FLUSH', 'WAITFOR', 'LOGMGR_QUEUE',
 'CHECKPOINT_QUEUE', 'REQUEST_FOR_DEADLOCK_SEARCH', 'XE_TIMER_EVENT', 'BROKER_TO_FLUSH',
 'BROKER_TASK_STOP', 'CLR_MANUAL_EVENT', 'CLR_AUTO_EVENT', 'DISPATCHER_QUEUE_SEMAPHORE',
 'FT_IFTS_SCHEDULER_IDLE_WAIT', 'XE_DISPATCHER_WAIT', 'XE_DISPATCHER_JOIN', 'BROKER_EVENTHANDLER',
 'TRACEWRITE', 'FT_IFTSHC_MUTEX', 'SQLTRACE_INCREMENTAL_FLUSH_SLEEP',
 'BROKER_RECEIVE_WAITFOR', 'ONDEMAND_TASK_QUEUE', 'DBMIRROR_EVENTS_QUEUE',
 'DBMIRRORING_CMD', 'BROKER_TRANSMITTER', 'SQLTRACE_WAIT_ENTRIES',
 'SLEEP_BPOOL_FLUSH', 'SQLTRACE_LOCK')
and s.program_name not like 'SQLAgent%'
and s.program_name not in ('Time Navigator Enterprise Edition')
and r.wait_time >= @threshold
--and s.program_name in ('.Net SqlClient Data Provider', 'GCC', 'Periph')

-- retrieve sessions waiting from resource
select request_session_id, request_db_name, request_query, request_command, request_login_name, request_host_name, request_program_name,
 wait_type, wait_resource, wait_time_s, request_start_time, blocking_session_id
from #temp
order by blocking_session_id, request_session_id


-- retrieve blocking sessions (except sessions )
select distinct b.blocking_session_id, b.blocking_db_name, b.blocking_query, b.blocking_command, b.blocking_login_name, b.blocking_host_name, b.blocking_program_name,
 b.blocking_start_time, b.blocking_status, b.blocking_host_process_id
from #temp b
where b.blocking_session_id is not null
and not exists(
	select 1
	from #temp r
	where r.request_session_id = b.blocking_session_id and r.blocking_session_id is not null
)
go

grant execute on dbo.DBM_Resource_Wait_Alert to DBMONITOR
go

/*
exec dbo.DBM_Resource_Wait_Alert -1
exec dbo.DBM_Resource_Wait_Alert 0
exec dbo.DBM_Resource_Wait_Alert 30000

sp_help 'sys.dm_exec_requests'
select * from #temp
*/


