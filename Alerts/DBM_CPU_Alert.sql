use msdb
go

--drop table dbo.DBM_SystemHealth_last
create table dbo.DBM_SystemHealth_last(
	lastTimeStamp bigint not null
)
go

drop procedure dbo.DBM_Process_CPU_alert
go
create procedure dbo.DBM_Process_CPU_alert(
	@threshold int = 70
)
as

declare @currentTimeStamp bigint = (select cpu_ticks/(cpu_ticks/ms_ticks) from sys.dm_os_sys_info)
declare @lastTimeStamp  bigint = (select top 1 lastTimeStamp from dbo.DBM_SystemHealth_last)

select
	@@SERVERNAME as 'Server',
	dateadd(ms, -1 * (@currentTimeStamp - [timestamp]), getdate()) as [Event Time],
	100 - SystemIdle as [Total Process CPU % Utilization],
	SQLProcessUtilization as [SQL Server Process CPU % Utilization],
	100 - SystemIdle - SQLProcessUtilization as [Other Process CPU % Utilization],
	'Total Process CPU > ' + CONVERT(varchar(10), @threshold) + ' %' as [Alert]
from ( 
	select
		record.value('(./Record/@id)[1]', 'int') as record_id,
		record.value('(./Record/SchedulerMonitorEvent/SystemHealth/SystemIdle)[1]', 'int') as [SystemIdle],
		record.value('(./Record/SchedulerMonitorEvent/SystemHealth/ProcessUtilization)[1]', 'int') as [SQLProcessUtilization],
		[timestamp] 
	from ( 
		select [timestamp], convert(xml, record) as [record]
		from sys.dm_os_ring_buffers
		where ring_buffer_type = N'RING_BUFFER_SCHEDULER_MONITOR'
		and record like '%<SystemHealth>%'
		and [timestamp] > @lastTimeStamp
		) as x 
	) as y 
where SystemIdle < 100 - @threshold
order by record_id desc

delete dbo.DBM_SystemHealth_last

insert into dbo.DBM_SystemHealth_last(lastTimeStamp) select @currentTimeStamp
go

grant execute on dbo.DBM_Process_CPU_alert to DBMONITOR
go



/*
delete dbo.DBM_SystemHealth_last
declare @currentTimeStamp bigint = (select cpu_ticks/(cpu_ticks/ms_ticks) from sys.dm_os_sys_info)
insert into dbo.DBM_SystemHealth_last(lastTimeStamp) select @currentTimeStamp - 36000000

exec dbo.DBM_Process_CPU_alert 10

select 10*60*60*1000

select * from dbo.DBM_SystemHealth_last

*/