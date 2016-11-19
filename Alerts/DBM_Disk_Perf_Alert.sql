use msdb
go

create table dbo.DBM_Disk_Perf_last(
	lastSysDate datetime not null
)
go

delete dbo.DBM_Disk_Perf_last
insert into dbo.DBM_Disk_Perf_last(lastSysDate) select getdate()
go

drop procedure dbo.DBM_Disk_Perf_alert
go
create procedure dbo.DBM_Disk_Perf_alert(
	@threshold int = 80 -- ms per read
)
as

declare @lastSysDate  datetime = (select top 1 lastSysDate from dbo.DBM_Disk_Perf_last)
declare @currentSysDate datetime

if object_id('tempdb..#t') is not null
	drop table #t

--select top 10 * from dbo.virtualFileStat

select @@SERVERNAME as 'Server', sysdate, DbName, FileType, FileName, 
 num_of_reads, num_of_bytes_read, io_stall_read_ms, convert(int, case when num_of_reads > 0 then 1.*io_stall_read_ms/num_of_reads else NULL end) as 'ms per read',
 num_of_writes, num_of_bytes_written, io_stall_write_ms, case when num_of_writes > 0 then 1.*io_stall_write_ms/num_of_writes else NULL end as 'ms per write',
 io_stall
into #t
from dbo.virtualFileStat
where 1 = 1
and sysdate > @lastSysDate

if @@rowcount = 0
	return

select top 50
 'Avg duration > ' + convert(varchar(10), @threshold) + ' ms' as 'event', sysdate as 'Period', DbName, [ms per read] as 'Avg duration per Read (ms)' , num_of_reads as 'Total nb reads', FileType, FileName
 from #t
where num_of_reads > 1000
and [ms per read] > @threshold
and datepart(hh, sysdate) between 8 and 19
order by sysdate, [ms per read] desc

select @currentSysDate = max(sysdate) from #t
delete dbo.DBM_Disk_Perf_last
insert into dbo.DBM_Disk_Perf_last(lastSysDate) select @currentSysDate
go

grant execute on dbo.DBM_Disk_Perf_alert to DBMONITOR
go

/*
exec dbo.DBM_Disk_Perf_alert 80

select * from dbo.DBM_Disk_Perf_last
*/