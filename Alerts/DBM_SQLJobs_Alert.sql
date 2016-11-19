use msdb
go

create table dbo.DBM_SQLJob_last(
	instance_id int not null
)
go
delete dbo.DBM_SQLJob_last
declare @current_instance_id int
select @current_instance_id = max(instance_id) from dbo.sysjobhistory with (nolock)
insert into dbo.DBM_SQLJob_last(instance_id) values(@current_instance_id)
go


drop procedure dbo.DBM_SQLJobs_alert
go
create procedure dbo.DBM_SQLJobs_alert
as
declare @current_instance_id int, @last_instance_id int

select @current_instance_id = max(instance_id) from dbo.sysjobhistory with (nolock)
select top 1 @last_instance_id = instance_id from dbo.DBM_SQLJob_last

select top 100 @@SERVERNAME as 'Server', j.name as 'Job name', j.description as 'Job description', jh.step_id, jh.step_name, jh.message, jh.run_date, jh.run_time
from dbo.sysjobhistory jh with (nolock)
left join dbo.sysjobs j on j.job_id = jh.job_id
where jh.instance_id >= @last_instance_id
and jh.instance_id < @current_instance_id
and jh.run_status = 0 -- Failed
and jh.step_id > 0 -- only steps
order by jh.instance_id desc

delete dbo.DBM_SQLJob_last

insert into dbo.DBM_SQLJob_last(instance_id) values(@current_instance_id)
go

grant execute on dbo.DBM_SQLJobs_alert to DBMONITOR
go


/*
exec dbo.DBM_SQLJobs_alert
select * from dbo.DBM_SQLJob_last

delete dbo.DBM_SQLJob_last
--insert into dbo.DBM_SQLJob_last(instance_id) values(6501971) -- 12
insert into dbo.DBM_SQLJob_last(instance_id) values(14782521) -- 17

select top 100 jh.instance_id, j.name as 'JobName', j.description, jh.step_id, jh.step_name, jh.message, jh.run_date, jh.run_time
from dbo.sysjobhistory jh with (nolock)
left join dbo.sysjobs j on j.job_id = jh.job_id
where 1 = 1
and jh.run_status = 0 -- Failed
and jh.step_id > 0 -- only steps
order by jh.instance_id desc


*/