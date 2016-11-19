use msdb
go

drop table dbo.DBM_PLE_last
go
create table dbo.DBM_PLE_last(
	instance_name varchar(128) not null,
	PLE_value bigint not null
)
go
create unique clustered index idx_instance_name on dbo.DBM_PLE_last(instance_name)
go

drop procedure dbo.DBM_memory_buffer_alert
go
create procedure dbo.DBM_memory_buffer_alert
as

declare @PLE_current table(
	instance_name varchar(128) not null,
	PLE_value bigint not null
)

--declare @cntr_value bigint
--select @cntr_value = max(cntr_value)
insert into @PLE_current(instance_name, PLE_value)
select  instance_name, cntr_value
from sys.dm_os_performance_counters
where 1 = 1
and object_name like '%Buffer Node%'
and counter_name = 'Page life expectancy'

select @@SERVERNAME as 'Server', 'Memory pressure ! Data buffer node ' + rtrim(c.instance_name) +  ' was flushed' as 'Alert'
from @PLE_current c
join dbo.DBM_PLE_last l on l.instance_name = c.instance_name
where c.PLE_value < l.PLE_value

delete dbo.DBM_PLE_last

insert into dbo.DBM_PLE_last(instance_name, PLE_value) select instance_name, PLE_value from @PLE_current

/*
declare @cntr_value bigint

select @cntr_value = max(cntr_value)
from sys.dm_os_performance_counters
where 1 = 1
and object_name like '%Buffer Node%'
--and object_name like '%Buffer Manager%'
and counter_name = 'Page life expectancy'

if exists(
	select 1
	from dbo.DBM_PLE_last
	where PLE_value > @cntr_value
)
	select @@SERVERNAME as 'Server', 'Memory pressure ! Data buffer was flushed at ' + convert(varchar(19), dateadd(ss, -@cntr_value, getdate()), 120)  as 'Alert'
else
	select '' as 'Server', '' as 'Alert' where 1 = 0

delete dbo.DBM_PLE_last

insert into dbo.DBM_PLE_last(PLE_value) values (@cntr_value)
*/
go

grant execute on dbo.DBM_memory_buffer_alert to DBMONITOR
go

----------------------------------------------------------------------------------
insert into dbo.DBM_PLE_last(instance_name, PLE_value)
select  instance_name, 99999999
from sys.dm_os_performance_counters
where 1 = 1
and object_name like '%Buffer Node%'
and counter_name = 'Page life expectancy'



--update dbo.DBM_PLE_last set PLE_value = 99999999
--exec dbo.DBM_memory_buffer_alert
-- select * from DBM_PLE_last
select @@servername

select dateadd(ss, -cntr_value, getdate()), *
from sys.dm_os_performance_counters
where counter_name = 'Page life expectancy'
order by 1


select top 10
 count(*) AS cached_pages_count,
 case database_id when 32767 then 'ResourceDb' else db_name(database_id) end as 'DB name',
 8 * count(*) /(1024) as 'Size MB', 8 * count(*) /(1024*1024) as 'Size GB',
 sum (cast ([free_space_in_bytes] as bigint)) / (1024 * 1024) AS 'Empty MB',
 sum (cast ([free_space_in_bytes] as bigint)) / (1024 * 1024 * 1024) AS 'Empty GB'
from sys.dm_os_buffer_descriptors
group by database_id
order by cached_pages_count desc

select * from sys.traces




use [S-money_online-PROD2]
select db_name(), count_big(*) as cached_pages_count, o.name, s.name, 8 * count_big(*) /(1024) as 'MB', 8 * count_big(*) /(1024*1024) as 'GB', 100.*sum(cast(free_space_in_bytes as bigint))/(count_big(*)*1024*8)
from sys.dm_os_buffer_descriptors as bd 
    inner join 
    (
        select p.object_id, index_id ,allocation_unit_id
        from sys.allocation_units as au
            inner join sys.partitions as p 
                on au.container_id = p.hobt_id 
                    and (au.type = 1 or au.type = 3)
        union all
        select p.object_id, index_id, allocation_unit_id
        from sys.allocation_units as au
            inner join sys.partitions as p 
                on au.container_id = p.partition_id 
                    and au.type = 2
    ) as obj 
        on bd.allocation_unit_id = obj.allocation_unit_id
join sys.objects o on o.object_id = obj.object_id
join sys.schemas s on s.schema_id = o.schema_id
where database_id = db_id()
--and o.name = 'ma table'
group by o.name, s.name
order by cached_pages_count desc

select instance_name, cntr_value, dateadd(ss, -cntr_value, getdate())
from sys.dm_os_performance_counters
where 1 = 1
and object_name like '%Buffer Node%'
and counter_name = 'Page life expectancy'
order by cntr_value desc


select top 10 *
from sys.fn_trace_gettable('D:\Microsoft SQL Server\MSSQL10.MSSQLSERVER\MSSQL\LOG\$DBM_Queries_And_Locks_366.trc', 1)
where 1 = 1
and EventClass in (10, 12)
and EndTime > dateadd(mi, -5, GETDATE())
--and EndTime > '2016-06-09T17:43:33.370' and StartTime <  '2016-06-09T17:43:59.370'
--and EndTime > '2016-06-09T17:43:33.370' and StartTime <  '2016-06-09T17:43:59.370'
order by reads desc


select top 10
db_name(convert(int, att.value)) as 'Database',
substring(sql_text.text, b.statement_start_offset/2 + 1, case when b.statement_end_offset = -1 then datalength(sql_text.text) else b.statement_end_offset/2  - b.statement_start_offset/2 + 1 end) as 'Stmt',
db_name(sql_text.dbid) as 'DatabaseObject',
isnull(object_name(sql_text.objectid, sql_text.dbid), '') as 'ObjectName',
b.last_execution_time, 
creation_time as 'first_execution_time',
b.last_physical_reads, b.max_physical_reads, b.total_physical_reads
from sys.dm_exec_query_stats b
cross apply sys.dm_exec_sql_text(b.sql_handle) as sql_text
cross apply sys.dm_exec_plan_attributes (b.plan_handle) as att
where 1 = 1
--and sql_text.text like '%dateadd(mi, -5, getdate())%'
and last_execution_time > dateadd(mi, -5, getdate())
and att.attribute = 'dbid'
order by b.last_physical_reads desc


with v_user_and_cp (user_id)       as (SELECT DISTINCT user_id FROM [izly].[cashing_point] WHERE izly_user_id = @IzlyUserId 
UNION SELECT @IzlyUserId)      ,      ct_ope          as         (       select   id_ope         ,client_or_sender         ,clt_or_snd_parent         ,clt_or_snd_typePro         ,clt_or_snd_nom         ,clt_or_snd_prenom         ,clt_or_rcv_nom         ,clt_or_rcv_prenom         ,clt_or_snd_Crousname         ,clt_or_rcv_Crousname         ,clt_or_snd_email         ,clt_or_snd_izLyEmail         ,clt_or_snd_conEmail         ,clt_or_rcv_email         ,clt_or_rcv_izLyEmail         ,clt_or_rcv_conEmail         ,clt_or_snd_proNom         ,clt_or_snd_proPreNom         ,clt_or_rcv_proNom         ,clt_or_snd_CrousId         ,clt_or_rcv_CrousId         ,clt_or_snd_NumUg         ,clt_or_rcv_NumUg         ,clt_or_snd_NumRu         ,clt_or_rcv_NumRu         ,clt_or_snd_NumUd         ,clt_or_rcv_NumUd         ,clt_or_snd_NumCaisse         ,clt_or_rcv_NumCaisse         ,clt_or_snd_flagPro         ,clt_or_rcv_flagPro         ,clt_or_rcv_flagok         ,somme         ,clt_or_snd_cp_type         ,clt_or_rcv_cp_type         ,idTable         ,crous_support_id         ,operation_mode_id         ,supp_ok         ,client_or_receiver         ,clt_or_rcv_parent         ,datet         ,operationtype         ,clienttransfer         ,moneyexchange         ,paymentrequest         ,ecommerce         ,distributeur         ,commission         ,domainpayment,         ope_is_refused,         ope_is_hidden                 from        scr.opeIzlyUsers_mois operation  with (nolock)       INNER JOIN        @opList opeType       on operation.operationtype = opeType.id_type          and operation.idTable=opeType.id_table       LEFT OUTER JOIN v_user_and_cp v_ucp with (nolock)       on operation.client_or_sender = v_ucp.user_id       LEFT OUTER JOIN v_user_and_cp v_ucp2  with (nolock)       on operation.client_or_receiver = v_ucp2.user_id       where          (         (operation.idtable not in (1,5) AND v_ucp.user_id is not null)         or (operation.idtable  in (1,5) and (v_ucp.user_id is not null OR v_ucp2.user_id is not null))          )                )      , ct_opefiltre as       (        select   row_number()  over(ORDER BY Operation.id_ope DESC)  as RowNum          , Operation.[id_ope] as id          , Operation.[datet]          , Operation.[operationtype]          , Operation.[clienttransfer]          , Operation.[moneyexchange]          , Operation.[paymentrequest]          , Operation.[ecommerce]          , Operation.[distributeur]          , Operation.[commission]            , Operation.[domainpayment]        from ct_ope Operation with (nolock)        where          (@Refused = ope_is_refused) AND  Operation.ope_is_hidden = 0            ),       ct_TOTAL as (select count(1) as TotalRows from ct_opefiltre with (nolock))      SELECT TOP (@PageSize)        RowNum,       [id]       ,[datet]       ,[operationtype]       ,[clienttransfer]       ,[moneyexchange]       ,[paymentrequest]       ,[ecommerce]       ,[distributeur]       ,[commission]         ,[domainpayment]         ,tTotal.TotalRows             FROM ct_opefiltre with (nolock)      CROSS APPLY (select TotalRows from ct_TOTAL) tTotal      WHERE   RowNum BETWEEN ((@CurrentPage-1) * @PageSize) + 1 AND @CurrentPage * @PageSize

--sp_help 'sys.dm_os_performance_counters'


