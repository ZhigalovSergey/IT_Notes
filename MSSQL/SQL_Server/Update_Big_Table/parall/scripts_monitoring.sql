
-- Посмотрим что получилось
select *
from sys.partition_functions as pf
	left join sys.partition_range_values as prv on
		prv.function_id = pf.function_id
where name = 'pf_item_snapshot'


-- Посмотрим что получилось
select
	sc.name + N'.' + so.name							 as [Schema.Table],
	si.index_id											 as [Index ID],
	si.type_desc										 as [Structure],
	si.name												 as [Index],
	stat.row_count										 as [Rows],
	stat.in_row_reserved_page_count * 8. / 1024. / 1024. as [In-Row GB],
	stat.lob_reserved_page_count * 8. / 1024. / 1024.	 as [LOB GB],
	p.partition_number									 as [Partition #],
	pf.name												 as [Partition Function],
	case pf.boundary_value_on_right
		when 1
			then 'Right / Lower'
		else 'Left / Upper'
	end													 as [Boundary Type],
	prv.value											 as [Boundary Point],
	fg.name												 as [Filegroup]
from
	sys.partition_functions as pf
	join sys.partition_schemes as ps on
		ps.function_id = pf.function_id
	join sys.indexes as si on
		si.data_space_id = ps.data_space_id
	join sys.objects as so on
		si.object_id = so.object_id
	join sys.schemas as sc on
		so.schema_id = sc.schema_id
	join sys.partitions as p on
		si.object_id = p.object_id and
		si.index_id = p.index_id
	left join sys.partition_range_values as prv on
		prv.function_id = pf.function_id and
		p.partition_number =
			case pf.boundary_value_on_right
				when 1
					then prv.boundary_id + 1
				else prv.boundary_id
			end
	/* For left-based functions, partition_number = boundary_id, 
       for right-based functions we need to add 1 */
	join sys.dm_db_partition_stats as stat on
		stat.object_id = p.object_id and
		stat.index_id = p.index_id and
		stat.index_id = p.index_id and
		stat.partition_id = p.partition_id and
		stat.partition_number = p.partition_number
	join sys.allocation_units as au on
		au.container_id = p.hobt_id and
		au.type_desc = 'IN_ROW_DATA'
	/* Avoiding double rows for columnstore indexes. */
	/* We can pick up LOB page count from partition_stats */
	join sys.filegroups as fg on
		fg.data_space_id = au.data_space_id
where
	so.object_id = object_id('core.item_snapshot')
order by
	[Schema.Table], [Index ID], [Partition Function], [Partition #];



-- Итоговая статистика
exec sp_spaceused
	@objname = N'core.item_snapshot'
	,@updateusage = N'TRUE';

-- Размер индекса
select
	tn.[name]					  as [Table name],
	ix.[name]					  as [Index name],
	sum(sz.[used_page_count]) * 8 as [Index size (KB)]
from
	sys.dm_db_partition_stats as sz
	inner join sys.indexes as ix on
		sz.[object_id] = ix.[object_id] and
		sz.[index_id] = ix.[index_id]
	inner join sys.tables tn on
		tn.OBJECT_ID = ix.object_id
where ix.[name] = N'ix_item_snapshot_item_key'
group by tn.[name], ix.[name]


-- мониторинг лога
with
cte as (
	select
		row_number() over (partition by iter_number order by insert_dt desc)																	  rn,
		datediff(ms, lag(insert_dt, 1) over (partition by iter_number order by insert_dt), insert_dt)											  diff_dt,
		inserted_item_snapshot_tgt / datediff(ms, lag(insert_dt, 1) over (partition by iter_number order by insert_dt), insert_dt) speed,
		*
	from
		[core].[item_snapshot_log] (nolock)
	where
		comment like 'filled item_snapshot_buf_%' or
		comment like 'filled item_snapshot_tgt%'
)
select * from cte where rn = 1 order by insert_dt desc


-- 4 sessions - 115 rows per sec - появились блокировки при вставке в tgt - 
-- 2 sessions - 115 rows per sec
-- 1 sessions - 100 rows per sec

-- средняя скорость за последнии 100 вставок
with cte as
(
select *, row_number() over (partition by 1 order by insert_dt desc) rn
from [core].[item_snapshot_log] (nolock)
where comment like 'filled item_snapshot_tgt%'
)
select sum(inserted_item_snapshot_tgt)/datediff(ms, min(insert_dt), max(insert_dt)) as speed, min(insert_dt), max(insert_dt)
from cte 
where rn < 100


-- средняя время на шаг
with cte as
(
select *
		, datediff(ms, lag(insert_dt, 1) over (partition by spid, iter_number order by insert_dt), insert_dt) diff_dt
		, row_number() over (partition by 1 order by insert_dt desc) rn
from [core].[item_snapshot_log] (nolock)
)
select step, comment, sum(diff_dt)/count(diff_dt) as avg_dt
from cte
group by step, comment
order by step, comment


-- доля времени на шаг
with cte as
(
select *
		, datediff(ms, lag(insert_dt, 1) over (partition by spid, iter_number order by insert_dt), insert_dt) diff_dt
		, row_number() over (partition by 1 order by insert_dt desc) rn
from [core].[item_snapshot_log] (nolock)
)
select step, sum(diff_dt)/count(diff_dt) as avg_dt, sum(diff_dt)/count(diff_dt)*100/sum(sum(diff_dt)/count(diff_dt)) over (partition by 1) as 'percent of total'
from cte
group by step
order by step



select *
from [core].[item_snapshot_log] (nolock)
order by insert_dt desc


-- нагрузка на лог транзакций при вставке
-- group by each operation
SELECT Operation, AllocUnitName, Context, count(*)
FROM sys.fn_dblog(NULL,NULL)
WHERE AllocUnitName LIKE 'core.item_snapshot_tgt'
GROUP BY Operation, AllocUnitName, Context

------------------------------------------------------------------------------------------------------------------
-------------------  Проверки  -----------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------


-- контроль конечного результата вставки
select sum(src_cnt) as src_cnt, sum(check_cnt) as check_cnt, sum(src_cnt) - sum(check_cnt) as diff
from
(
select 0 as src_cnt, count(*) check_cnt
from [core].[item_snapshot_item_key_check]
union all
select count(*) cnt, 0
from [core].[item_snapshot_item_key]
) t


select [item_key]
from [core].[item_snapshot_item_key] src
where not exists (select top (1) 1
					from [core].[item_snapshot_item_key_check] tgt 
					where tgt.[item_key] = src.item_key)


select min(item_key) mn, max(item_key) mx, count(*) cnt
from [core].[item_snapshot_item_key]


-- просмотр лога дублей
-- truncate table [core].[item_snapshot_errors]
select * from [core].[item_snapshot_errors] (nolock)

-- сравнение разных уровней изоляции
select * 
from [core].[item_snapshot_map_of_tasks] (nolock) -- или (readuncommitted)
order by insert_dt desc

select * 
from [core].[item_snapshot_map_of_tasks] (readcommitted)
order by id desc


-- truncate table [core].[item_snapshot_map_of_tasks]
-- проверка на дубликаты задач
select *
from
	(
		select *, row_number() over (partition by part_id order by id) rn
		from [core].[item_snapshot_map_of_tasks]
	) t
where rn > 1




-------------------------------------------------------------------------------------------------------------
------------------------ проверка на взаимные блокировки   --------------------------------------------------
-------------------------------------------------------------------------------------------------------------

exec sp_whoisactive

--Монторинг выполняемых запросов
set transaction isolation level read uncommitted
select
	es.session_id												[sid],
	er.blocking_session_id										as blocking_sid,
	cast(er.total_elapsed_time / 1000. / 60. as decimal(34, 2)) as elapsed_time_min,
	es.[host_name],
	es.login_name,
	er.[status],
	er.command,
	db_name(er.database_id)										as [database_name],
	substring(qt.[text], (er.statement_start_offset / 2) + 1,
	((case
		when er.statement_end_offset = -1
			then len(convert(nvarchar(max), qt.[text])) * 2
		else er.statement_end_offset
	end - er.statement_start_offset) / 2) + 1)					as [individual_query],
	qt.[text]													as parent_query,
	es.[program_name],
	er.start_time,
	qp.query_plan,
	er.wait_type,
	er.cpu_time,
	er.logical_reads,
	er.open_transaction_count,
	er.last_wait_type,
	er.percent_complete,
	mg.requested_memory_kb / 1024								as requested_memory_MB,
	mg.granted_memory_kb / 1024									as granted_memory_MB,
	qp.query_plan
from
	sys.dm_exec_requests as er
	inner join sys.dm_exec_sessions as es on
		er.session_id = es.session_id
	left join sys.dm_exec_query_memory_grants as mg on
		es.session_id = mg.session_id
	cross apply sys.dm_exec_sql_text(er.[sql_handle]) as qt
	outer apply sys.dm_exec_query_plan(er.plan_handle) as qp
where
	--es.is_user_process = 1 and
	es.session_id not in (@@spid)
order by
	es.login_name
--er.logical_reads desc
--[individual_query], er.session_id desc
option (recompile);
