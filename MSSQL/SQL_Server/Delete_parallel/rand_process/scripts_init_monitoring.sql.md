```sql
------------------------------------------------------------------------------------------------------------------
------------------ Задача - параллельно удалять строки в таблице -------------------------------------------------
------------------------------------------------------------------------------------------------------------------

-- без партицилнирования возникают взаимные блокировки

-- вычисление диапазона item_key
-- drop table [core].[item_snapshot_item_key]
select [item_key], IDENTITY(int, 1,1) as id into [core].[item_snapshot_item_key] from core.item_snapshot group by [item_key]


select count(*) cnt
from [core].[item_snapshot_item_key]

select top (10) *, id/10000 as part_id
from [core].[item_snapshot_item_key]
where id > 120000



-- создание функции партиционирования
-- Таблица или индекс в SQL Server могут содержать до 15 000 секций = 40 лет ежедневных партиций
-- DROP PARTITION SCHEME ps_item_snapshot_item_key
-- DROP PARTITION FUNCTION pf_item_snapshot_item_key

create partition function pf_item_snapshot_item_key (int)
as range right for values (0)

declare @sql nvarchar(4000),
		@cnt int = 0

while @cnt < 1000
begin
	set @cnt = @cnt + 1
	set @sql =
	'ALTER PARTITION FUNCTION pf_item_snapshot_item_key ()
	SPLIT RANGE (' + cast(@cnt*10000 as nvarchar(10)) + ')'
	--select @sql
	exec sp_executesql @sql
end

-- Посмотрим что получилось
select *
from sys.partition_functions as pf
	left join sys.partition_range_values as prv on
		prv.function_id = pf.function_id
where name = 'pf_item_snapshot_item_key'

-- создание схемы партиционирования
CREATE PARTITION SCHEME ps_item_snapshot_item_key
AS PARTITION pf_item_snapshot_item_key
ALL TO ('STAT');


-- создание таблицы
-- drop table if exists [core].[item_snapshot_item_key]
create table [core].[item_snapshot_item_key] (
	[item_key] [bigint] not null,
	[id] [int] not null IDENTITY(1,1)
) ON ps_item_snapshot_item_key (id)

insert into [core].[item_snapshot_item_key] 
([item_key])
select 
[item_key]
from core.item_snapshot 
group by [item_key]

-- select по номеру партиции
declare @part_id int = 2

-- insert into [core].[item_snapshot_item_key_check] ([item_key])
select [item_key]
from [core].[item_snapshot_item_key]
where $PARTITION.pf_item_snapshot_item_key (id) = $PARTITION.pf_item_snapshot_item_key(10000*(@part_id - 1))

select @@rowcount


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
	so.object_id = object_id('core.item_snapshot_item_key')
order by
	[Schema.Table], [Index ID], [Partition Function], [Partition #];


-- Даже для партиционированной таблицы при параллельном удалении возникают блокировки

-- будем контролировать конечный результат вставкой в другую таблицу
-- truncate table [core].[item_snapshot_item_key_check]
create table [core].[item_snapshot_item_key_check](
	[item_key] [bigint] NOT NULL
)


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

-- Возможно, уменьшить количество блокировок поможет снятие эскалации блокировок.

alter table [core].[item_snapshot_item_key]
set (LOCK_ESCALATION = DISABLE)

select name, lock_escalation_desc
from sys.tables
where name = 'item_snapshot_item_key'

------------------------------------------------------------------------------------------------------------------
-------------------  Создание структуры таблиц -------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------

-- карта задач
-- drop table [core].[item_snapshot_map_of_tasks]
-- truncate table [core].[item_snapshot_map_of_tasks] 
-- select * from [core].[item_snapshot_map_of_tasks] (nolock)
create table [core].[item_snapshot_map_of_tasks] (
	id int not null identity(1,1),
	spid int default @@spid,
	part_id int,
	insert_dt datetime
	)

-- лог дублей
-- truncate table [core].[item_snapshot_errors]
-- select * from [core].[item_snapshot_errors] (nolock)
create table [core].[item_snapshot_errors] (
	id int not null identity(1,1),
	spid int default @@spid,
	part_id int,
	ErrorNumber int,
	ErrorProcedure nvarchar(255),
	ErrorLine int,
	ErrorMessage nvarchar(255),
	insert_dt datetime
	)

------------------------------------------------------------------------------------------------------------------
-------------------  Проверки  -----------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------

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

-- проверка на взаимные блокировки
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
```