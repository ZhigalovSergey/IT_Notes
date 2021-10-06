```sql
-- select * from tempdb.dbo.map_of_tasks order by insert_start_dt

/*

-- delete
-- select *
from tempdb.dbo.map_of_tasks
where result = 'failed'

truncate table core.items_collections_places_snapshot
with (partitions (1 to 11));
go

*/

/*

update tempdb.dbo.map_of_tasks
	set status = 'stop'
where task_id = 1



with cte as
(
	select status, row_number() over (partition by ExecutionInstanceGUID order by task_id desc) rn
	from tempdb.dbo.map_of_tasks
	where ExecutionInstanceGUID = '{81EFF91A-FFB1-4B41-87D2-383376F6F6A6}'
)
select status
from cte
where rn = 1

*/

-- truncate table tempdb.dbo.map_of_tasks

-- drop table if exists tempdb.dbo.map_of_tasks


create table tempdb.dbo.map_of_tasks (
	id int not null identity (1, 1),
	ExecutionInstanceGUID nvarchar(128),
	task_id int,
	insert_start_dt datetime,
	insert_finish_dt datetime,
	result nvarchar(128),
	status nvarchar(128) default N'running' --N'stop' --N'running'
)

-- select * from tempdb.dbo.load_log order by insert_start_dt
/*

select 
	(
		select sum(pstats.row_count) sm
		from sys.dm_db_partition_stats as pstats
		where pstats.object_id = object_id('core.items_collections_places_snapshot')
	)
	/datediff(ms, min(insert_start_dt), max(insert_finish_dt)) pages_per_ms from tempdb.dbo.load_log
*/
-- 608 pages_per_ms


-- truncate table tempdb.dbo.load_log

-- drop table if exists tempdb.dbo.load_log

create table tempdb.dbo.load_log (
	id int not null identity (1, 1),
	task_id int,
	date_from date,
	date_to date,
	insert_start_dt datetime,
	insert_finish_dt datetime,
	result nvarchar(255)
)
```