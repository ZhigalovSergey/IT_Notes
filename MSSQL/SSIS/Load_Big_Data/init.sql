
-- select * from tempdb.dbo.map_of_tasks

-- drop table if exists tempdb.dbo.map_of_tasks

create table tempdb.dbo.map_of_tasks (
	id int not null identity (1, 1),
	ExecutionInstanceGUID nvarchar(128),
	task_id int,
	insert_dt datetime,
	result nvarchar(128)
)

-- select * from tempdb.dbo.load_log

-- drop table if exists tempdb.dbo.load_log

create table tempdb.dbo.load_log (
	id int not null identity (1, 1),
	task_id int,
	date_from date,
	date_to date,
	insert_dt datetime,
	result nvarchar(128)
)
