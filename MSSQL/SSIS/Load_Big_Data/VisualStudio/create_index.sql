use MDWH_SNAPSHOT
go

-- exec create_index 0

create proc create_index
	@task_id int
as begin

	-- truncate table [core].[items_collections_places_snapshot]
	-- declare @task_id int = 24

	declare @st date = (select min([snapshot_date]) from [MDWH].[core].[items_collections_places_snapshot]),
			@fin date = getdate()

	select dateadd(day, 1, eomonth(@st, -1 + @task_id)), dateadd(day, 1, eomonth(@st, @task_id))

	-- select datediff(month, @st, getdate())

	insert into tempdb.dbo.load_log 
		(
			task_id
			,date_from
			,date_to
			,insert_dt
		)
	select 
			@task_id
			,dateadd(day, 1, eomonth(@st, -1 + @task_id))
			,dateadd(day, 1, eomonth(@st, @task_id))
			,getdate()

	drop table if exists [core].[items_collections_places_snapshot_buf]

	declare @sql nvarchar(4000)

	set @sql =
	N'create table [core].[items_collections_places_snapshot_buf] (
		[snapshot_date] [date] not null CHECK ([snapshot_date] >= '''+cast(dateadd(day, 1, eomonth(@st, -1 + @task_id)) as nvarchar(10))+''' AND [snapshot_date] < '''+cast(dateadd(day, 1, eomonth(@st, @task_id)) as nvarchar(10))+'''),
		[collection_id] [BIGINT] not null,
		[item_key] [BIGINT] not null,
		[items_collections_places_type_id] [TINYINT] not null,
		[display_order] [BIGINT] not null,
		[num] [BIGINT] default (1) not null,
		[mt_insert_dt] [DATETIME2](0) not null,
		[mt_update_dt] [DATETIME2](0) not null,
		[mt_delete_dt] [DATETIME2](0) null
	) on STAT'

	-- select @sql

	exec sys.sp_executesql @sql

	alter table core.items_collections_places_snapshot_heap
	switch partition $partition.pf_items_collections_places_snapshot(dateadd(day, 1, eomonth(@st, -1 + @task_id)))
	to [core].[items_collections_places_snapshot_buf] 
	WITH (WAIT_AT_LOW_PRIORITY (MAX_DURATION = 1 MINUTES, ABORT_AFTER_WAIT = BLOCKERS))


	alter table [core].[items_collections_places_snapshot_buf] add constraint [pk_items_collections_places_snapshot_buf] primary key clustered
	(
	[item_key] asc,
	[collection_id] asc,
	[snapshot_date] asc
	) 


	alter table core.items_collections_places_snapshot_buf
	switch to [core].[items_collections_places_snapshot] 
	partition $partition.pf_items_collections_places_snapshot(dateadd(day, 1, eomonth(@st, -1 + @task_id)))


	update tempdb.dbo.load_log 
		set result = 'success'
	where task_id = @task_id

end
