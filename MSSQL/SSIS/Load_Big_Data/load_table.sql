
use MDWH_SNAPSHOT
go

/*

select [snapshot_date], count(*) cnt
from [core].items_collections_places_snapshot
group by [snapshot_date] 
order by [snapshot_date] 

*/

-- exec load_table 0

alter proc load_table
	@task_id int
as begin

	-- truncate table [core].[items_collections_places_snapshot]

	declare @st date = (select min([snapshot_date]) from [MDWH].[core].[items_collections_places_snapshot]),
			@fin date = getdate()

	-- select datediff(month, @st, getdate())

	insert into tempdb.dbo.load_log 
		(
			task_id
			,date_from
			,date_to
		)
	select 
			@task_id
			,dateadd(day, 1, eomonth(@st, -1 + @task_id))
			,dateadd(day, 1, eomonth(@st, @task_id))

	insert into [core].items_collections_places_snapshot with (tablock) (
		[snapshot_date],
		[collection_id],
		[item_key],
		[items_collections_places_type_id],
		[display_order],
		[num],
		[mt_insert_dt],
		[mt_update_dt],
		[mt_delete_dt]
	)
	select
		[snapshot_date],
		[collection_id],
		[item_key],
		[items_collections_places_type_id],
		[display_order],
		[num],
		[mt_insert_dt],
		[mt_update_dt],
		[mt_delete_dt]
	from
		[MDWH].[core].[items_collections_places_snapshot]
	where
		snapshot_date >= dateadd(day, 1, eomonth(@st, -1 + @task_id)) and
		snapshot_date < dateadd(day, 1, eomonth(@st, @task_id))

	update tempdb.dbo.load_log 
		set result = 'success'
	where task_id = @task_id

end