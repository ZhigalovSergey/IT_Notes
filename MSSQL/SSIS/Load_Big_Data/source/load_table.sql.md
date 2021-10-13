```sql
use [MDWH_Archive]
go

/*

declare @st date = (select min([snapshot_date]) from [MDWH].[core].[items_collections_places_snapshot])

select [snapshot_date], count(*) cnt
from [core].items_collections_places_snapshot
where [snapshot_date] >= dateadd(day, 1, eomonth(@st, 16)) and
		snapshot_date < dateadd(day, 1, eomonth(@st, 17))
group by [snapshot_date] 
order by [snapshot_date] 

*/

/*
declare @result nvarchar(255)
exec load_table 0, @result output
select @result
*/

--create 
alter proc load_table
	@task_id int,
	@result nvarchar(128) output
as begin

	-- truncate table [core].[items_collections_places_snapshot]

	declare @st date = '20190701', --(select min([snapshot_date]) from [MDWH_SNAPSHOT].[core].[items_collections_places_snapshot] where [snapshot_date] < '20190801'),
			@fin date = getdate()

	--select @st, @fin
	-- select datediff(month, @st, getdate())
	
	declare @date_from date, @date_to date
	set @date_from = dateadd(day, 1, eomonth(@st, -1 + @task_id))
	set @date_to = dateadd(day, 1, eomonth(@st, @task_id))
	insert into tempdb.dbo.load_log 
		(
			task_id
			,date_from
			,date_to
			,insert_start_dt
		)
	select 
			@task_id
			,@date_from
			,@date_to
			,getdate()

	if (
		select
			pstats.row_count
		from
			sys.dm_db_partition_stats as pstats
		where
			pstats.object_id = object_id('core.items_collections_places_snapshot') and
			pstats.partition_number = $partition.[pf_items_collections_places_snapshot](convert(nvarchar(10), @date_from, 112))
		) > 0
		begin
			update tempdb.dbo.load_log 
				set result = 'failed: The specified partition of target table is not empty',
					insert_finish_dt = getdate()
			where task_id = @task_id
			set @result = 'failed'	
			return
		end

	begin try 

		declare @sql nvarchar(4000)

		set @sql = N'drop table if exists core.items_collections_places_snapshot_' + cast(@task_id as nvarchar)

		exec sp_executesql @sql

		set @sql = 
			N'create table [core].[items_collections_places_snapshot_' + cast(@task_id as nvarchar) + '] (
			[snapshot_date] [DATE] not null CHECK ([snapshot_date] >= ''' + convert(nvarchar(10), @date_from, 112 ) + ''' AND [snapshot_date] < ''' + convert(nvarchar(10), @date_to, 112 ) + '''),
			[collection_id] [BIGINT] not null,
			[item_key] [BIGINT] not null,
			[items_collections_places_type_id] [TINYINT] not null,
			[display_order] [BIGINT] not null,
			[num] [BIGINT] not null default (1),
			[mt_insert_dt] [DATETIME2](0) not null,
			[mt_update_dt] [DATETIME2](0) not null,
			[mt_delete_dt] [DATETIME2](0) null
			) on [STAT]'

		--select @sql
		exec sp_executesql @sql

		set @sql = 
			N'insert into [core].items_collections_places_snapshot_' + cast(@task_id as nvarchar) + ' with (tablock) 
			(
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
				[MDWH_SNAPSHOT].[core].[items_collections_places_snapshot]
			where num <= 210 and 
				snapshot_date >= ''' + convert(nvarchar(10), @date_from, 112 ) + ''' and
				snapshot_date < ''' + convert(nvarchar(10), @date_to, 112 ) + ''''

		exec sp_executesql @sql

		set @sql =
			N'alter table [core].[items_collections_places_snapshot_' + cast(@task_id as nvarchar) + '] add constraint [pk_items_collections_places_snapshot_' + cast(@task_id as nvarchar) + '] primary key clustered
			(
			[snapshot_date] asc,
			[item_key] asc,
			[collection_id] asc
			)'

		exec sp_executesql @sql

		set @sql =
			N'alter table [core].[items_collections_places_snapshot_' + cast(@task_id as nvarchar) + ']
			switch to [core].[items_collections_places_snapshot] partition $partition.[pf_items_collections_places_snapshot](''' + convert(nvarchar(10), @date_from, 112 ) + ''')'

		exec sp_executesql @sql

		set @sql = N'drop table core.items_collections_places_snapshot_' + cast(@task_id as nvarchar)

		exec sp_executesql @sql

		update tempdb.dbo.load_log 
			set result = 'success',
				insert_finish_dt = getdate()
		where task_id = @task_id
		set @result = 'success'
	end try
	begin catch
		update tempdb.dbo.load_log 
			set result = 'failed: ' + error_message(),
				insert_finish_dt = getdate()
		where task_id = @task_id

		set @sql = N'drop table if exists core.items_collections_places_snapshot_' + cast(@task_id as nvarchar)

		exec sp_executesql @sql

		set @result = 'failed'
	end catch
end

```