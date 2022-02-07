
create procedure [core].[collection_x_collection_display_name_sync]
as begin
	set nocount on

	declare @mt_dt datetime2(0) = sysdatetime()
	declare @load_dt datetime2(0) = dateadd(dd, -3, @mt_dt)
	
	drop table if exists #collection_x_collection_display_name
	create table #collection_x_collection_display_name (
		collection_key bigint not null,
		collection_collection_display_name nvarchar(255) not null
	)

	insert into #collection_x_collection_display_name (
		collection_key,
		collection_collection_display_name
	)
	select
		collection_key,
		src.collection_display_name
	from
		[MDWH_RAW].raw_malibu_malibu_malibu.collections src
		inner join core.collection_s_malibu as collection on
			src.collection_id = collection.collection_id
	where
		src.mt_update_dt >= @load_dt

	insert into core.collection_x_collection_display_name (
		collection_key,
		collection_collection_display_name,
		mt_insert_dt,
		mt_update_dt
	)
	select
		collection_key,
		collection_collection_display_name,
		@mt_dt,
		@mt_dt
	from
		#collection_x_collection_display_name as src
	where
		collection_collection_display_name is not null and
		not exists (
			select
				1
			from
				core.collection_x_collection_display_name as tgt
			where
				src.collection_key = tgt.collection_key
		)

	update tgt
	set
		tgt.collection_collection_display_name = src.collection_collection_display_name,
		tgt.mt_update_dt = @mt_dt
	from
		core.collection_x_collection_display_name as tgt
		inner join #collection_x_collection_display_name as src on
			src.collection_key = tgt.collection_key
	where
		not exists (select tgt.collection_collection_display_name intersect select src.collection_collection_display_name)
end
	go


