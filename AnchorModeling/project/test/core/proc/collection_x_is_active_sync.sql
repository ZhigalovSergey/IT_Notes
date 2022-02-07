
create procedure [core].[collection_x_is_active_sync]
as begin
	set nocount on

	declare @mt_dt datetime2(0) = sysdatetime()
	declare @load_dt datetime2(0) = dateadd(dd, -3, @mt_dt)
	
	drop table if exists #collection_x_is_active
	create table #collection_x_is_active (
		collection_key bigint not null,
		collection_is_active bit not null
	)

	insert into #collection_x_is_active (
		collection_key,
		collection_is_active
	)
	select
		collection_key,
		src.is_active
	from
		[MDWH_RAW].raw_malibu_malibu_malibu.collections src
		inner join core.collection_s_malibu as collection on
			src.collection_id = collection.collection_id
	where
		src.mt_update_dt >= @load_dt

	insert into core.collection_x_is_active (
		collection_key,
		collection_is_active,
		mt_insert_dt,
		mt_update_dt
	)
	select
		collection_key,
		collection_is_active,
		@mt_dt,
		@mt_dt
	from
		#collection_x_is_active as src
	where
		collection_is_active is not null and
		not exists (
			select
				1
			from
				core.collection_x_is_active as tgt
			where
				src.collection_key = tgt.collection_key
		)

	update tgt
	set
		tgt.collection_is_active = src.collection_is_active,
		tgt.mt_update_dt = @mt_dt
	from
		core.collection_x_is_active as tgt
		inner join #collection_x_is_active as src on
			src.collection_key = tgt.collection_key
	where
		not exists (select tgt.collection_is_active intersect select src.collection_is_active)
end
	go


