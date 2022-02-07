
create procedure [core].[collection_x_collection_type_id_sync]
as begin
	set nocount on

	declare @mt_dt datetime2(0) = sysdatetime()
	declare @load_dt datetime2(0) = dateadd(dd, -3, @mt_dt)
	
	drop table if exists #collection_x_collection_type_id
	create table #collection_x_collection_type_id (
		collection_key bigint not null,
		collection_collection_type_id int not null
	)

	insert into #collection_x_collection_type_id (
		collection_key,
		collection_collection_type_id
	)
	select
		collection_key,
		src.collection_type_id
	from
		[MDWH_RAW].raw_malibu_malibu_malibu.collections src
		inner join core.collection_s_malibu as collection on
			src.collection_id = collection.collection_id
	where
		src.mt_update_dt >= @load_dt

	insert into core.collection_x_collection_type_id (
		collection_key,
		collection_collection_type_id,
		mt_insert_dt,
		mt_update_dt
	)
	select
		collection_key,
		collection_collection_type_id,
		@mt_dt,
		@mt_dt
	from
		#collection_x_collection_type_id as src
	where
		collection_collection_type_id is not null and
		not exists (
			select
				1
			from
				core.collection_x_collection_type_id as tgt
			where
				src.collection_key = tgt.collection_key
		)

	update tgt
	set
		tgt.collection_collection_type_id = src.collection_collection_type_id,
		tgt.mt_update_dt = @mt_dt
	from
		core.collection_x_collection_type_id as tgt
		inner join #collection_x_collection_type_id as src on
			src.collection_key = tgt.collection_key
	where
		not exists (select tgt.collection_collection_type_id intersect select src.collection_collection_type_id)
end
	go


