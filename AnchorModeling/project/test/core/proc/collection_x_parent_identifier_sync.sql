
create procedure [core].[collection_x_parent_identifier_sync]
as begin
	set nocount on

	declare @mt_dt datetime2(0) = sysdatetime()
	declare @load_dt datetime2(0) = dateadd(dd, -3, @mt_dt)
	
	drop table if exists #collection_x_parent_identifier
	create table #collection_x_parent_identifier (
		collection_key bigint not null,
		collection_parent_identifier nvarchar(255) not null
	)

	insert into #collection_x_parent_identifier (
		collection_key,
		collection_parent_identifier
	)
	select
		collection_key,
		src.parent_identifier
	from
		[MDWH_RAW].raw_malibu_malibu_malibu.collections src
		inner join core.collection_s_malibu as collection on
			src.collection_id = collection.collection_id
	where
		src.mt_update_dt >= @load_dt

	insert into core.collection_x_parent_identifier (
		collection_key,
		collection_parent_identifier,
		mt_insert_dt,
		mt_update_dt
	)
	select
		collection_key,
		collection_parent_identifier,
		@mt_dt,
		@mt_dt
	from
		#collection_x_parent_identifier as src
	where
		collection_parent_identifier is not null and
		not exists (
			select
				1
			from
				core.collection_x_parent_identifier as tgt
			where
				src.collection_key = tgt.collection_key
		)

	update tgt
	set
		tgt.collection_parent_identifier = src.collection_parent_identifier,
		tgt.mt_update_dt = @mt_dt
	from
		core.collection_x_parent_identifier as tgt
		inner join #collection_x_parent_identifier as src on
			src.collection_key = tgt.collection_key
	where
		not exists (select tgt.collection_parent_identifier intersect select src.collection_parent_identifier)
end
	go


