
create procedure [core].[#anchor#_x_#attr#_sync]
as begin
	set nocount on

	declare @mt_dt datetime2(0) = sysdatetime()
	declare @load_dt datetime2(0) = dateadd(dd, -3, @mt_dt)
	
	drop table if exists ##anchor#_x_#attr#
	create table ##anchor#_x_#attr# (
		#anchor#_key bigint not null,
		#anchor#_#attr# #type# null
	)

	insert into ##anchor#_x_#attr# (
		#anchor#_key,
		#anchor#_#attr#
	)
	select
		#anchor#_key,
		src.#src_attr#
	from
		[MDWH_RAW].#schema#.#table# src
		inner join core.#anchor#_s_#src_name# as #anchor# on
			src.#business_key# = #anchor#.#business_key#
	where
		src.mt_update_dt >= @load_dt

	insert into core.#anchor#_x_#attr# (
		#anchor#_key,
		#anchor#_#attr#,
		mt_insert_dt,
		mt_update_dt
	)
	select
		#anchor#_key,
		#anchor#_#attr#,
		@mt_dt,
		@mt_dt
	from
		##anchor#_x_#attr# as src
	where
		#anchor#_#attr# is not null and
		not exists (
			select
				1
			from
				core.#anchor#_x_#attr# as tgt
			where
				src.#anchor#_key = tgt.#anchor#_key
		)

	update tgt
	set
		tgt.#anchor#_#attr# = src.#anchor#_#attr#,
		tgt.mt_update_dt = @mt_dt
	from
		core.#anchor#_x_#attr# as tgt
		inner join ##anchor#_x_#attr# as src on
			src.#anchor#_key = tgt.#anchor#_key
	where
		not exists (select tgt.#anchor#_#attr# intersect select src.#anchor#_#attr#)
end
	go


