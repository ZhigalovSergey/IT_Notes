
create procedure [core].[utm_###_sync]
as begin
	set nocount on

	declare @mt_dt datetime2(0) = sysdatetime()
	declare @load_dt datetime2(0) = dateadd(dd, -3, @mt_dt)
	drop table if exists #utm_###

	create table #utm_### (
		utm_key bigint not null,
		### nvarchar(4000) null
	)

	insert into #utm_### (
		utm_key,
		###
	)
	select
		utm_key,
		gbq.TrafficType
	from
		[MDWH_RAW].[raw_gbq168810_g161014_dwh_output].[dimUtm] as gbq
		inner join core.[utm_gbq] as utm on
			gbq.id = utm.[hash]
	where
		gbq.mt_update_dt >= @load_dt

	insert into core.utm_### (
		utm_key,
		###,
		mt_insert_dt,
		mt_update_dt
	)
	select
		utm_key,
		###,
		@mt_dt,
		@mt_dt
	from
		#utm_### as src
	where
		### is not null and
		not exists (
			select
				1
			from
				core.utm_### as tgt
			where
				src.utm_key = tgt.utm_key
		)

	update tgt
	set
		tgt.### = src.###,
		tgt.mt_update_dt = @mt_dt
	from
		core.utm_### as tgt
		inner join #utm_### as src on
			src.utm_key = tgt.utm_key
	where
		not (isnull(tgt.###, '||') = isnull(src.###, '||'))
end
	go


