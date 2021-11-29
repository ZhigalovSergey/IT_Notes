
create procedure [core].[utm_traffic_is_paid_sync]
as begin
	set nocount on

	declare @mt_dt datetime2(0) = sysdatetime()
	declare @load_dt datetime2(0) = dateadd(dd, -3, @mt_dt)
	drop table if exists #utm_traffic_is_paid

	create table #utm_traffic_is_paid (
		utm_key bigint not null,
		traffic_is_paid nvarchar(4000) null
	)

	insert into #utm_traffic_is_paid (
		utm_key,
		traffic_is_paid
	)
	select
		utm_key,
		gbq.IsPaidTraffic
	from
		[MDWH_RAW].[raw_gbq168810_g161014_dwh_output].[dimUtm] as gbq
		inner join core.[utm_gbq] as utm on
			gbq.id = utm.[hash]
	where
		gbq.mt_update_dt >= @load_dt

	create unique clustered index cix on #utm_traffic_is_paid (utm_key)

	insert into core.utm_traffic_is_paid (
		utm_key,
		traffic_is_paid,
		mt_insert_dt,
		mt_update_dt
	)
	select
		utm_key,
		traffic_is_paid,
		@mt_dt,
		@mt_dt
	from
		#utm_traffic_is_paid as src
	where
		traffic_is_paid is not null and
		not exists (
			select
				1
			from
				core.utm_traffic_is_paid as tgt
			where
				src.utm_key = tgt.utm_key
		)

	update tgt
	set
		tgt.traffic_is_paid = src.traffic_is_paid,
		tgt.mt_update_dt = @mt_dt
	from
		core.utm_traffic_is_paid as tgt
		inner join #utm_traffic_is_paid as src on
			src.utm_key = tgt.utm_key
	where
		not (isnull(tgt.traffic_is_paid, '||') = isnull(src.traffic_is_paid, '||'))
end
	go


