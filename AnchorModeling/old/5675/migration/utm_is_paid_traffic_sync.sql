
create procedure [core].[utm_is_paid_traffic_sync]
as begin
	set nocount on

	declare @mt_dt datetime2(0) = sysdatetime()
	declare @load_dt datetime2(0) = dateadd(dd, -3, @mt_dt)
	drop table if exists #utm_is_paid_traffic

	create table #utm_is_paid_traffic (
		utm_key bigint not null,
		is_paid_traffic nvarchar(4000) null
	)

	insert into #utm_is_paid_traffic (
		utm_key,
		is_paid_traffic
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

	insert into core.utm_is_paid_traffic (
		utm_key,
		is_paid_traffic,
		mt_insert_dt,
		mt_update_dt
	)
	select
		utm_key,
		is_paid_traffic,
		@mt_dt,
		@mt_dt
	from
		#utm_is_paid_traffic as src
	where
		is_paid_traffic is not null and
		not exists (
			select
				1
			from
				core.utm_is_paid_traffic as tgt
			where
				src.utm_key = tgt.utm_key
		)

	update tgt
	set
		tgt.is_paid_traffic = src.is_paid_traffic,
		tgt.mt_update_dt = @mt_dt
	from
		core.utm_is_paid_traffic as tgt
		inner join #utm_is_paid_traffic as src on
			src.utm_key = tgt.utm_key
	where
		not (isnull(tgt.is_paid_traffic, '||') = isnull(src.is_paid_traffic, '||'))
end
	go


