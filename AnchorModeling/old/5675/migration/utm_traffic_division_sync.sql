
create procedure [core].[utm_traffic_division_sync]
as begin
	set nocount on

	declare @mt_dt datetime2(0) = sysdatetime()
	declare @load_dt datetime2(0) = dateadd(dd, -3, @mt_dt)
	drop table if exists #utm_traffic_division

	create table #utm_traffic_division (
		utm_key bigint not null,
		traffic_division nvarchar(4000) null
	)

	insert into #utm_traffic_division (
		utm_key,
		traffic_division
	)
	select
		utm_key,
		gbq.TrafficDivision
	from
		[MDWH_RAW].[raw_gbq168810_g161014_dwh_output].[dimUtm] as gbq
		inner join core.[utm_gbq] as utm on
			gbq.id = utm.[hash]
	where
		gbq.mt_update_dt >= @load_dt

	insert into core.utm_traffic_division (
		utm_key,
		traffic_division,
		mt_insert_dt,
		mt_update_dt
	)
	select
		utm_key,
		traffic_division,
		@mt_dt,
		@mt_dt
	from
		#utm_traffic_division as src
	where
		traffic_division is not null and
		not exists (
			select
				1
			from
				core.utm_traffic_division as tgt
			where
				src.utm_key = tgt.utm_key
		)

	update tgt
	set
		tgt.traffic_division = src.traffic_division,
		tgt.mt_update_dt = @mt_dt
	from
		core.utm_traffic_division as tgt
		inner join #utm_traffic_division as src on
			src.utm_key = tgt.utm_key
	where
		not (isnull(tgt.traffic_division, '||') = isnull(src.traffic_division, '||'))
end
	go


