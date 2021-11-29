
create procedure [core].[utm_traffic_subcategory_sync]
as begin
	set nocount on

	declare @mt_dt datetime2(0) = sysdatetime()
	declare @load_dt datetime2(0) = dateadd(dd, -3, @mt_dt)
	drop table if exists #utm_traffic_subcategory

	create table #utm_traffic_subcategory (
		utm_key bigint not null,
		traffic_subcategory nvarchar(4000) null
	)

	insert into #utm_traffic_subcategory (
		utm_key,
		traffic_subcategory
	)
	select
		utm_key,
		gbq.TrafficSubcategory
	from
		[MDWH_RAW].[raw_gbq168810_g161014_dwh_output].[dimUtm] as gbq
		inner join core.[utm_gbq] as utm on
			gbq.id = utm.[hash]
	where
		gbq.mt_update_dt >= @load_dt

	create unique clustered index cix on #utm_traffic_subcategory (utm_key)

	insert into core.utm_traffic_subcategory (
		utm_key,
		traffic_subcategory,
		mt_insert_dt,
		mt_update_dt
	)
	select
		utm_key,
		traffic_subcategory,
		@mt_dt,
		@mt_dt
	from
		#utm_traffic_subcategory as src
	where
		traffic_subcategory is not null and
		not exists (
			select
				1
			from
				core.utm_traffic_subcategory as tgt
			where
				src.utm_key = tgt.utm_key
		)

	update tgt
	set
		tgt.traffic_subcategory = src.traffic_subcategory,
		tgt.mt_update_dt = @mt_dt
	from
		core.utm_traffic_subcategory as tgt
		inner join #utm_traffic_subcategory as src on
			src.utm_key = tgt.utm_key
	where
		not (isnull(tgt.traffic_subcategory, '||') = isnull(src.traffic_subcategory, '||'))
end
	go


