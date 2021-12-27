
create procedure [core].[utm_campaign_target_web_level4_sync]
as begin
	set nocount on

	declare @mt_dt datetime2(0) = sysdatetime()
	declare @load_dt datetime2(0) = dateadd(dd, -3, @mt_dt)
	drop table if exists #utm_campaign_target_web_level4

	create table #utm_campaign_target_web_level4 (
		utm_key bigint not null,
		campaign_target_web_level4 nvarchar(4000) null
	)

	insert into #utm_campaign_target_web_level4 (
		utm_key,
		campaign_target_web_level4
	)
	select
		utm_key,
		gbq.CampaignTargetWebLevel4
	from
		[MDWH_RAW].[raw_gbq168810_g161014_dwh_output].[dimUtm] as gbq
		inner join core.[utm_gbq] as utm on
			gbq.id = utm.[hash]
	where
		gbq.mt_update_dt >= @load_dt

	insert into core.utm_campaign_target_web_level4 (
		utm_key,
		campaign_target_web_level4,
		mt_insert_dt,
		mt_update_dt
	)
	select
		utm_key,
		campaign_target_web_level4,
		@mt_dt,
		@mt_dt
	from
		#utm_campaign_target_web_level4 as src
	where
		campaign_target_web_level4 is not null and
		not exists (
			select
				1
			from
				core.utm_campaign_target_web_level4 as tgt
			where
				src.utm_key = tgt.utm_key
		)

	update tgt
	set
		tgt.campaign_target_web_level4 = src.campaign_target_web_level4,
		tgt.mt_update_dt = @mt_dt
	from
		core.utm_campaign_target_web_level4 as tgt
		inner join #utm_campaign_target_web_level4 as src on
			src.utm_key = tgt.utm_key
	where
		not (isnull(tgt.campaign_target_web_level4, '||') = isnull(src.campaign_target_web_level4, '||'))
end
	go

