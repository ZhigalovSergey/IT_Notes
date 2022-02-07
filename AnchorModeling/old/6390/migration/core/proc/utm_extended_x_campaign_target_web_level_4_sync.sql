
create procedure [core].[utm_extended_x_campaign_target_web_level_4_sync]
as begin
	set nocount on

	declare @mt_dt datetime2(0) = sysdatetime()
	declare @load_dt datetime2(0) = dateadd(dd, -3, @mt_dt)
	drop table if exists #utm_extended_x_campaign_target_web_level_4

	create table #utm_extended_x_campaign_target_web_level_4 (
		utm_extended_key bigint not null,
		utm_extended_campaign_target_web_level_4 nvarchar(4000) null
	)

	insert into #utm_extended_x_campaign_target_web_level_4 (
		utm_extended_key,
		utm_extended_campaign_target_web_level_4
	)
	select
		utm_extended_key,
		src.CampaignTargetWebLevel4
	from
		[MDWH_RAW].[raw_gbq168810_g161014_dwh_output].[dimUtmExtended] src
		inner join core.[utm_extended_s_gbq] as utm_extended on
			src.id = utm_extended.[utm_extended_hash]
	where
		src.mt_update_dt >= @load_dt

	insert into core.utm_extended_x_campaign_target_web_level_4 (
		utm_extended_key,
		utm_extended_campaign_target_web_level_4,
		mt_insert_dt,
		mt_update_dt
	)
	select
		utm_extended_key,
		utm_extended_campaign_target_web_level_4,
		@mt_dt,
		@mt_dt
	from
		#utm_extended_x_campaign_target_web_level_4 as src
	where
		utm_extended_campaign_target_web_level_4 is not null and
		not exists (
			select
				1
			from
				core.utm_extended_x_campaign_target_web_level_4 as tgt
			where
				src.utm_extended_key = tgt.utm_extended_key
		)

	update tgt
	set
		tgt.utm_extended_campaign_target_web_level_4 = src.utm_extended_campaign_target_web_level_4,
		tgt.mt_update_dt = @mt_dt
	from
		core.utm_extended_x_campaign_target_web_level_4 as tgt
		inner join #utm_extended_x_campaign_target_web_level_4 as src on
			src.utm_extended_key = tgt.utm_extended_key
	where
		not (isnull(tgt.utm_extended_campaign_target_web_level_4, '||') = isnull(src.utm_extended_campaign_target_web_level_4, '||'))
end
	go


