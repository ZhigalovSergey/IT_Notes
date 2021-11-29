--use [mdwh]
--go

-- drop proc [dbo].[utm_sync]
-- exec [dbo].[utm_sync]

create procedure [dbo].[utm_sync]
as begin
	set nocount on;

	declare @mt_dt datetime2(0) = sysdatetime()
	declare @load_dt datetime2(0) = dateadd(dd, -3, @mt_dt)

	declare @exec_guid uniqueidentifier = newid()
	declare @proc_id bigint = @@procid

	execute maintenance.insertexecution
		@step = N'start dbo.utm',
		@execguid = @exec_guid,
		@procid = @proc_id,
		@rows = @@rowcount;

	insert into dbo.utm (
		[utm_key],
		[medium],
		[source],
		[campaign],
		[hash],
		[traffic_type],
		[traffic_category],
		[traffic_subcategory],
		[traffic_division],
		[traffic_channel],
		[is_paid_traffic],
		[campaign_target_category_id],
		[campaign_target_web_level1],
		[campaign_target_web_level2],
		[campaign_target_web_level3],
		[campaign_target_web_level4],
		[campaign_target_web_level5],
		[campaign_target_web_level6],
		[mt_insert_dt],
		[mt_update_dt]
	)
	select
		src.utm_key,
		null   as [medium],
		null   as [source],
		null   as [campaign],
		null   as [hash],
		null   as [traffic_type],
		null   as [traffic_category],
		null   as [traffic_subcategory],
		null   as [traffic_division],
		null   as [traffic_channel],
		null   as [is_paid_traffic],
		null   as [campaign_target_category_id],
		null   as [campaign_target_web_level1],
		null   as [campaign_target_web_level2],
		null   as [campaign_target_web_level3],
		null   as [campaign_target_web_level4],
		null   as [campaign_target_web_level5],
		null   as [campaign_target_web_level6],
		@mt_dt as mt_insert_dt,
		@mt_dt as mt_update_dt
	from
		core.utm as src
	where
		src.mt_insert_dt >= @load_dt and
		not exists (
			select
				1
			from
				dbo.utm as utm
			where
				utm.utm_key = src.utm_key
		)

	execute maintenance.insertexecution
		@step = N'insert into ',
		@execguid = @exec_guid,
		@procid = @proc_id,
		@rows = @@rowcount;

	update tgt
	set
		tgt.[medium] = src.[medium],
		tgt.[source] = src.[source],
		tgt.[campaign] = src.[campaign],
		tgt.[hash] = src.[hash],
		tgt.mt_update_dt = @mt_dt
	from
		dbo.utm as tgt
		inner join core.utm_gbq as src on
			tgt.utm_key = src.utm_key
	where
		tgt.mt_update_dt >= @load_dt
		and not (isnull(tgt.[medium], '||') = isnull(src.[medium], '||')
			and isnull(tgt.[source], '||') = isnull(src.[source], '||')
			and isnull(tgt.[campaign], '||') = isnull(src.[campaign], '||')
			and isnull(tgt.[hash], '||') = isnull(src.[hash], '||'))

	execute maintenance.insertexecution
		@step = N'update hash (source, medium, campaign)',
		@execguid = @exec_guid,
		@procid = @proc_id,
		@rows = @@rowcount;

	update tgt
	set
		tgt.traffic_type = src.traffic_type,
		tgt.mt_update_dt = @mt_dt
	from
		dbo.utm as tgt
		inner join core.utm_traffic_type as src on
			tgt.utm_key = src.utm_key
	where
		tgt.mt_update_dt >= @load_dt
		and not (isnull(tgt.traffic_type, '||') = isnull(src.traffic_type, '||'))

	execute maintenance.insertexecution
		@step = N'update traffic_type',
		@execguid = @exec_guid,
		@procid = @proc_id,
		@rows = @@rowcount;

	update tgt
	set
		tgt.traffic_category = src.traffic_category,
		tgt.mt_update_dt = @mt_dt
	from
		dbo.utm as tgt
		inner join core.utm_traffic_category as src on
			tgt.utm_key = src.utm_key
	where
		tgt.mt_update_dt >= @load_dt
		and not (isnull(tgt.traffic_category, '||') = isnull(src.traffic_category, '||'))

	execute maintenance.insertexecution
		@step = N'update traffic_category',
		@execguid = @exec_guid,
		@procid = @proc_id,
		@rows = @@rowcount;

	update tgt
	set
		tgt.traffic_subcategory = src.traffic_subcategory,
		tgt.mt_update_dt = @mt_dt
	from
		dbo.utm as tgt
		inner join core.utm_traffic_subcategory as src on
			tgt.utm_key = src.utm_key
	where
		tgt.mt_update_dt >= @load_dt
		and not (isnull(tgt.traffic_subcategory, '||') = isnull(src.traffic_subcategory, '||'))

	execute maintenance.insertexecution
		@step = N'update traffic_subcategory',
		@execguid = @exec_guid,
		@procid = @proc_id,
		@rows = @@rowcount;

	update tgt
	set
		tgt.traffic_division = src.traffic_division,
		tgt.mt_update_dt = @mt_dt
	from
		dbo.utm as tgt
		inner join core.utm_traffic_division as src on
			tgt.utm_key = src.utm_key
	where
		tgt.mt_update_dt >= @load_dt
		and not (isnull(tgt.traffic_division, '||') = isnull(src.traffic_division, '||'))

	execute maintenance.insertexecution
		@step = N'update traffic_divisioN',
		@execguid = @exec_guid,
		@procid = @proc_id,
		@rows = @@rowcount;


	update tgt
	set
		tgt.traffic_channel = src.traffic_channel,
		tgt.mt_update_dt = @mt_dt
	from
		dbo.utm as tgt
		inner join core.utm_traffic_channel as src on
			tgt.utm_key = src.utm_key
	where
		tgt.mt_update_dt >= @load_dt
		and not (isnull(tgt.traffic_channel, '||') = isnull(src.traffic_channel, '||'))

	execute maintenance.insertexecution
		@step = N'update traffic_channel',
		@execguid = @exec_guid,
		@procid = @proc_id,
		@rows = @@rowcount;

	update tgt
	set
		tgt.is_paid_traffic = src.is_paid_traffic,
		tgt.mt_update_dt = @mt_dt
	from
		dbo.utm as tgt
		inner join core.utm_is_paid_traffic as src on
			tgt.utm_key = src.utm_key
	where
		tgt.mt_update_dt >= @load_dt
		and not (isnull(tgt.is_paid_traffic, '||') = isnull(src.is_paid_traffic, '||'))

	execute maintenance.insertexecution
		@step = N'update is_paid_traffic',
		@execguid = @exec_guid,
		@procid = @proc_id,
		@rows = @@rowcount;

	update tgt
	set
		tgt.campaign_target_category_id = src.campaign_target_category_id,
		tgt.mt_update_dt = @mt_dt
	from
		dbo.utm as tgt
		inner join core.utm_campaign_target_category_id as src on
			tgt.utm_key = src.utm_key
	where
		tgt.mt_update_dt >= @load_dt
		and not (isnull(tgt.campaign_target_category_id, '||') = isnull(src.campaign_target_category_id, '||'))

	execute maintenance.insertexecution
		@step = N'update campaign_target_category_id',
		@execguid = @exec_guid,
		@procid = @proc_id,
		@rows = @@rowcount;

	update tgt
	set
		tgt.campaign_target_web_level1 = src.campaign_target_web_level1,
		tgt.mt_update_dt = @mt_dt
	from
		dbo.utm as tgt
		inner join core.utm_campaign_target_web_level1 as src on
			tgt.utm_key = src.utm_key
	where
		tgt.mt_update_dt >= @load_dt
		and not (isnull(tgt.campaign_target_web_level1, '||') = isnull(src.campaign_target_web_level1, '||'))

	execute maintenance.insertexecution
		@step = N'update campaign_target_web_level1',
		@execguid = @exec_guid,
		@procid = @proc_id,
		@rows = @@rowcount;

	update tgt
	set
		tgt.campaign_target_web_level2 = src.campaign_target_web_level2,
		tgt.mt_update_dt = @mt_dt
	from
		dbo.utm as tgt
		inner join core.utm_campaign_target_web_level2 as src on
			tgt.utm_key = src.utm_key
	where
		tgt.mt_update_dt >= @load_dt
		and not (isnull(tgt.campaign_target_web_level2, '||') = isnull(src.campaign_target_web_level2, '||'))

	execute maintenance.insertexecution
		@step = N'update campaign_target_web_level2',
		@execguid = @exec_guid,
		@procid = @proc_id,
		@rows = @@rowcount;

	update tgt
	set
		tgt.campaign_target_web_level3 = src.campaign_target_web_level3,
		tgt.mt_update_dt = @mt_dt
	from
		dbo.utm as tgt
		inner join core.utm_campaign_target_web_level3 as src on
			tgt.utm_key = src.utm_key
	where
		tgt.mt_update_dt >= @load_dt
		and not (isnull(tgt.campaign_target_web_level3, '||') = isnull(src.campaign_target_web_level3, '||'))

	execute maintenance.insertexecution
		@step = N'update campaign_target_web_level3',
		@execguid = @exec_guid,
		@procid = @proc_id,
		@rows = @@rowcount;

	update tgt
	set
		tgt.campaign_target_web_level4 = src.campaign_target_web_level4,
		tgt.mt_update_dt = @mt_dt
	from
		dbo.utm as tgt
		inner join core.utm_campaign_target_web_level4 as src on
			tgt.utm_key = src.utm_key
	where
		tgt.mt_update_dt >= @load_dt
		and not (isnull(tgt.campaign_target_web_level4, '||') = isnull(src.campaign_target_web_level4, '||'))

	execute maintenance.insertexecution
		@step = N'update campaign_target_web_level4',
		@execguid = @exec_guid,
		@procid = @proc_id,
		@rows = @@rowcount;

	update tgt
	set
		tgt.campaign_target_web_level5 = src.campaign_target_web_level5,
		tgt.mt_update_dt = @mt_dt
	from
		dbo.utm as tgt
		inner join core.utm_campaign_target_web_level5 as src on
			tgt.utm_key = src.utm_key
	where
		tgt.mt_update_dt >= @load_dt
		and not (isnull(tgt.campaign_target_web_level5, '||') = isnull(src.campaign_target_web_level5, '||'))

	execute maintenance.insertexecution
		@step = N'update campaign_target_web_level5',
		@execguid = @exec_guid,
		@procid = @proc_id,
		@rows = @@rowcount;

	update tgt
	set
		tgt.campaign_target_web_level6 = src.campaign_target_web_level6,
		tgt.mt_update_dt = @mt_dt
	from
		dbo.utm as tgt
		inner join core.utm_campaign_target_web_level6 as src on
			tgt.utm_key = src.utm_key
	where
		tgt.mt_update_dt >= @load_dt
		and not (isnull(tgt.campaign_target_web_level6, '||') = isnull(src.campaign_target_web_level6, '||'))

	execute maintenance.insertexecution
		@step = N'update campaign_target_web_level6',
		@execguid = @exec_guid,
		@procid = @proc_id,
		@rows = @@rowcount;

	execute maintenance.insertexecution
		@step = N'finish dbo.utm',
		@execguid = @exec_guid,
		@procid = @proc_id,
		@rows = @@rowcount;
end
	go


