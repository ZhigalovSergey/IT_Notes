
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
		and not (isnull(tgt.traffic_channel,'||') = isnull(src.traffic_channel,'||'))

	execute maintenance.insertexecution
		@step = n'update traffic_channel',
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
		and not (isnull(tgt.is_paid_traffic,'||') = isnull(src.is_paid_traffic,'||'))

	execute maintenance.insertexecution
		@step = n'update is_paid_traffic',
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
		and not (isnull(tgt.campaign_target_category_id,'||') = isnull(src.campaign_target_category_id,'||'))

	execute maintenance.insertexecution
		@step = n'update campaign_target_category_id',
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
		and not (isnull(tgt.campaign_target_web_level1,'||') = isnull(src.campaign_target_web_level1,'||'))

	execute maintenance.insertexecution
		@step = n'update campaign_target_web_level1',
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
		and not (isnull(tgt.campaign_target_web_level2,'||') = isnull(src.campaign_target_web_level2,'||'))

	execute maintenance.insertexecution
		@step = n'update campaign_target_web_level2',
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
		and not (isnull(tgt.campaign_target_web_level3,'||') = isnull(src.campaign_target_web_level3,'||'))

	execute maintenance.insertexecution
		@step = n'update campaign_target_web_level3',
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
		and not (isnull(tgt.campaign_target_web_level4,'||') = isnull(src.campaign_target_web_level4,'||'))

	execute maintenance.insertexecution
		@step = n'update campaign_target_web_level4',
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
		and not (isnull(tgt.campaign_target_web_level5,'||') = isnull(src.campaign_target_web_level5,'||'))

	execute maintenance.insertexecution
		@step = n'update campaign_target_web_level5',
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
		and not (isnull(tgt.campaign_target_web_level6,'||') = isnull(src.campaign_target_web_level6,'||'))

	execute maintenance.insertexecution
		@step = n'update campaign_target_web_level6',
		@execguid = @exec_guid,
		@procid = @proc_id,
		@rows = @@rowcount;
		