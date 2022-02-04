
create procedure [dbo].[utm_extended_sync]
as begin
	set nocount on;

	declare @mt_dt datetime2(0) = sysdatetime()
	declare @load_dt datetime2(0) = dateadd(dd, -3, @mt_dt)

	declare @exec_guid uniqueidentifier = newid()
	declare @proc_id bigint = @@procid

	execute maintenance.InsertExecution
		@Step = N'start dbo.utm_extended',
		@ExecGUID = @exec_guid,
		@ProcID = @proc_id,
		@Rows = @@rowcount;

	insert into dbo.utm_extended (
		[utm_extended_key],
		[utm_extended_hash],
		[utm_extended_source],
		[utm_extended_medium],
		[utm_extended_campaign],
		[utm_extended_content],
		[utm_extended_term],
		[utm_extended_traffic_type],
		[utm_extended_traffic_category],
		[utm_extended_traffic_subcategory],
		[utm_extended_traffic_division],
		[utm_extended_traffic_channel],
		[utm_extended_traffic_is_paid],
		[utm_extended_campaign_target_category_id],
		[utm_extended_campaign_target_web_level_1],
		[utm_extended_campaign_target_web_level_2],
		[utm_extended_campaign_target_web_level_3],
		[utm_extended_campaign_target_web_level_4],
		[utm_extended_campaign_target_web_level_5],
		[utm_extended_campaign_target_web_level_6],
		[mt_insert_dt],
		[mt_update_dt]
	)
	select
		src.utm_extended_key,
		null as [utm_extended_hash],
		null as [utm_extended_source],
		null as [utm_extended_medium],
		null as [utm_extended_campaign],
		null as [utm_extended_content],
		null as [utm_extended_term],
		null as [utm_extended_traffic_type],
		null as [utm_extended_traffic_category],
		null as [utm_extended_traffic_subcategory],
		null as [utm_extended_traffic_division],
		null as [utm_extended_traffic_channel],
		null as [utm_extended_traffic_is_paid],
		null as [utm_extended_campaign_target_category_id],
		null as [utm_extended_campaign_target_web_level_1],
		null as [utm_extended_campaign_target_web_level_2],
		null as [utm_extended_campaign_target_web_level_3],
		null as [utm_extended_campaign_target_web_level_4],
		null as [utm_extended_campaign_target_web_level_5],
		null as [utm_extended_campaign_target_web_level_6],
		@mt_dt as mt_insert_dt,
		@mt_dt as mt_update_dt
	from
		core.utm_extended as src
	where
		src.mt_insert_dt >= @load_dt and
		not exists (
			select
				1
			from
				dbo.utm_extended as tgt
			where
				tgt.utm_extended_key = src.utm_extended_key
		)

	execute maintenance.InsertExecution
		@Step = N'insert into dbo.utm_extended',
		@ExecGUID = @exec_guid,
		@ProcID = @proc_id,
		@Rows = @@rowcount;

	update tgt
	set
		tgt.[utm_extended_hash] = src.[utm_extended_hash],
		tgt.[utm_extended_source] = src.[utm_extended_source],
		tgt.[utm_extended_medium] = src.[utm_extended_medium],
		tgt.[utm_extended_campaign] = src.[utm_extended_campaign],
		tgt.[utm_extended_content] = src.[utm_extended_content],
		tgt.[utm_extended_term] = src.[utm_extended_term],
		tgt.mt_update_dt = @mt_dt
	from
		dbo.utm_extended as tgt
		inner join core.utm_extended_s_gbq as src on
			tgt.utm_extended_key = src.utm_extended_key
	where
		tgt.mt_update_dt >= @load_dt
		and not (
				isnull(tgt.[utm_extended_hash], '||') = isnull(src.[utm_extended_hash], '||')
			and isnull(tgt.[utm_extended_source], '||') = isnull(src.[utm_extended_source], '||')
			and isnull(tgt.[utm_extended_medium], '||') = isnull(src.[utm_extended_medium], '||')
			and isnull(tgt.[utm_extended_campaign], '||') = isnull(src.[utm_extended_campaign], '||')
			and isnull(tgt.[utm_extended_content], '||') = isnull(src.[utm_extended_content], '||')
			and isnull(tgt.[utm_extended_term], '||') = isnull(src.[utm_extended_term], '||')
			)

	execute maintenance.InsertExecution
		@Step = N'update hash, source, medium, campaign, content, term)',
		@ExecGUID = @exec_guid,
		@ProcID = @proc_id,
		@Rows = @@rowcount;
		
	
	
	update tgt
	set
		tgt.utm_extended_traffic_type = src.utm_extended_traffic_type,
		tgt.mt_update_dt = @mt_dt
	from
		dbo.utm_extended as tgt
		inner join core.utm_extended_x_traffic_type as src on
			tgt.utm_extended_key = src.utm_extended_key
	where
		tgt.mt_update_dt >= @load_dt
		and not (isnull(tgt.utm_extended_traffic_type, '') = isnull(src.utm_extended_traffic_type, ''))

	execute maintenance.InsertExecution
		@Step = N'update utm_extended_traffic_type',
		@ExecGUID = @exec_guid,
		@ProcID = @proc_id,
		@Rows = @@rowcount;
			
	update tgt
	set
		tgt.utm_extended_traffic_category = src.utm_extended_traffic_category,
		tgt.mt_update_dt = @mt_dt
	from
		dbo.utm_extended as tgt
		inner join core.utm_extended_x_traffic_category as src on
			tgt.utm_extended_key = src.utm_extended_key
	where
		tgt.mt_update_dt >= @load_dt
		and not (isnull(tgt.utm_extended_traffic_category, '') = isnull(src.utm_extended_traffic_category, ''))

	execute maintenance.InsertExecution
		@Step = N'update utm_extended_traffic_category',
		@ExecGUID = @exec_guid,
		@ProcID = @proc_id,
		@Rows = @@rowcount;
			
	update tgt
	set
		tgt.utm_extended_traffic_subcategory = src.utm_extended_traffic_subcategory,
		tgt.mt_update_dt = @mt_dt
	from
		dbo.utm_extended as tgt
		inner join core.utm_extended_x_traffic_subcategory as src on
			tgt.utm_extended_key = src.utm_extended_key
	where
		tgt.mt_update_dt >= @load_dt
		and not (isnull(tgt.utm_extended_traffic_subcategory, '') = isnull(src.utm_extended_traffic_subcategory, ''))

	execute maintenance.InsertExecution
		@Step = N'update utm_extended_traffic_subcategory',
		@ExecGUID = @exec_guid,
		@ProcID = @proc_id,
		@Rows = @@rowcount;
			
	update tgt
	set
		tgt.utm_extended_traffic_division = src.utm_extended_traffic_division,
		tgt.mt_update_dt = @mt_dt
	from
		dbo.utm_extended as tgt
		inner join core.utm_extended_x_traffic_division as src on
			tgt.utm_extended_key = src.utm_extended_key
	where
		tgt.mt_update_dt >= @load_dt
		and not (isnull(tgt.utm_extended_traffic_division, '') = isnull(src.utm_extended_traffic_division, ''))

	execute maintenance.InsertExecution
		@Step = N'update utm_extended_traffic_division',
		@ExecGUID = @exec_guid,
		@ProcID = @proc_id,
		@Rows = @@rowcount;
			
	update tgt
	set
		tgt.utm_extended_traffic_channel = src.utm_extended_traffic_channel,
		tgt.mt_update_dt = @mt_dt
	from
		dbo.utm_extended as tgt
		inner join core.utm_extended_x_traffic_channel as src on
			tgt.utm_extended_key = src.utm_extended_key
	where
		tgt.mt_update_dt >= @load_dt
		and not (isnull(tgt.utm_extended_traffic_channel, '') = isnull(src.utm_extended_traffic_channel, ''))

	execute maintenance.InsertExecution
		@Step = N'update utm_extended_traffic_channel',
		@ExecGUID = @exec_guid,
		@ProcID = @proc_id,
		@Rows = @@rowcount;
			
	update tgt
	set
		tgt.utm_extended_traffic_is_paid = src.utm_extended_traffic_is_paid,
		tgt.mt_update_dt = @mt_dt
	from
		dbo.utm_extended as tgt
		inner join core.utm_extended_x_traffic_is_paid as src on
			tgt.utm_extended_key = src.utm_extended_key
	where
		tgt.mt_update_dt >= @load_dt
		and not (isnull(tgt.utm_extended_traffic_is_paid, '') = isnull(src.utm_extended_traffic_is_paid, ''))

	execute maintenance.InsertExecution
		@Step = N'update utm_extended_traffic_is_paid',
		@ExecGUID = @exec_guid,
		@ProcID = @proc_id,
		@Rows = @@rowcount;
			
	update tgt
	set
		tgt.utm_extended_campaign_target_category_id = src.utm_extended_campaign_target_category_id,
		tgt.mt_update_dt = @mt_dt
	from
		dbo.utm_extended as tgt
		inner join core.utm_extended_x_campaign_target_category_id as src on
			tgt.utm_extended_key = src.utm_extended_key
	where
		tgt.mt_update_dt >= @load_dt
		and not (isnull(tgt.utm_extended_campaign_target_category_id, '') = isnull(src.utm_extended_campaign_target_category_id, ''))

	execute maintenance.InsertExecution
		@Step = N'update utm_extended_campaign_target_category_id',
		@ExecGUID = @exec_guid,
		@ProcID = @proc_id,
		@Rows = @@rowcount;
			
	update tgt
	set
		tgt.utm_extended_campaign_target_web_level_1 = src.utm_extended_campaign_target_web_level_1,
		tgt.mt_update_dt = @mt_dt
	from
		dbo.utm_extended as tgt
		inner join core.utm_extended_x_campaign_target_web_level_1 as src on
			tgt.utm_extended_key = src.utm_extended_key
	where
		tgt.mt_update_dt >= @load_dt
		and not (isnull(tgt.utm_extended_campaign_target_web_level_1, '') = isnull(src.utm_extended_campaign_target_web_level_1, ''))

	execute maintenance.InsertExecution
		@Step = N'update utm_extended_campaign_target_web_level_1',
		@ExecGUID = @exec_guid,
		@ProcID = @proc_id,
		@Rows = @@rowcount;
			
	update tgt
	set
		tgt.utm_extended_campaign_target_web_level_2 = src.utm_extended_campaign_target_web_level_2,
		tgt.mt_update_dt = @mt_dt
	from
		dbo.utm_extended as tgt
		inner join core.utm_extended_x_campaign_target_web_level_2 as src on
			tgt.utm_extended_key = src.utm_extended_key
	where
		tgt.mt_update_dt >= @load_dt
		and not (isnull(tgt.utm_extended_campaign_target_web_level_2, '') = isnull(src.utm_extended_campaign_target_web_level_2, ''))

	execute maintenance.InsertExecution
		@Step = N'update utm_extended_campaign_target_web_level_2',
		@ExecGUID = @exec_guid,
		@ProcID = @proc_id,
		@Rows = @@rowcount;
			
	update tgt
	set
		tgt.utm_extended_campaign_target_web_level_3 = src.utm_extended_campaign_target_web_level_3,
		tgt.mt_update_dt = @mt_dt
	from
		dbo.utm_extended as tgt
		inner join core.utm_extended_x_campaign_target_web_level_3 as src on
			tgt.utm_extended_key = src.utm_extended_key
	where
		tgt.mt_update_dt >= @load_dt
		and not (isnull(tgt.utm_extended_campaign_target_web_level_3, '') = isnull(src.utm_extended_campaign_target_web_level_3, ''))

	execute maintenance.InsertExecution
		@Step = N'update utm_extended_campaign_target_web_level_3',
		@ExecGUID = @exec_guid,
		@ProcID = @proc_id,
		@Rows = @@rowcount;
			
	update tgt
	set
		tgt.utm_extended_campaign_target_web_level_4 = src.utm_extended_campaign_target_web_level_4,
		tgt.mt_update_dt = @mt_dt
	from
		dbo.utm_extended as tgt
		inner join core.utm_extended_x_campaign_target_web_level_4 as src on
			tgt.utm_extended_key = src.utm_extended_key
	where
		tgt.mt_update_dt >= @load_dt
		and not (isnull(tgt.utm_extended_campaign_target_web_level_4, '') = isnull(src.utm_extended_campaign_target_web_level_4, ''))

	execute maintenance.InsertExecution
		@Step = N'update utm_extended_campaign_target_web_level_4',
		@ExecGUID = @exec_guid,
		@ProcID = @proc_id,
		@Rows = @@rowcount;
			
	update tgt
	set
		tgt.utm_extended_campaign_target_web_level_5 = src.utm_extended_campaign_target_web_level_5,
		tgt.mt_update_dt = @mt_dt
	from
		dbo.utm_extended as tgt
		inner join core.utm_extended_x_campaign_target_web_level_5 as src on
			tgt.utm_extended_key = src.utm_extended_key
	where
		tgt.mt_update_dt >= @load_dt
		and not (isnull(tgt.utm_extended_campaign_target_web_level_5, '') = isnull(src.utm_extended_campaign_target_web_level_5, ''))

	execute maintenance.InsertExecution
		@Step = N'update utm_extended_campaign_target_web_level_5',
		@ExecGUID = @exec_guid,
		@ProcID = @proc_id,
		@Rows = @@rowcount;
			
	update tgt
	set
		tgt.utm_extended_campaign_target_web_level_6 = src.utm_extended_campaign_target_web_level_6,
		tgt.mt_update_dt = @mt_dt
	from
		dbo.utm_extended as tgt
		inner join core.utm_extended_x_campaign_target_web_level_6 as src on
			tgt.utm_extended_key = src.utm_extended_key
	where
		tgt.mt_update_dt >= @load_dt
		and not (isnull(tgt.utm_extended_campaign_target_web_level_6, '') = isnull(src.utm_extended_campaign_target_web_level_6, ''))

	execute maintenance.InsertExecution
		@Step = N'update utm_extended_campaign_target_web_level_6',
		@ExecGUID = @exec_guid,
		@ProcID = @proc_id,
		@Rows = @@rowcount;
			
	
end
	go