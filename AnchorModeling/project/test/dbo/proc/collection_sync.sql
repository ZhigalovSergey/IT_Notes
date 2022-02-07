
create procedure [dbo].[collection_sync]
as begin
	set nocount on;

	declare @mt_dt datetime2(0) = sysdatetime()
	declare @load_dt datetime2(0) = dateadd(dd, -3, @mt_dt)

	declare @exec_guid uniqueidentifier = newid()
	declare @proc_id bigint = @@procid

	execute maintenance.InsertExecution
		@Step = N'start dbo.collection',
		@ExecGUID = @exec_guid,
		@ProcID = @proc_id,
		@Rows = @@rowcount;

	insert into dbo.collection (
		[collection_key],
		[collection_collection_id],
		[collection_collection_type_id],
		[collection_parent_id],
		[collection_identifier],
		[collection_parent_identifier],
		[collection_collection_name],
		[collection_collection_display_name],
		[collection_is_active],
		[mt_insert_dt],
		[mt_update_dt]
	)
	select
		src.collection_key,
		null as [collection_collection_id],
		null as [collection_collection_type_id],
		null as [collection_parent_id],
		null as [collection_identifier],
		null as [collection_parent_identifier],
		null as [collection_collection_name],
		null as [collection_collection_display_name],
		null as [collection_is_active],
		@mt_dt as mt_insert_dt,
		@mt_dt as mt_update_dt
	from
		core.collection as src
	where
		src.mt_insert_dt >= @load_dt and
		not exists (
			select
				1
			from
				dbo.collection as tgt
			where
				tgt.collection_key = src.collection_key
		)

	execute maintenance.InsertExecution
		@Step = N'insert into dbo.collection',
		@ExecGUID = @exec_guid,
		@ProcID = @proc_id,
		@Rows = @@rowcount;

	update tgt
	set
		tgt.[collection_collection_id] = src.[collection_id],
		tgt.mt_update_dt = @mt_dt
	from
		dbo.collection as tgt
		inner join core.collection_malibu as src on
			tgt.collection_key = src.collection_key
	where
		tgt.mt_update_dt >= @load_dt
		and not exists 
			(select tgt.collection_collection_id intersect select src.collection_id)

	execute maintenance.InsertExecution
		@Step = N'update collection_id',
		@ExecGUID = @exec_guid,
		@ProcID = @proc_id,
		@Rows = @@rowcount;
	
	
	update tgt
	set
		tgt.collection_collection_type_id = tm.collection_type_id,
		tgt.mt_update_dt = @mt_dt
	from
		dbo.collection as tgt
		inner join core.collection_collection_type_lnk as t_lnk on
			tgt.collection_key = t_lnk.collection_key
		inner join core.collection_type_malibu as tm on
			t_lnk.collection_type_key = tm.collection_type_key
	where
		t_lnk.mt_update_dt >= @load_dt
		and not exists (select tgt.collection_collection_type_id intersect select tm.collection_type_id)

	execute maintenance.InsertExecution
		@Step = N'update collection_collection_type_id',
		@ExecGUID = @exec_guid,
		@ProcID = @proc_id,
		@Rows = @@rowcount;
			
	update tgt
	set
		tgt.collection_parent_id = src.collection_parent_id,
		tgt.mt_update_dt = @mt_dt
	from
		dbo.collection as tgt
		inner join core.collection_x_parent_id as src on
			tgt.collection_key = src.collection_key
	where
		tgt.mt_update_dt >= @load_dt
		and not exists (select tgt.collection_parent_id intersect select src.collection_parent_id)

	execute maintenance.InsertExecution
		@Step = N'update collection_parent_id',
		@ExecGUID = @exec_guid,
		@ProcID = @proc_id,
		@Rows = @@rowcount;
			
	update tgt
	set
		tgt.collection_identifier = src.identifier,
		tgt.mt_update_dt = @mt_dt
	from
		dbo.collection as tgt
		inner join core.collection_identifier as src on
			tgt.collection_key = src.collection_key
	where
		tgt.mt_update_dt >= @load_dt
		and not exists (select tgt.collection_identifier intersect select src.identifier)

	execute maintenance.InsertExecution
		@Step = N'update collection_identifier',
		@ExecGUID = @exec_guid,
		@ProcID = @proc_id,
		@Rows = @@rowcount;
			
	update tgt
	set
		tgt.collection_parent_identifier = src.collection_parent_identifier,
		tgt.mt_update_dt = @mt_dt
	from
		dbo.collection as tgt
		inner join core.collection_x_parent_identifier as src on
			tgt.collection_key = src.collection_key
	where
		tgt.mt_update_dt >= @load_dt
		and not exists (select tgt.collection_parent_identifier intersect select src.collection_parent_identifier)

	execute maintenance.InsertExecution
		@Step = N'update collection_parent_identifier',
		@ExecGUID = @exec_guid,
		@ProcID = @proc_id,
		@Rows = @@rowcount;
			
	update tgt
	set
		tgt.collection_collection_name = src.collection_name,
		tgt.mt_update_dt = @mt_dt
	from
		dbo.collection as tgt
		inner join core.collection_collection_name as src on
			tgt.collection_key = src.collection_key
	where
		tgt.mt_update_dt >= @load_dt
		and not exists (select tgt.collection_collection_name intersect select src.collection_name)

	execute maintenance.InsertExecution
		@Step = N'update collection_collection_name',
		@ExecGUID = @exec_guid,
		@ProcID = @proc_id,
		@Rows = @@rowcount;
			
	update tgt
	set
		tgt.collection_collection_display_name = src.collection_display_name,
		tgt.mt_update_dt = @mt_dt
	from
		dbo.collection as tgt
		inner join core.collection_collection_display_name as src on
			tgt.collection_key = src.collection_key
	where
		tgt.mt_update_dt >= @load_dt
		and not exists (select tgt.collection_collection_display_name intersect select src.collection_display_name)

	execute maintenance.InsertExecution
		@Step = N'update collection_collection_display_name',
		@ExecGUID = @exec_guid,
		@ProcID = @proc_id,
		@Rows = @@rowcount;
			
	update tgt
	set
		tgt.collection_is_active = src.collection_is_active,
		tgt.mt_update_dt = @mt_dt
	from
		dbo.collection as tgt
		inner join core.collection_x_is_active as src on
			tgt.collection_key = src.collection_key
	where
		tgt.mt_update_dt >= @load_dt
		and not exists (select tgt.collection_is_active intersect select src.collection_is_active)

	execute maintenance.InsertExecution
		@Step = N'update collection_is_active',
		@ExecGUID = @exec_guid,
		@ProcID = @proc_id,
		@Rows = @@rowcount;
			
	
end
	go