
create procedure [dbo].[#anchor#_sync]
as begin
	set nocount on;

	declare @mt_dt datetime2(0) = sysdatetime()
	declare @load_dt datetime2(0) = dateadd(dd, -3, @mt_dt)

	declare @exec_guid uniqueidentifier = newid()
	declare @proc_id bigint = @@procid

	execute maintenance.InsertExecution
		@Step = N'start dbo.#anchor#',
		@ExecGUID = @exec_guid,
		@ProcID = @proc_id,
		@Rows = @@rowcount;

	insert into dbo.#anchor# (
		[#anchor#_key],
#list#
		[mt_insert_dt],
		[mt_update_dt]
	)
	select
		src.#anchor#_key,
#init#
		@mt_dt as mt_insert_dt,
		@mt_dt as mt_update_dt
	from
		core.#anchor# as src
	where
		src.mt_insert_dt >= @load_dt and
		not exists (
			select
				1
			from
				dbo.#anchor# as tgt
			where
				tgt.#anchor#_key = src.#anchor#_key
		)

	execute maintenance.InsertExecution
		@Step = N'insert into dbo.#anchor#',
		@ExecGUID = @exec_guid,
		@ProcID = @proc_id,
		@Rows = @@rowcount;

#upd_business_key#
	
#upd_attrs#	
	
end
	go