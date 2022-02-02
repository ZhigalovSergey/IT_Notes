	
	update tgt
	set
		tgt.#anchor#_#attr# = src.#anchor#_#attr#,
		tgt.mt_update_dt = @mt_dt
	from
		dbo.#anchor# as tgt
		inner join core.#anchor#_x_#attr# as src on
			tgt.#anchor#_key = src.#anchor#_key
	where
		tgt.mt_update_dt >= @load_dt
		and not (isnull(tgt.#anchor#_#attr#, '') = isnull(src.#anchor#_#attr#, ''))

	execute maintenance.InsertExecution
		@Step = N'update #anchor#_#attr#',
		@ExecGUID = @exec_guid,
		@ProcID = @proc_id,
		@Rows = @@rowcount;
		