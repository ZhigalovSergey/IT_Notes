	update tgt
	set
		#set_attrs# --tgt.[#anchor#_#attr_bk#] = src.[#anchor#_#attr_bk#],
		tgt.mt_update_dt = @mt_dt
	from
		dbo.#anchor# as tgt
		inner join core.#anchor#_s_#src_name# as src on
			tgt.#anchor#_key = src.#anchor#_key
	where
		tgt.mt_update_dt >= @load_dt
		and not exists 
			#where_attrs# --(select tgt.#anchor#_#attr_bk# intersect select src.#anchor#_#attr_bk#)

	execute maintenance.InsertExecution
		@Step = N'update #attr_bk#',
		@ExecGUID = @exec_guid,
		@ProcID = @proc_id,
		@Rows = @@rowcount;