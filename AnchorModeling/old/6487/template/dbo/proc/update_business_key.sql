	update tgt
	set
		tgt.[#anchor#_#attr_bk_1#] = src.[#anchor#_#attr_bk_1#],
		tgt.[#anchor#_#attr_bk_2#] = src.[#anchor#_#attr_bk_2#],
		tgt.[#anchor#_#attr_bk_3#] = src.[#anchor#_#attr_bk_3#],
		tgt.[#anchor#_#attr_bk_4#] = src.[#anchor#_#attr_bk_4#],
		tgt.[#anchor#_#attr_bk_5#] = src.[#anchor#_#attr_bk_5#],
		tgt.[#anchor#_#attr_bk_6#] = src.[#anchor#_#attr_bk_6#],
		tgt.mt_update_dt = @mt_dt
	from
		dbo.#anchor# as tgt
		inner join core.#anchor#_s_#src_name# as src on
			tgt.#anchor#_key = src.#anchor#_key
	where
		tgt.mt_update_dt >= @load_dt
		and not (
				isnull(tgt.[#anchor#_#attr_bk_1#], '||') = isnull(src.[#anchor#_#attr_bk_1#], '||')
			and isnull(tgt.[#anchor#_#attr_bk_2#], '||') = isnull(src.[#anchor#_#attr_bk_2#], '||')
			and isnull(tgt.[#anchor#_#attr_bk_3#], '||') = isnull(src.[#anchor#_#attr_bk_3#], '||')
			and isnull(tgt.[#anchor#_#attr_bk_4#], '||') = isnull(src.[#anchor#_#attr_bk_4#], '||')
			and isnull(tgt.[#anchor#_#attr_bk_5#], '||') = isnull(src.[#anchor#_#attr_bk_5#], '||')
			and isnull(tgt.[#anchor#_#attr_bk_6#], '||') = isnull(src.[#anchor#_#attr_bk_6#], '||')
			)

	execute maintenance.InsertExecution
		@Step = N'update #attr_bk_1#, #attr_bk_2#, #attr_bk_3#, #attr_bk_4#, #attr_bk_5#, #attr_bk_6#)',
		@ExecGUID = @exec_guid,
		@ProcID = @proc_id,
		@Rows = @@rowcount;
		