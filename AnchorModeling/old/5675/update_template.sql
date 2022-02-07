
	update tgt
	set
		tgt.### = src.###,
		tgt.mt_update_dt = @mt_dt
	from
		dbo.utm as tgt
		inner join core.utm_### as src on
			tgt.utm_key = src.utm_key
	where
		tgt.mt_update_dt >= @load_dt
		and not (isnull(tgt.###,'||') = isnull(src.###,'||'))

	execute maintenance.insertexecution
		@step = n'update ###',
		@execguid = @exec_guid,
		@procid = @proc_id,
		@rows = @@rowcount;
		