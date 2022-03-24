CREATE procedure maintenance_idx.operations_exec
as begin
	set nocount on;
	set datefirst 1;
	set dateformat ymd;

	begin try
		declare @rec_id int,
				@cmd_SQL nvarchar(4000),
				@exec_guid uniqueidentifier = newid(),
				@proc_id bigint = @@procid




		set @cmd_SQL = 'Запущена обработка очереди ';

		execute maintenance.InsertExecution
			@Step = @cmd_SQL,
			@ExecGUID = @exec_guid,
			@ProcID = @proc_id,
			@Rows = @@rowcount;


		if object_id('tempdb..#cmd_list') is null
			raiserror ('Отсутствует таблица #cmd_list ', 16, 1);

		declare CurOp cursor fast_forward
		for
		 select
			 rec_id,
			 cmd_SQL
		 from
			 #cmd_list
		 where
			 start_dt is null
		 order by
			 rec_id

		open CurOp;

		fetch next
		from CurOp
		into @rec_id
		, @cmd_SQL

		while @@fetch_status = 0
		begin

			update #cmd_list
			set
				start_dt = sysdatetime()
			where
				rec_id = @rec_id

			exec (@cmd_SQL);


			execute maintenance.InsertExecution
				@Step = @cmd_SQL,
				@ExecGUID = @exec_guid,
				@ProcID = @proc_id,
				@Rows = @@rowcount;

			update #cmd_list
			set
				end_dt = sysdatetime()
			where
				rec_id = @rec_id

			fetch next
			from CurOp
			into @rec_id
			, @cmd_SQL
		end;

		close CurOp;

		deallocate CurOp;

		execute maintenance.InsertExecution
			@Step = 'Завершена обработка очереди!',
			@ExecGUID = @exec_guid,
			@ProcID = @proc_id,
			@Rows = @@rowcount;


	end try

	begin catch
		declare @error_state int = error_state();
		declare @error_severity int = iif(error_severity() > 16, 16, error_severity());
		declare @error_message varchar(max) = isnull('Ошибка #' + nullif(cast(nullif(error_number(), 50000) as varchar), '') + ': ', '') + isnull(nullif(error_message(), ''), 'Неизвестная ошибка') + +isnull(char(10) + '        ' + 'at    ' + isnull(error_procedure() + '    :    ', '') + cast(error_line() as varchar(10)), '');
		declare @error_body varchar(max) = 'В процедуре обработки очереди произошла ошибка <br>' + isnull(@error_message, 'Текста ошибки определить не удалось') + ';cmd = ' + isnull('@cmd_SQL', '') + '<br><br>'



		execute maintenance.InsertExecution
			@Step = @error_message,
			@ExecGUID = @exec_guid,
			@ProcID = @proc_id,
			@Rows = @@rowcount;


		raiserror (
		@error_message
		, @error_severity
		, @error_state
		);

	end catch

end;