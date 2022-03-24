CREATE procedure maintenance_idx.maintenance_columnstore_table
(
	@dbase_name sysname,--база
	@schema_name sysname = null,--схема, можно маску с %
	@tab_name sysname = null,--имя таблицы , можно маску с %
	@fragmentation_threshold tinyint = 30,--порог фрагментации для перестроения
	@mode bit = 1,--режим: 0-непосредственное выполнение, 1- постановка в очередь
	@priority_lvl tinyint = 100,--приоритет
	@expiration_minutes int = 30,--запустить операцию не позже 
	@expiration_dt datetime2(0) = null,--запустить операцию не позже ,  после этого времени если операция выполняется то она отстреливается- имеет выше приоритет над @expiration_minutes
	@lock_timeout int = null,--лимит блокировки
	@time_limit smallint = null,--лимит на выполнение в минутах
	@waitfor_dt datetime2(0) = null,--время запуска
	@obj_identifier varchar(255) = null --более подробная идентификация объекта, например при ослуживании таблицы помимо имени указываем индекс и партицию

)
as begin

	set nocount on;
	set datefirst 1;
	set dateformat ymd;
	declare @comment varchar(1000),
			@exec_guid uniqueidentifier = newid(),
			@proc_id bigint = @@procid,
			@mt_dt datetime2(0) = sysdatetime()
	declare @expiration_dt_p datetime2(0) = isnull(@expiration_dt, dateadd(minute, @expiration_minutes, @mt_dt)),-- время после которго запуск неактуален
			@source_desc varchar(4000) = object_schema_name(@proc_id) + '.' + object_name(@proc_id),
			@operation_group uniqueidentifier = newid()

	drop table if exists #cmd_list;
	create table #cmd_list (
		rec_id int not null identity (1, 1) primary key,
		type_rec varchar(30) null,
		obj_name sysname not null,
		cmd_SQL nvarchar(4000) not null,
		start_dt datetime2(0) null,
		end_dt datetime2(0) null,
		result nvarchar(4000) null,
		obj_identifier nvarchar(4000) null,
		priority_lvl tinyint null,
		time_limit [SMALLINT] null,
		lock_timeout [INT] null
	);

	begin try



		set @comment = N'START ; @mode = ' + format(cast(@mode as int), '0') + '; db_name = ' + @dbase_name + N'; schema_name = ' + isnull(@schema_name, 'NULL') + N'; tab_name = ' + isnull(@tab_name, 'NULL');


		execute maintenance.InsertExecution
			@Step = @comment,
			@ExecGUID = @exec_guid,
			@ProcID = @proc_id,
			@Rows = @@rowcount;




		--собираем статистику и фрагментацию по индексам
		exec maintenance_idx.index_stat_sync
			@dbase_name = @dbase_name,
			@schema_name = @schema_name,
			@tab_name = @tab_name;


		with
		index_stat_columnstore as (
			select
				sch_name,
				table_name,
				index_name,
				partition_number,
				fragmentation,
				row_count,
				partition_count = count(1) over (partition by sch_name, table_name, index_name)

			from
				maintenance_idx.index_stat
			where
				dbase_name = @dbase_name and
				sch_name like isnull(@schema_name, '%') and
				table_name like isnull(@tab_name, '%') and
				index_type like '%COLUMNSTORE'


		)
		insert into #cmd_list (
			obj_name,
			cmd_SQL,
			obj_identifier,
			priority_lvl,
			time_limit,
			lock_timeout
		)
		select
			'[' + st.sch_name + '].[' + st.table_name + ']'																																							as obj_name,
			'ALTER INDEX [' + st.index_name + '] ON [' + st.sch_name + '].[' + st.table_name + '] REBUILD PARTITION = ' + iif(partition_count = 1, 'ALL', format(st.partition_number, '0')) + ' WITH (ONLINE = ON)' as cmd_SQL,
			'{"schema_name":"' + st.sch_name + '", ' + '"table_name":"' + st.table_name + '", ' + '"index_name":"' + st.index_name + '", ' + '"partition_number":"' + format(st.partition_number, '0') + '"}'		as obj_identifier,
			isnull(s.priority_lvl, @priority_lvl)																																									as priority_lvl,
			isnull(s.time_limit, @time_limit)																																										as priority_lvl,
			isnull(s.lock_timeout, @lock_timeout)																																									as priority_lvl
		from
			index_stat_columnstore as st
			left join maintenance_idx.index_maintenance_setting as s on
				st.sch_name = s.sch_name and
				st.table_name = s.table_name
		where
			st.row_count > 0 and
			st.fragmentation >= isnull(s.fragmentation_threshold, @fragmentation_threshold)
		order by
			isnull(s.priority_lvl, @priority_lvl) desc, st.sch_name, st.table_name, st.index_name, partition_number desc
		option (force order);


		execute maintenance.InsertExecution
			@Step = 'Сформирован список операций по перестройке партиций',
			@ExecGUID = @exec_guid,
			@ProcID = @proc_id,
			@Rows = @@rowcount;




		if @mode = 1
		begin

			exec queue_sql.queue_add
				@mode = 1,
				@dbase_name = @dbase_name,
				@obj_name = @tab_name,
				@obj_enable_parallel = 0,
				@expiration_dt = @expiration_dt_p,
				@priority_lvl = @priority_lvl,
				@source_desc = @source_desc,
				@operation_group = null,
				@lock_timeout = @lock_timeout,
				@time_limit = @time_limit,
				@waitfor_dt = @waitfor_dt;

			execute maintenance.InsertExecution
				@Step = 'Операции обслуживания поставлены в очередь!',
				@ExecGUID = @exec_guid,
				@ProcID = @proc_id,
				@Rows = @@rowcount;


		end
		else
		begin
			exec maintenance_idx.operations_exec;

			execute maintenance.InsertExecution
				@Step = 'Выполнены операции обслуживания!',
				@ExecGUID = @exec_guid,
				@ProcID = @proc_id,
				@Rows = @@rowcount;
		end


	end try

	begin catch

		declare @error_state int = error_state();
		declare @error_severity int = iif(error_severity() > 16, 16, error_severity());
		declare @error_message varchar(max) = isnull('Ошибка #' + nullif(cast(nullif(error_number(), 50000) as varchar), '') + ': ', '') + isnull(nullif(error_message(), ''), 'Неизвестная ошибка') + +isnull(char(10) + '        ' + 'at    ' + isnull(error_procedure() + '    :    ', '') + cast(error_line() as varchar(10)), '');
		declare @error_body varchar(max) = 'В процедуре обслуживания columnstore индексов произошла ошибка <br>' + isnull(@error_message, 'Текста ошибки определить не удалось') + ';cmd = ' + isnull('@comment', '') + '<br><br>'



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