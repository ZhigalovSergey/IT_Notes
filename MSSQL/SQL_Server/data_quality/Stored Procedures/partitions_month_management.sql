create procedure maintenance.partitions_month_management
(
	@filegroup_new sysname = 'MDWH_RAW_ONLINE',--файловая групп для новых партиций
	@filegroup_arc sysname = 'MDWH_RAW_HISTORY'--файловая групп для архивных партиций
)
as 
begin

	set nocount on;
	set datefirst 1;
	set dateformat ymd;

	begin try
		declare @prt_begin_val date = '20170101',
				@prt_begin_new_dir date ='20211101',-- dateadd(yy, datediff(yy,0,getdate()), 0),
				@prt_end_val date = dateadd(MM, 2, getdate()),
				@rec_id int,
				@cmd_txt nvarchar(4000),
				@default_filegroup sysname = @filegroup_arc,
				@exec_guid uniqueidentifier = newid(),
				@proc_id bigint = @@procid





		set @cmd_txt = 'Запущено формирование партиций , файловая группа = ' + @filegroup_new;


		execute maintenance.InsertExecution
			@Step = @cmd_txt,
			@ExecGUID = @exec_guid,
			@ProcID = @proc_id,
			@Rows = @@rowcount;

		drop table if exists #cmd_list;
		create table #cmd_list (
			rec_id int not null identity (1, 1) primary key,
			type_rec varchar(30) not null,
			cmd_SQL nvarchar(4000) not null,
			start_dt datetime2(0) null,
			end_dt datetime2(0) null,
			result nvarchar(4000) null
		);

		drop table if exists #partition_func;
		create table #partition_func (
			partition_function_name sysname not null primary key,
			data_type sysname not null,
			partition_scheme_name sysname not null,
			is_exists_partition_function bit not null,
			is_exists_partition_scheme bit not null,
			current_partition_number int null
		);

		with
		need_functions as (
			select
				partition_function_name = 'asw_pf_month_date',
				partition_scheme_name =	  'asw_ps_month_date',
				data_type =				  'date'
			union all
			select
				partition_function_name = 'asw_pf_month_dtm2_0',
				partition_scheme_name =	  'asw_ps_month_dtm2_0',
				data_type =				  'datetime2(0)'
			union all
			select
				partition_function_name = 'asw_pf_month_dtm2_3',
				partition_scheme_name =	  'asw_ps_month_dtm2_3',
				data_type =				  'datetime2(3)'
			union all
			select
				partition_function_name = 'asw_pf_month_dtm2_7',
				partition_scheme_name =	  'asw_ps_month_dtm2_7',
				data_type =				  'datetime2(7)'
			/*union all
			select
				partition_function_name = 'asw_pf_month_bigint',
				partition_scheme_name =	  'asw_ps_month_bigint',
				data_type =				  'bigint'*/
		)
		insert into #partition_func (
			partition_function_name,
			data_type,
			partition_scheme_name,
			is_exists_partition_function,
			is_exists_partition_scheme
		)
		select
			t.partition_function_name,
			t.data_type,
			isnull(ps.[name], t.partition_scheme_name) as partition_scheme_name,
			iif(pf.function_id is null, 0, 1)		   as is_exists_partition_function,
			iif(ps.data_space_id is null, 0, 1)		   as is_exists_partition_scheme

		from
			need_functions as t
			left join sys.partition_functions as pf on
				pf.[name] = t.partition_function_name
			left join sys.partition_schemes as ps on
				ps.function_id = pf.function_id and
				t.partition_scheme_name = ps.[name]


		execute maintenance.InsertExecution
			@Step = 'Выгружен список функций для обработки',
			@ExecGUID = @exec_guid,
			@ProcID = @proc_id,
			@Rows = @@rowcount;





		--настроечная табличка
		drop table if exists #calendar;
		create table #calendar (
			day_id datetime2(3) not null primary key,
			fg_name sysname not null,
			day_bigint bigint not null 
		);
		with
		DATES as (
			select
				@prt_begin_val as day_id

			union all

			select
				dateadd(MM, 1, day_id) as day_id
			from
				DATES
			where
				day_id < @prt_end_val
		)
		insert into #calendar (
			day_id,
			fg_name,
			day_bigint
		)
		select
			cast(concat(day_id, ' 00:00:00.000') as datetime2(3)) as day_id,
			fg_name =	iif(day_id < @prt_begin_new_dir, @filegroup_arc, @filegroup_new),
			day_bigint = cast(format(day_id,'yyMM')  as bigint) * 1000000000000000

		from
			DATES
		option (maxrecursion 0);



		execute maintenance.InsertExecution
			@Step = 'Сформирована настроечная таблица',
			@ExecGUID = @exec_guid,
			@ProcID = @proc_id,
			@Rows = @@rowcount;





		;
		with
		first_fg as (
			select top (1)
				day_id,
				fg_name
			from
				#calendar
			order by
				day_id
		)
		insert into #cmd_list (
			type_rec,
			cmd_SQL
		)
		select
			'CRT_FUNCTION',
			sql_crt_func = 'CREATE PARTITION FUNCTION [' + partition_function_name + '](' + data_type + ') AS RANGE RIGHT FOR VALUES (' 
				+	case	when data_type = 'bigint' then format(cast(format(fg.day_id,'yyMM')  as bigint) * 1000000000000000  ,'0')
							else '''' + convert(varchar(10), fg.day_id, 112) + ''''
							end
				+ ')'
		from
			#partition_func as f
			cross join first_fg as fg
		where
			f.is_exists_partition_function = 0


		execute maintenance.InsertExecution
			@Step = 'Сформирован список операций создания функций',
			@ExecGUID = @exec_guid,
			@ProcID = @proc_id,
			@Rows = @@rowcount;




		exec maintenance.queue_operations_exec;

		;
		with
		first_fg as (
			select top (1)
				day_id,
				fg_name
			from
				#calendar
			order by
				day_id
		)
		insert into #cmd_list (
			type_rec,
			cmd_SQL
		)
		select
			'CRT_SCHEME',
			sql_crt_scm = 'CREATE PARTITION SCHEME [' + partition_scheme_name + ']  AS PARTITION [' + partition_function_name + '] TO ([' + @default_filegroup + '], [' + fg.fg_name + '])'
		from
			#partition_func as f
			cross join first_fg as fg
		where
			f.is_exists_partition_scheme = 0


		execute maintenance.InsertExecution
			@Step = 'Сформирован список операций создания схем',
			@ExecGUID = @exec_guid,
			@ProcID = @proc_id,
			@Rows = @@rowcount;




		exec maintenance.queue_operations_exec;

	--Актуализаци функции секционирования


		;
		with
		expectation as (
			select
				pfnc.function_id as pfunc_id,
				pfnc.name		 as pfunc_name,
				pshm.name		 as pschema_name,
				day_id			 as prange_val,
				row_number() over (
				order by spd.day_id asc
				)				 as prange_num,
				spd.fg_name		 as fg_name,
				pf_need.data_type as pf_data_type
			from
				sys.partition_functions as pfnc with (nolock)
				inner join #partition_func as pf_need on
					pf_need.partition_function_name = pfnc.name and 
					pf_need.data_type != 'bigint'
				inner join sys.partition_schemes as pshm with (nolock) on
					pshm.function_id = pfnc.function_id
				cross join #calendar as spd
		),
		reality as (
			select
				pfnc.function_id				as pfunc_id,
				pfnc.name						as pfunc_name,
				pshm.name						as pschema_name,
				cast(prv.value as datetime2(3)) as prange_val,
				dds.destination_id				as prange_num,
				fgs.name						as fg_name
			from
				sys.partition_functions as pfnc with (nolock)
				inner join #partition_func as pf_need on
					pf_need.partition_function_name = pfnc.name and 
					pf_need.data_type != 'bigint'
				inner join sys.partition_schemes as pshm with (nolock) on
					pshm.function_id = pfnc.function_id
				inner join sys.destination_data_spaces as dds with (nolock) on
					dds.partition_scheme_id = pshm.data_space_id
				inner join sys.partition_range_values as prv on
					prv.function_id = pfnc.function_id and
					dds.destination_id =
						case pfnc.boundary_value_on_right
							when 1
								then prv.boundary_id + 1
							else prv.boundary_id
						end
				inner join sys.filegroups as fgs with (nolock) on
					fgs.data_space_id = dds.data_space_id

		)
		insert into #cmd_list (
			type_rec,
			cmd_SQL
		)
		select
			'PARTITION' as type_rec,
			case
				when ept.pfunc_name is null
					then concat(
					'ALTER PARTITION FUNCTION ', rlt.pfunc_name, '() MERGE RANGE (N''', rlt.prange_val, ''');'
					)
				when rlt.pfunc_name is null
					then concat(
					'ALTER PARTITION SCHEME ', ept.pschema_name, ' NEXT USED ', ept.fg_name, ';', 'ALTER PARTITION FUNCTION ', ept.pfunc_name, '() SPLIT RANGE (N''', ept.prange_val, ''');'
					)
				when ept.prange_val <> rlt.prange_val
					then concat(
					'ALTER PARTITION SCHEME ', ept.pschema_name, ' NEXT USED ', ept.fg_name, ';', 'ALTER PARTITION FUNCTION ', rlt.pfunc_name, '() MERGE RANGE (N''', rlt.prange_val--CONVERT(VARCHAR(10), rlt.prange_val, 112)
					, ''');', 'ALTER PARTITION FUNCTION ', ept.pfunc_name, '() SPLIT RANGE (N''', ept.prange_val--CONVERT(VARCHAR(10), ept.prange_val, 112)
					, ''');'
					)
			end			as cmd_SQL
		from
			expectation as ept
			full outer join reality as rlt on
				ept.prange_val = rlt.prange_val and
				ept.pfunc_name = rlt.pfunc_name
		where
			1 = 1 and
			(
				ept.pfunc_name is null or
				rlt.pfunc_name is null or				
				ept.prange_val <> rlt.prange_val
			)
		order by
			isnull(ept.prange_val, rlt.prange_val);


		execute maintenance.InsertExecution
			@Step = 'Сформирован список операций с партициями (дата)',
			@ExecGUID = @exec_guid,
			@ProcID = @proc_id,
			@Rows = @@rowcount;



		;
		with
		expectation as (
			select
				pfnc.function_id as pfunc_id,
				pfnc.name		 as pfunc_name,
				pshm.name		 as pschema_name,
				day_bigint		 as prange_val,
				row_number() over (
				order by spd.day_id asc
				)				 as prange_num,
				spd.fg_name		 as fg_name,
				pf_need.data_type as pf_data_type
			from
				sys.partition_functions as pfnc with (nolock)
				inner join #partition_func as pf_need on
					pf_need.partition_function_name = pfnc.name and 
					pf_need.data_type = 'bigint'
				inner join sys.partition_schemes as pshm with (nolock) on
					pshm.function_id = pfnc.function_id
				cross join #calendar as spd
		),
		reality as (
			select
				pfnc.function_id				as pfunc_id,
				pfnc.name						as pfunc_name,
				pshm.name						as pschema_name,
				cast(prv.value as bigint)		as prange_val,
				dds.destination_id				as prange_num,
				fgs.name						as fg_name
			from
				sys.partition_functions as pfnc with (nolock)
				inner join #partition_func as pf_need on
					pf_need.partition_function_name = pfnc.name and 
					pf_need.data_type = 'bigint'
				inner join sys.partition_schemes as pshm with (nolock) on
					pshm.function_id = pfnc.function_id
				inner join sys.destination_data_spaces as dds with (nolock) on
					dds.partition_scheme_id = pshm.data_space_id
				inner join sys.partition_range_values as prv on
					prv.function_id = pfnc.function_id and
					dds.destination_id =
						case pfnc.boundary_value_on_right
							when 1
								then prv.boundary_id + 1
							else prv.boundary_id
						end
				inner join sys.filegroups as fgs with (nolock) on
					fgs.data_space_id = dds.data_space_id

		)
		insert into #cmd_list (
			type_rec,
			cmd_SQL
		)
		select
			'PARTITION' as type_rec,
			case
				when ept.pfunc_name is null
					then concat(
					'ALTER PARTITION FUNCTION ', rlt.pfunc_name, '() MERGE RANGE (', rlt.prange_val, ');'
					)
				when rlt.pfunc_name is null
					then concat(
					'ALTER PARTITION SCHEME ', ept.pschema_name, ' NEXT USED ', ept.fg_name, ';', 'ALTER PARTITION FUNCTION ', ept.pfunc_name, '() SPLIT RANGE (', ept.prange_val, ');'
					)
				when ept.prange_val <> rlt.prange_val
					then concat(
					  'ALTER PARTITION SCHEME ', ept.pschema_name, ' NEXT USED ', ept.fg_name, ';'
					, 'ALTER PARTITION FUNCTION ', rlt.pfunc_name, '() MERGE RANGE (', rlt.prange_val	, ');'
					, 'ALTER PARTITION FUNCTION ', ept.pfunc_name, '() SPLIT RANGE (', ept.prange_val	, ');'
					)
			end			as cmd_SQL
		from
			expectation as ept
			full outer join reality as rlt on
				ept.prange_val = rlt.prange_val and
				ept.pfunc_name = rlt.pfunc_name
		where
			1 = 1 and
			(
				ept.pfunc_name is null or
				rlt.pfunc_name is null or
				ept.prange_val <> rlt.prange_val
			)
		order by
			isnull(ept.prange_val, rlt.prange_val);


		execute maintenance.InsertExecution
			@Step = 'Сформирован список операций с партициями (bigint)',
			@ExecGUID = @exec_guid,
			@ProcID = @proc_id,
			@Rows = @@rowcount;

		exec maintenance.queue_operations_exec;



		drop table if exists #current_partition_number;
		create table #current_partition_number (
			partition_function_name sysname not null primary key,
			current_partition_number int null
		);
		select
			@cmd_txt = string_agg('select partition_function_name = ''' + partition_function_name + ''' , current_partition_number =  $PARTITION.' + partition_function_name 
				+ case when data_type = 'bigint' then'(CAST(cast(format(getdate(),''yyMM'')  as bigint) * 1000000000000000 AS ' 
					else '(CAST(CAST(GETDATE() AS DATE) AS ' 
				  end
				+ data_type + ')) ', char(10) + ' union all ' + char(10))
		from
			#partition_func
		where
			is_exists_partition_function = 1;


		insert into #current_partition_number (
			partition_function_name,
			current_partition_number
		)
		exec (@cmd_txt);


		update f
		set
			current_partition_number = n.current_partition_number
		from
			#partition_func as f
			inner join #current_partition_number as n on
				f.partition_function_name = n.partition_function_name

		execute maintenance.InsertExecution
			@Step = 'Проставлены текущие номера партиций',
			@ExecGUID = @exec_guid,
			@ProcID = @proc_id,
			@Rows = @@rowcount;



		with
		PARTITIONS_TO_CLEAN as (
			select
				sc.name + N'.' + so.name							 as table_full_name,
				si.index_id											 as index_id,
				si.type_desc										 as index_type_desc,
				si.name												 as index_name,
				stat.row_count										 as row_count,
				stat.in_row_reserved_page_count * 8. / 1024. / 1024. as row_gb,
				stat.lob_reserved_page_count * 8. / 1024. / 1024.	 as lob_gb,
				p.partition_number									 as partition_number,
				p.data_compression_desc								 as partition_data_compression_desc,
				pf.name												 as partition_function_name,
				case pf.boundary_value_on_right
					when 1
						then 'Right / Lower'
					else 'Left / Upper'
				end													 as boundary_type,
				prv.value											 as boundary_point,
				fg.name												 as fg_name,
				sql_truncate =										 
					case
						when stat.row_count >= 0
							and p.partition_number < cp.current_partition_number - stg.retention_partition_count
							then 'TRUNCATE TABLE ' + sc.name + N'.' + so.name + '   WITH (PARTITIONS (' + format(p.partition_number, '0') + ')) ; '
						else ''
					end
			from
				maintenance.partitions_month_retention_setting as stg 
				inner join sys.tables as sst on 
					sst.object_id =  object_id(stg.table_full_name) and 
					sst.temporal_type_desc = 'NON_TEMPORAL_TABLE'
				inner join sys.indexes as si on
					si.object_id = sst.object_id and
					si.index_id in (0, 1)
				inner join sys.objects as so on
					si.object_id = so.object_id
				inner join sys.schemas as sc on
					so.schema_id = sc.schema_id
				inner join sys.partition_schemes as ps on
					si.data_space_id = ps.data_space_id
				inner join sys.partition_functions as pf on
					ps.function_id = pf.function_id
				inner join #partition_func as cp on
					cp.partition_function_name = pf.name and
					cp.current_partition_number > 1
				inner join sys.partitions as p on
					si.object_id = p.object_id and
					si.index_id = p.index_id
				inner join sys.partition_range_values as prv on
					prv.function_id = pf.function_id and
					p.partition_number =
						case pf.boundary_value_on_right
							when 1
								then prv.boundary_id + 1
							else prv.boundary_id
						end        /* For left-based functions, partition_number = boundary_id,            for right-based functions we need to add 1 */
				inner join sys.dm_db_partition_stats as stat on
					stat.object_id = p.object_id and
					stat.index_id = p.index_id and
					stat.index_id = p.index_id and
					stat.partition_id = p.partition_id and
					stat.partition_number = p.partition_number
				inner join sys.allocation_units as au on
					au.container_id = p.hobt_id and
					au.type_desc = 'IN_ROW_DATA'
				inner join sys.filegroups as fg on
					fg.data_space_id = au.data_space_id
			where
				stat.row_count > 0 and
				p.partition_number < cp.current_partition_number - stg.retention_partition_count
		)
		insert into #cmd_list (
			type_rec,
			cmd_SQL,
			result
		)
		select
			type_rec = 'TRUNCATE',
			cmd_SQL =   sql_truncate,
			result =   format(row_count, '0')
		from
			PARTITIONS_TO_CLEAN
		order by
			table_full_name, partition_number
		option (force order);

		execute maintenance.InsertExecution
			@Step = 'Сформирован список операций очистки партиций',
			@ExecGUID = @exec_guid,
			@ProcID = @proc_id,
			@Rows = @@rowcount;

		exec maintenance.queue_operations_exec;



		execute maintenance.InsertExecution
			@Step = 'Формирование месячных партиций выполнено!',
			@ExecGUID = @exec_guid,
			@ProcID = @proc_id,
			@Rows = @@rowcount;


	end try

	begin catch
		declare @error_state int = error_state();
		declare @error_severity int = iif(error_severity() > 16, 16, error_severity());
		declare @error_message varchar(max) = isnull('Ошибка #' + nullif(cast(nullif(error_number(), 50000) as varchar), '') + ': ', '') + isnull(nullif(error_message(), ''), 'Неизвестная ошибка') + +isnull(char(10) + '        ' + 'at    ' + isnull(error_procedure() + '    :    ', '') + cast(error_line() as varchar(10)), '');
		declare @error_body varchar(max) = 'В процедуре формирования месячных партиций произошла ошибка <br>' + isnull(@error_message, 'Текста ошибки определить не удалось') + ';cmd = ' + isnull('@cmd_txt', '') + '<br><br>'



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



GO