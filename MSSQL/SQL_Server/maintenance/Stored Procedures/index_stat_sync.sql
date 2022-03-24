CREATE  procedure maintenance_idx.index_stat_sync (
	@dbase_name sysname,
	@schema_name sysname =  null,--схема, можно маску с %
	@tab_name sysname  = null--имя таблицы , можно маску с %
) as 
begin
	set nocount on

	declare @mt_dt datetime2(0) = sysdatetime()
	declare @exec_guid uniqueidentifier = newid()
	declare @proc_id bigint = @@procid;
	declare @sql_cmd nvarchar(max)
	declare @comment nvarchar(4000)

	set @dbase_name = upper(@dbase_name);

	set @comment = N'START ; db_name = ' + @dbase_name +  N'; schema_name = ' + isnull(@schema_name,'NULL')  +  N'; tab_name = ' + isnull(@tab_name,'NULL');

	execute maintenance.InsertExecution
		@Step = @comment,
		@ExecGUID = @exec_guid,
		@ProcID = @proc_id,
		@Rows = @@rowcount;


	drop table if exists #src;
	create table #src (
		sch_name sysname not null,
		table_name sysname not null,
		index_name sysname not null,
		partition_number int not null,
		index_type sysname not null,
		size_MB numeric(19, 3) not null,
		row_count bigint not null,
		fragmentation tinyint null,
		[object_id] int not null,
		index_id int not null
	);

	set @sql_cmd = 'use ' + @dbase_name + ';
	select
		t.[object_id],
		i.index_id,
		sh.name						  as sch_name,
		t.name						  as table_name,
		i.name						  as index_name,
		s.partition_number,
		i.type_desc					  as index_type,
		s.used_page_count * 8 / 1024. as size_MB,
		s.row_count					  as row_count,
		null						  fragmentation
	from
		sys.objects as t
		inner join sys.schemas as sh on
			sh.schema_id = t.schema_id
		inner join sys.dm_db_partition_stats as s on
			s.[object_id] = t.[object_id]
		inner join sys.indexes as i on
			s.[object_id] = i.[object_id] and
			s.index_id = i.index_id
		inner join sys.partitions as p on
			p.object_id = t.object_id and
			p.index_id = i.index_id and
			s.partition_number = p.partition_number
	where
		sh.name like ''' + isnull(@schema_name,'%') + ''' and 
		t.name  like ''' + isnull(@tab_name,   '%') + ''' and 
		i.type_desc like ''%COLUMNSTORE'';	
	';



	insert into #src with (tablockx) (
		[object_id],
		index_id,
		sch_name,
		table_name,
		index_name,
		partition_number,
		index_type,
		size_MB,
		row_count,
		fragmentation
	)
	exec (@sql_cmd);

	execute maintenance.InsertExecution
		@Step = N'insert #src',
		@ExecGUID = @exec_guid,
		@ProcID = @proc_id,
		@Rows = @@rowcount;

	



	create unique clustered index cix on #src (sch_name, table_name, index_name, partition_number);
	create unique nonclustered index nix on #src ([object_id], index_id, partition_number);

	execute maintenance.InsertExecution
		@Step = N'idx #src',
		@ExecGUID = @exec_guid,
		@ProcID = @proc_id,
		@Rows = @@rowcount;



	drop table if exists #partition_fragmentation
	create table #partition_fragmentation (
		object_id int not null,
		index_id int not null,
		partition_number int not null,
		fragmentation tinyint not null
	);


	set @sql_cmd = 'use ' + @dbase_name + ';
		with
		index_list as (
			select
				t.[object_id],
				i.index_id,
				sh.name as sch_name,
				t.name  as table_name,
				i.name  as index_name
			from
				sys.objects as t
				inner join sys.schemas as sh on
					sh.schema_id = t.schema_id
				inner join sys.indexes as i on
					t.[object_id] = i.[object_id]
			where
				sh.name like ''' + isnull(@schema_name,'%') + ''' and 
				t.name  like ''' + isnull(@tab_name,   '%') + ''' and 
				i.type_desc like ''%COLUMNSTORE''
		),
		stat_agg as (
			select
				csfs.object_id,
				csfs.index_id,
				csfs.partition_number,
				sum(csfs.total_rows)										   as total_rows,
				sum(csfs.deleted_rows)										   as deleted_rows,
				sum(iif(state_desc not in (''COMPRESSED''), csfs.total_rows, 0)) as no_compressed_rows
			from
				index_list as i
				inner loop join sys.dm_db_column_store_row_group_physical_stats as csfs on
					i.[object_id] = csfs.[object_id] and
					csfs.index_id = i.index_id
			group by
				csfs.object_id,
				csfs.index_id,
				csfs.partition_number
		)
		select
			st.[object_id],
			st.index_id,
			st.partition_number,
			isnull(try_cast(100. * (deleted_rows + no_compressed_rows) / nullif(total_rows, 0) as tinyint), 100) as fragmentation
		from
			stat_agg as st
		option (force order)
	';

	insert into #partition_fragmentation (
		object_id,
		index_id,
		partition_number,
		fragmentation
	)
	exec (@sql_cmd);


	execute maintenance.InsertExecution
		@Step = N'insert #partition_fragmentation',
		@ExecGUID = @exec_guid,
		@ProcID = @proc_id,
		@Rows = @@rowcount;

	create unique clustered index nix on #partition_fragmentation ([object_id], index_id, partition_number);

	execute maintenance.InsertExecution
		@Step = N'idx #partition_fragmentation',
		@ExecGUID = @exec_guid,
		@ProcID = @proc_id,
		@Rows = @@rowcount;



	update s
	set
		fragmentation = p.fragmentation
	from
		#src as s
		inner join #partition_fragmentation as p on
			p.[object_id] = s.[object_id] and
			s.index_id = p.index_id and
			s.partition_number = p.partition_number


	execute maintenance.InsertExecution
		@Step = N'update #src',
		@ExecGUID = @exec_guid,
		@ProcID = @proc_id,
		@Rows = @@rowcount;

	update tgt
	set
		tgt.index_type = src.index_type,
		tgt.size_MB = src.size_MB,
		tgt.row_count = src.row_count,
		tgt.fragmentation = src.fragmentation,
		tgt.mt_update_dt = @mt_dt
	from
		maintenance_idx.index_stat as tgt
		inner join #src as src on
			tgt.dbase_name = @dbase_name and
			tgt.sch_name = src.sch_name and
			tgt.table_name = src.table_name and
			tgt.index_name = src.index_name and
			tgt.partition_number = src.partition_number
	where
		not exists (
			select
				tgt.index_type,
				tgt.size_MB,
				tgt.row_count,
				tgt.fragmentation
			intersect
			select
				src.index_type,
				src.size_MB,
				src.row_count,
				src.fragmentation
		)



	execute maintenance.InsertExecution
		@Step = 'update maintenance_idx.index_stat',
		@ExecGUID = @exec_guid,
		@ProcID = @proc_id,
		@Rows = @@rowcount;



	insert into maintenance_idx.index_stat (
		dbase_name,
		sch_name,
		table_name,
		index_name,
		partition_number,
		index_type,
		size_MB,
		row_count,
		fragmentation,
		mt_insert_dt,
		mt_update_dt
	)
	select
		@dbase_name as dbase_name,
		sch_name,
		table_name,
		index_name,
		partition_number,
		index_type,
		size_MB,
		row_count,
		fragmentation,
		@mt_dt as mt_insert_dt,
		@mt_dt as mt_update_dt

	from
		#src as src
	where

		not exists (
			select
				1
			from
				maintenance_idx.index_stat as tgt
			where
				tgt.dbase_name = @dbase_name and
				tgt.sch_name = src.sch_name and
				tgt.table_name = src.table_name and
				tgt.index_name = src.index_name and
				tgt.partition_number = src.partition_number
		);

	execute maintenance.InsertExecution
		@Step = 'FINISH - insert maintenance_idx.index_stat',
		@ExecGUID = @exec_guid,
		@ProcID = @proc_id,
		@Rows = @@rowcount;


end