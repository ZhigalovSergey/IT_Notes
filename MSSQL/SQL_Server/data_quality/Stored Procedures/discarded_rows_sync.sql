--<help>
/*
[maintenance].[discarded_rows_sync] - Логирование отброшенных строк
	@proc_id - @@procid
	@temp_table_name_for_check - данные подлежащие проверки
	@сheck_mode - используемый шаблон для проверки. На данный момент реализован шаблон N'insert duplicate key'

Процедура делает проверку по стандартным шаблонам и логирует строки, которые не прошли проверку в таблице [maintenance].[discarded_rows]
История хранится до 3 месяцев
Для удобства просмотра можно использовать представление [maintenance].[vw_discarded_rows]
Стандартны пример использования: 
	declare @res int
	exec @res = [maintenance].[discarded_rows_sync] @proc_id = @proc_id, @temp_table_name_for_check = '#promo_dashboard', @сheck_mode = N'insert duplicate key'

Также есть возможность сделать пользовательскую проверку и передать результат для логирования в параметрах:
	@error_json - JSON с отброшенными строками
	@error_count_rows - кол-во отброшенных строк

Стандартны пример использования:
	declare @error_json nvarchar(max)
	set @error_json = (
		select 
			id,
			postcode
		from #temp_table_name_for_check
		where len(postcode) not between 6 and 8
		for json auto, include_null_values
		)
	declare @error_count_rows int
	set @error_count_rows = (select count(*) from #temp_table_name_for_check where len(postcode) not between 6 and 8)

	declare @res int
	exec @res = [maintenance].[discarded_rows_sync] @proc_id = @proc_id, @error_json = @error_json, @error_count_rows = @error_count_rows, @сheck_mode = N'error in postcode'

*/
--</help>

create procedure [maintenance].[discarded_rows_sync] 
	@proc_id bigint = null, 
	@sch_name sysname = null,
	@tbl_name sysname = null,
	@temp_table_name_for_check sysname = null, 
	@сheck_mode nvarchar(255), 
	@error_json nvarchar(max) = null, 
	@error_count_rows int = null
as begin
	set nocount on;
	
	-- check proc_name
	if (reverse(substring(reverse(object_name(@proc_id)),1,5)) <> '_sync')
	begin
		raiserror('Error: The proc name must to have suffix %s',15,1,'_sync')
		return -1
	end

	-- check tbl_name
	declare @table_full_name nvarchar(255)
	set @table_full_name = object_schema_name(@proc_id)+'.'+substring(object_name(@proc_id),1,len(object_name(@proc_id))-5)
	if ((select object_id(@table_full_name)) is null)
	begin
		raiserror('Error: The table name %s is not exist!',15,1, @table_full_name)
		return -1
	end

	if (@temp_table_name_for_check is not null)
	begin
		if (@сheck_mode = N'insert duplicate key')
		begin
			--declare @error_json nvarchar(max)
			declare @sql nvarchar(max) = ''
			set @sql = 'set @error_json_out = ('+char(13)+'select top 10'+char(13)
			set @sql = @sql + stuff((select ','+c.name+char(13)
			from tempdb.sys.columns c
			join tempdb.sys.types tp with (nowait) on c.user_type_id = tp.user_type_id
			where object_id = object_id(concat('tempdb.dbo.',@temp_table_name_for_check))
			order by c.column_id
			for xml path(''), type).value('.', 'nvarchar(max)'),1,1,' ')
			set @sql = @sql + 'from '+@temp_table_name_for_check+char(13)
			set @sql = @sql + 'where mt_rn > 1'+char(13)
			set @sql = @sql + 'for json auto, include_null_values)'

			declare @error_json_def nvarchar(100) = N'@error_json_out nvarchar(max) output'
			exec sp_executesql @sql, @error_json_def, @error_json_out = @error_json output
			--select @error_json

			--declare @error_count_rows int
			set @sql = 'set @error_count_rows_out = (select count(*) from '+@temp_table_name_for_check+' where mt_rn > 1)'
			declare @error_count_rows_def nvarchar(100) = N'@error_count_rows_out int output'
			exec sp_executesql @sql, @error_count_rows_def, @error_count_rows_out = @error_count_rows output
			--select @error_count_rows
		end
		
		-- add new check
	end

	if (@error_count_rows = 0 or @error_json is null)
		return 0

	declare @error_type_id int
	declare @dt datetime2(0) = getdate()

	exec [maintenance].[error_type_sync] @сheck_mode, @error_type_id output

	insert into [maintenance].[discarded_rows] 
	(
	[table_full_name]
	,[error_type_id]
	,[error_json]
	,[error_count_rows]
	,[mt_insert_dt]
	)
	select 
		@table_full_name,
		@error_type_id, 
		@error_json,
		@error_count_rows,
		@dt

	return @error_count_rows
end
go