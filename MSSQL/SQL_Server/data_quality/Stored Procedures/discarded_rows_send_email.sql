create procedure [maintenance].[discarded_rows_send_email] @table_full_name sysname = '%', @date_from date = null, @date_to date = null, @recipients varchar(8000) = null
as begin
	set nocount on;

	if (@date_from is null)
	set @date_from = (select dateadd(dd, -2, getdate()))

	if (@date_to is null)
	set @date_to = getdate()

	if (@recipients is null)
	set @recipients = 'sergey.zhigalov@sbermegamarket.ru'

	drop table if exists #res_error
	create table #res_error (
		table_full_name nvarchar(255),
		error_desc nvarchar(255),
		error_cnt int,
		mt_insert_dt datetime2(0)
	)

	declare @sql nvarchar(4000)

	set @sql = 
	'insert into #res_error 
	(
		table_full_name,
		error_desc,
		error_cnt,
		mt_insert_dt
	)
	select
		[table_full_name]			as table_full_name,
		[error_type_desc]			as error_desc,
		[error_count_rows]			as error_cnt,
		mt_insert_dt
	from
		[maintenance].[vw_discarded_rows]
	where
		[table_full_name] like ''' + @table_full_name + ''' and 
		mt_insert_dt >= ''' + convert(nvarchar(8), @date_from, 112) + ''' and mt_insert_dt < ''' + convert(nvarchar(8),(select dateadd(dd, 1, @date_to)),112) + ''''

	--select @sql 
	exec sp_executesql @sql 
	--select * from #res_error

	if (exists (select 1 from #res_error))
	begin
		declare @subject nvarchar(max) = N'Ошибки в представлении GBQ',
				@message_text nvarchar(max) = N'Уважаемые коллеги, при загрузке данных из GBQ обнаружены ошибки',
				@HTML nvarchar(max),
				@data_context nvarchar(max) = '<br><span style="font-size:12px">' + isnull(db_name() + '.' + object_schema_name(@@procid) + '.' + object_name(@@procid), 'proc') + '</span>',
				--@recipients varchar(255) = 'roman.ignatenko@sbermegamarket.ru;pavel.sharapov@sbermegamarket.ru',
				@copy_recipients varchar(255) = 'roman.solanov@sbermegamarket.ru',
				@blind_copy_recipients varchar(255) = 'sergey.zhigalov@sbermegamarket.ru'

		set @HTML = @message_text +
		N'<br>Перечень ошибок в данных из GBQ:<br>
		<table border="1" cellpadding="1" cellspacing="1">
		<tr style="background-color: gold;">
		<th>table_full_name</th>
		<th>error_desc</th>
		<th>error_cnt</th>
		<th>mt_insert_dt</th>
		</tr>' +
		cast((
			select
				td = table_full_name,
				'',
				td = error_desc,
				'',
				td = error_cnt,
				'',
				td = mt_insert_dt
			from
				#res_error
			order by
				table_full_name, error_desc, mt_insert_dt
			for xml path ('tr'), type
		)
		as nvarchar(max)) +
		N'</table>' +
		@data_context;

		exec msdb.dbo.sp_send_dbmail
			@profile_name = 'BI_Goods',
			@recipients = @recipients,
			@copy_recipients = @copy_recipients,
			@blind_copy_recipients = @blind_copy_recipients,
			@subject = @subject,
			@body_format = 'HTML',
			@body = @HTML
	end
end

GO


