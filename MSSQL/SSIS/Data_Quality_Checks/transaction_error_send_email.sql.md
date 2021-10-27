```sql

-- drop procedure [stg_bank_reestr_sberspasibo].[transaction_error_send_email]

create procedure [stg_bank_reestr_sberspasibo].[transaction_error_send_email]
as begin
	set nocount on;

	drop table if exists #errors
	create table #errors (
		[file_name] nvarchar(255) not null,
		[row] nvarchar(512) not null,
		[description] nvarchar(4000) null
	)

	insert into #errors (
		[file_name],
		[row],
		[description]
	)
	select [file_name], [row], [description] from [stg_bank_reestr_sberspasibo].[transaction_error] as e


	if (exists (select 1 from #errors)
		)
	begin
		declare @subject nvarchar(max) = N'Ошибки в файлах каталога \\mir-sdb-005\Input\reestr_bank\sberspasibo',
				@message_text nvarchar(max) = N'Уважаемые коллеги, при загрузке данных sberspasibo обнаружены ошибки:',
				@HTML nvarchar(max),
				@data_context nvarchar(max) = '<br><span style="font-size:12px">' + isnull(db_name() + '.' + object_schema_name(@@procid) + '.' + object_name(@@procid), 'proc') + '</span>',
				@recipients varchar(255) = 'artem.shabrov@sbermegamarket.ru',
				@blind_copy_recipients varchar(255) = 'sergey.zhigalov@sbermegamarket.ru'

		set @HTML = @message_text +
		N'<br><br>
		<table border="1" cellpadding="1" cellspacing="1">
		<tr style="background-color: gold;">
		<th>file name</th>
		<th>row</th>
		<th>description</th>
		</tr>' +
		cast((
			select
				td = [file_name],
				'',
				td = [row],
				'',
				td = [description]
			from
				#errors
			order by
				[file_name]
			for xml path ('tr'), type
		)
		as nvarchar(max)) +
		N'</table>' +
		@data_context;

		exec msdb.dbo.sp_send_dbmail
			@profile_name = 'BI_Goods',
			@recipients = @recipients,
			@blind_copy_recipients = @blind_copy_recipients,
			@subject = @subject,
			@body_format = 'HTML',
			@body = @HTML
	end
end
	go
```