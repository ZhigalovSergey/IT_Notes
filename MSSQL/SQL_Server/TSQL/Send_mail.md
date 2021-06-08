# Отправить письмо

```sql
create procedure [stg_delivery].[payment_errors_send_email]
as begin
	set nocount on;

	if (exists (select 1 from [stg_delivery].[payment_errors])
		)
	begin
		declare @subject nvarchar(max) = N'Ошибки в файлах каталога delivery_payment',
				@message_text nvarchar(max) = N'Уважаемые коллеги, при загрузке данных об оплате заказа от операторов доставки обнаружены ошибки',
				@HTML nvarchar(max),
				@data_context nvarchar(max) = '<br><span style="font-size:12px">' + isnull(db_name() + '.' + object_schema_name(@@procid) + '.' + object_name(@@procid), 'proc') + '</span>',
				@recipients varchar(255) = 'sergey.zhigalov@sbermegamarket.ru'
		
		set @HTML = @message_text +
		N'<br>Перечень ошибок в данных из csv-файлов по платежам от операторов доставки:<br>
		<table border="1" cellpadding="1" cellspacing="1">
		<tr style="background-color: gold;">
		<th>file name</th>
		<th>row</th>
		</tr>' +
		cast((
			select
				td = [file_name],
				'',
				td = [row]
			from
				[stg_delivery].[payment_errors]
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
### Полезные ссылки:  
- [Хранимая процедура sp_send_dbmail](https://docs.microsoft.com/ru-ru/sql/relational-databases/system-stored-procedures/sp-send-dbmail-transact-sql?view=sql-server-ver15)  