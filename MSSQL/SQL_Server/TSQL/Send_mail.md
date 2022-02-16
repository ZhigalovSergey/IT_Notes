# Отправить письмо

Письмо под конкретную загрузку в DWH

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
Письмо под список объектов получаемых по шаблону

```sql

create procedure [stg_gbq168810_g161014_dwh_output].[errors_send_email]
as begin
	set nocount on;

	declare @sql nvarchar(4000)
	declare @schema_name nvarchar(255)
	declare @tbl_name_error nvarchar(255)
	declare @view_name_gbq nvarchar(255)

	drop table if exists #res_error
	create table #res_error (
		schema_name nvarchar(255),
		view_name_gbq nvarchar(255),
		tbl_name_error nvarchar(255),
		error_type nvarchar(255),
		error_cnt int
	)

	declare tbl_error cursor
	for
	 select
		 sch.name as schema_name,
		 ob.name  as tbl_name_error,
		 substring(ob.name, 1, len(ob.name) - 6) as view_name_gbq
	 from
		 sys.schemas sch
		 left join sys.objects ob on
			 ob.schema_id = sch.schema_id
	 where
		 sch.name like 'stg_gbq168810_g161014_dwh_output' and
		 ob.name like '%error'
	 order by
		 sch.name, ob.name


	open tbl_error
	fetch next from tbl_error into @schema_name, @tbl_name_error, @view_name_gbq

	while @@FETCH_STATUS = 0
	begin
		set @sql = N'
			INSERT INTO #res_error
			(
				schema_name, view_name_gbq, tbl_name_error, error_type, error_cnt
			)
			SELECT ''' + @schema_name + ''', ''' + @view_name_gbq + '''
					, ''' + @tbl_name_error + ''', [error_desc], count(*) cnt
			FROM ' + @schema_name + '.' + @tbl_name_error + '
			where [mt_insert_dt] > cast(getdate() as date)
			group by [error_desc]'

		exec sp_executesql
			@sql

		fetch next from tbl_error into @schema_name, @tbl_name_error, @view_name_gbq
	end

	close tbl_error
	deallocate tbl_error

	-- select * from #res_error

	if (exists (select 1 from #res_error)
		)
	begin
		declare @subject nvarchar(max) = N'Ошибки в представлении GBQ',
				@message_text nvarchar(max) = N'Уважаемые коллеги, при загрузке данных из GBQ обнаружены ошибки',
				@HTML nvarchar(max),
				@data_context nvarchar(max) = '<br><span style="font-size:12px">' + isnull(db_name() + '.' + object_schema_name(@@procid) + '.' + object_name(@@procid), 'proc') + '</span>',
				@recipients varchar(255) = 'roman.ignatenko@sbermegamarket.ru',
				@blind_copy_recipients varchar(255) = 'sergey.zhigalov@sbermegamarket.ru'
		set @HTML = @message_text +
		N'<br>Перечень ошибок в данных из GBQ:<br>
		<table border="1" cellpadding="1" cellspacing="1">
		<tr style="background-color: gold;">
		<th>schema name</th>
		<th>view name GBQ</th>
		<th>table name error</th>
		<th>error_type</th>
		<th>error_cnt</th>
		</tr>' +
		cast((
			select
				td = schema_name,
				'',
				td = view_name_gbq,
				'',
				td = tbl_name_error,
				'',
				td = error_type,
				'',
				td = error_cnt
			from
				#res_error
			order by
				tbl_name_error, error_type
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
```

Письмо по остановленным заданиям

```sql
declare @subject nvarchar(max) = N'Long running jobs are stoped',
		@message_text nvarchar(max) = N'Уважаемые коллеги, внимание! Были остановлены долго работающие задания.',
		@HTML nvarchar(max) = N'<br><table border="1" cellpadding="1" cellspacing="1"><tr style="background-color: gold;"><th>Long running job</th></tr><td>',
		@data_context nvarchar(max) = '<br><span style="font-size:12px">STOP_long_job</span>',
		@recipients varchar(255) = 'sergey.zhigalov@sbermegamarket.ru',
		@blind_copy_recipients varchar(255) = '',
		@stoped varchar(255) = ''

declare @job nvarchar(255)

declare jobs_cursor cursor for   
select N'FD5D119A-0D0C-4D2D-8BC8-9F9856055E70' as job_id -- input_gbq_sessionDetails_GA_7
union all
select N'CBD19FE9-0DE0-410D-A7BE-2A95A1A7F6A1' as job_id -- input_gbq_app_sessions_7d
union all
select N'8A642BC9-BABF-4A02-9169-D04450E4B0B9' as job_id -- input_gbq

open jobs_cursor
fetch next from jobs_cursor
into @job

while @@fetch_status = 0  
begin  
	if exists (
				SELECT sj.Name, sj.job_id, datediff(mi, sja.start_execution_date, getdate()) duration
				FROM msdb.dbo.sysjobs sj
				JOIN msdb.dbo.sysjobactivity sja
				ON sj.job_id = sja.job_id
				WHERE session_id = (
					SELECT MAX(session_id) FROM msdb.dbo.sysjobactivity)	-- make sure this is the most recent run SQL Server Agent
					and sj.job_id = @job
					and sja.start_execution_date IS NOT NULL	-- job is currently running
					and sja.stop_execution_date IS NULL		-- job hasn't stopped running
					and datediff(mi, sja.start_execution_date, getdate()) > 1 -- duration more 240 min
		)
		begin
			exec msdb.dbo.sp_stop_job @job_id = @job
			set @stoped = @stoped + (select name from msdb.dbo.sysjobs where job_id = @job) + '</td>'
		end
	fetch next from jobs_cursor
	into @job
end
close jobs_cursor
deallocate jobs_cursor

if @stoped <> ''
	begin 
		set @HTML = @message_text + @HTML + @stoped + '</tr></table>' + @data_context
		exec msdb.dbo.sp_send_dbmail
			@profile_name = 'BI_Goods',
			@recipients = @recipients,
			@blind_copy_recipients = @blind_copy_recipients,
			@subject = @subject,
			@body_format = 'HTML',
			@body = @HTML
	end
```



### Полезные ссылки:  
- [Хранимая процедура sp_send_dbmail](https://docs.microsoft.com/ru-ru/sql/relational-databases/system-stored-procedures/sp-send-dbmail-transact-sql?view=sql-server-ver15)  
- [Докментация по DECLARE CURSOR](https://docs.microsoft.com/ru-ru/sql/t-sql/language-elements/declare-cursor-transact-sql?view=sql-server-ver15)  
- [mssqltips - SQL Server Cursor Example](https://www.mssqltips.com/sqlservertip/1599/cursor-in-sql-server/)  