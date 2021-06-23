# Process kill

```sql
IF (SELECT OBJECT_ID('tempdb..#killprocces'))IS NOT NULL DROP TABLE #killprocces

select es.cmd,db_name(es.dbid) as dbname,es.blocked, es.loginame,es.hostname, es.lastwaittype,spid,es.program_name,
[Individual Query] = SUBSTRING (st.text,es.stmt_start/2,
(CASE
WHEN es.stmt_end = -1 THEN LEN(CONVERT(NVARCHAR(MAX), st.text)) * 2
ELSE es.stmt_end
END - es.stmt_start)/2+2),
ib.event_info,
dmes.last_request_end_time,
dmes.last_request_start_time,
[transaction_begin_time],
er.wait_time,
es.open_tran, 0 AS Killed
into #killprocces

from sys.dm_tran_session_transactions dts

-- inner join sys.dm_exec_sessions es on es.session_id = dts.session_id
inner join sys.sysprocesses es on dts.session_id=es.spid
left join sys.dm_exec_sessions dmes
on dmes.session_id=es.spid
left join sys.dm_exec_requests er
on er.session_id=es.spid


left join
sys.dm_tran_active_transactions dta
on dts.transaction_id=dta.transaction_id
--left join sys.dm_tran_database_transactions dt on

-- dts.transaction_id=dt.transaction_id
outer apply sys.dm_exec_input_buffer (es.spid,er.request_id) ib
outer apply sys.dm_exec_sql_text(es.sql_handle) st

where es.spid>49
--and ib.event_info not like '%ProductService.refresh_service_data%'
and ib.event_info not like '%update_stats%'
and ib.event_info not like '%index_rebuild%'
and ib.event_info not like '%DBCC %'
and ib.event_info not like 'sp_server_diagnostics'
and ltrim(cmd) not like 'BACKUP DATABAS%'

--and dateadd(hour,-1,getdate())> IIF(es.status in ('running','suspended') , [last_request_start_time], [transaction_begin_time])
and ((dateadd(hour,-4,getdate())> [last_request_start_time] and es.status in ('running','suspended'))
or (dateadd(minute,-10,getdate())> transaction_begin_time and es.status = 'sleeping'))
and dateadd(hour,-1,getdate())> es.login_time
and isnull(es.lastwaittype,'NULL') not in ( 'SP_SERVER_DIAGNOSTICS_SLEEP','WAITFOR','BROKER_RECEIVE_WAITFOR','TRACEWRITE')
order by dmes.last_request_end_time desc


begin

declare @id int
declare @sql nvarchar(255)

DECLARE session_Cursor CURSOR FOR
select spid from #killprocces where killed=0
OPEN session_Cursor
FETCH NEXT FROM session_Cursor
into @id



WHILE @@FETCH_STATUS = 0
BEGIN
set @sql=''
set @sql = 'kill '+cast(@id as nvarchar(255))
exec (@sql)


declare @body varchar(max),
@subject varchar(255)

set @body = ''
SELECT @body='Command = '+ rtrim (isnull(cmd,''))+
'<br>' +'Database = ' + rtrim (isnull(cast(dbname as varchar),''))+ '</b>'+
'<br>' +'Blocked = '+ rtrim (isnull(cast(Blocked as varchar),'')) + '</b>'+
'<br>' +'Loginame = '+ rtrim (isnull(cast(Loginame as varchar),'')) + '</b>'+
'<br>' +'Hostname = '+ rtrim (isnull(hostname,''))+ '</b>'+
'<br>' +'Lastwaittype = '+ rtrim (isnull(lastwaittype,''))+ '</b>'+
'<br>' +'Session ID = '+ rtrim (isnull( spid,''))+ '</b>'+
'<br>' +'Application Name = '+ rtrim (isnull([program_name],''))+ '</b>'+
'<br>' +'Individual query = '+ rtrim (isnull([Individual Query],''))+ '</b>'+
'<br>' +'Exec Text = '+rtrim (isnull([event_info],''))+ '</b>'+
'<br>' +'Last request End time = '+rtrim (isnull(last_request_end_time,''))+ '</b>'+
'<br>' +'Last request Start time = '+rtrim (isnull(last_request_start_time,'')) + '</b>'+
'<br>' +'Tran Start time = '+rtrim (isnull([transaction_begin_time],''))+ '</b>'+
--'<br>' +'Blocked query plan = '+[Individual query plan] + '</b>'
'<br>' +'Wait time = '+rtrim (isnull(wait_time,''))+ '</b>'+
'<br>' +'Open tran count = ' + rtrim (isnull(cast(open_tran as varchar),''))+'</b>'
--'<br>' +'Blocked sql statement = '+ [sessionwaittype] + '</b>'
FROM #killprocces where spid = @id


set @subject = 'Process Killed on '+cast(@@Servername as varchar(255))

exec msdb..sp_send_dbmail @profile_name = 'DBA ALerts',
@recipients = 'dba_alerts@goods.ru',
-- @copy_recipients = 'dba_alerts@goods.ru',
@subject = @subject,
@body = @body,
@body_format = 'HTML'


FETCH NEXT FROM session_Cursor into @id
END;
CLOSE session_Cursor;
DEALLOCATE session_Cursor;
END
```

