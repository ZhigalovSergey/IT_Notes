# Мониторинг отката транзакции

```sql
select
dt.database_transaction_log_bytes_reserved/1024,
dt.database_transaction_begin_time,host_process_id,cn.client_net_address,
db_name(dt.database_id),
CASE at.transaction_type
WHEN 1 THEN 'Read/write'
WHEN 2 THEN 'Read-only'
WHEN 3 THEN 'System'
WHEN 4 THEN 'Distributed'
END AS transaction_type,dts.session_id as spid,
dt.database_transaction_begin_time,at.transaction_begin_time, es.status as status,
CASE at.transaction_state
WHEN 0 THEN 'Not fully initialized'
WHEN 1 THEN 'Initialized, not started'
WHEN 2 THEN 'Active'
WHEN 3 THEN 'Ended'
WHEN 4 THEN 'Commit initiated'
WHEN 5 THEN 'Prepared, awaiting resolution'
WHEN 6 THEN 'Committed'
WHEN 7 THEN 'Rolling back'
WHEN 8 THEN 'Rolled back'
END AS transaction_state ,
er.blocking_session_id,
er.wait_time,sp.open_tran as trancount,
dt.transaction_id,
es.program_name as [programname],
cast (st.text as varchar(200)) as proctext,
[individual query] = substring (st.text,er.statement_start_offset/2,
(case
when er.statement_end_offset = -1 then len(convert(nvarchar(max), st.text)) * 2
else er.statement_end_offset
end - er.statement_start_offset)/2),
er.wait_type as sessionwaittype,
es.login_name as loginname,
es.host_name as hostname,
dt.database_transaction_log_bytes_reserved/1024 as [LogReserved in KB],
dt.database_transaction_log_bytes_used/1024 as [Logused in KB],
dt.database_transaction_log_record_count AS [Log Records],
es.last_request_start_time,
cast(pln.query_plan as xml) as [individual query plan],at.name as TranType
from sys.dm_tran_session_transactions dts

 inner join sys.dm_exec_sessions es on es.session_id = dts.session_id
inner join sys.sysprocesses sp on es.session_id=sp.spid
left join sys.dm_exec_connections cn on cn.session_id = es.session_id
left join sys.dm_exec_requests er on es.session_id=er.session_id
left join sys.dm_tran_database_transactions dt on
dts.transaction_id=dt.transaction_id
left join sys.dm_tran_active_transactions at on
dts.transaction_id=at.transaction_id
outer apply sys.dm_exec_sql_text((cn.most_recent_sql_handle)) st
outer apply sys.dm_exec_text_query_plan(er.plan_handle, er.statement_start_offset, er.statement_end_offset ) pln
where sp.open_tran>0
and dt.database_id>1 and db_name(dt.database_id) is not null
and transaction_state is not null
and dt.database_transaction_begin_time is not null
and es.session_id in (406)
--and dt.database_transaction_log_record_count >0
order by at.transaction_begin_time desc
```