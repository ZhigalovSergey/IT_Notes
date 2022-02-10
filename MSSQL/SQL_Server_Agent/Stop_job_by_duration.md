#### [Заметки по SQL Server Agent](./SQLAgent_note.md)  
## Остановка заданий по длительности

```sql
declare @job nvarchar(255)

declare jobs_cursor cursor for   
select N'FD5D119A-0D0C-4D2D-8BC8-9F9856055E70' as job_id -- input_gbq_sessionDetails_GA_7
union all
select N'CBD19FE9-0DE0-410D-A7BE-2A95A1A7F6A1' as job_id -- input_gbq_app_sessions_7d
 
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
					and datediff(mi, sja.start_execution_date, getdate()) > 9 -- duration more 240 min
		)
		exec msdb.dbo.sp_stop_job @job_id = @job
	fetch next from jobs_cursor
	into @job
end
```

