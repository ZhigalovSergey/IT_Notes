### [Заметки по SQL Server](../SQLServer_note.md)  

## Обслуживание партиционированных таблиц  

```sql
-- создание партиции на текущий месяц
if not exists
(
	select cast(value as date) snapshot_date
	from sys.partition_functions as pf
		left join sys.partition_range_values as prv on
			prv.function_id = pf.function_id
	where name = 'pf_offer_snapshot'
	and cast(value as date) = dateadd(day, 1, eomonth(@snapshot_date, -1))
)
begin
	begin tran
		alter table core.offer_snapshot
		switch partition $partition.pf_offer_snapshot('1900-01-01')
		to [core].[offer_snapshot_empty] 
		WITH (WAIT_AT_LOW_PRIORITY (MAX_DURATION = 1 MINUTES, ABORT_AFTER_WAIT = BLOCKERS))

		ALTER PARTITION SCHEME ps_offer_snapshot
		NEXT USED [STAT]

		ALTER PARTITION FUNCTION pf_offer_snapshot ()
		SPLIT RANGE ('' + cast(dateadd(day, 1, eomonth(@snapshot_date, -1)) as nvarchar(10)) + '')
	commit
end

-- создание партиции на следующий месяц
if not exists
(
	select cast(value as date) snapshot_date
	from sys.partition_functions as pf
		left join sys.partition_range_values as prv on
			prv.function_id = pf.function_id
	where name = 'pf_offer_snapshot'
	and cast(value as date) = dateadd(day, 1, eomonth(@snapshot_date, 0))
)
begin
	begin tran
		alter table core.offer_snapshot
		switch partition $partition.pf_offer_snapshot('1900-01-01')
		to [core].[offer_snapshot_empty] 
		WITH (WAIT_AT_LOW_PRIORITY (MAX_DURATION = 1 MINUTES, ABORT_AFTER_WAIT = BLOCKERS))

		ALTER PARTITION SCHEME ps_offer_snapshot
		NEXT USED [STAT]

		ALTER PARTITION FUNCTION pf_offer_snapshot ()
		SPLIT RANGE ('' + cast(dateadd(day, 1, eomonth(@snapshot_date, 0)) as nvarchar(10)) + '')
	commit
end
```

