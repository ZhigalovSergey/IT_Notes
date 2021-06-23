# kill блокирующих сессий

Создаем пустую таблицу

```sql
create table [core].[item_snapshot_сlean] (
	[snapshot_date] [date] not null,
	[item_key] [bigint] not null,
	[offers_count] [int] not null,
	[is_item_with_offers] [bit] not null,
	[is_item_with_orders] [bit] not null,
	[is_exists_web_level] [bit] not null,
	[is_more_than_one_offer] [bit] not null,
	[is_new_active_sku] [bit] not null,
	[is_item_with_disappeared_offers] [bit] not null default 0,
	constraint [pk_item_snapshot_сlean] primary key clustered
	(
	[item_key] asc,
	[snapshot_date] asc
	)
)
go
```

Запускаем тестовый запрос

```sql
begin tran
    select * 
    from core.item_snapshot (holdlock)
    where [snapshot_date] = '1900-01-01'
    waitfor delay '00:02:00'
commit
```

Запускаем запрос с WAIT_AT_LOW_PRIORITY

```sql
alter table core.item_snapshot
switch partition $partition.pf_item_snapshot('1900-01-01')
to [core].[item_snapshot_сlean] 
WITH (WAIT_AT_LOW_PRIORITY (MAX_DURATION = 1 MINUTES, ABORT_AFTER_WAIT = BLOCKERS))
```



### Полезные ссылки:  

- [Докментация по ALTER TABLE](https://docs.microsoft.com/ru-ru/sql/t-sql/statements/alter-table-transact-sql?view=sql-server-ver15)  
- [Detect and Automatically Kill Low Priority Blocking Sessions](https://www.mssqltips.com/sqlservertip/3285/detect-and-automatically-kill-low-priority-blocking-sessions-in-sql-server/)  
- [Manage multiple partitions in multiple filegroups in SQL Server for cleanup purposes](https://www.mssqltips.com/sqlservertip/1580/manage-multiple-partitions-in-multiple-filegroups-in-sql-server-for-cleanup-purposes/)  
- [CHECK CONSTRAINT](https://habr.com/ru/company/infopulse/blog/263833/)  
- [ALTER PARTITION FUNCTION with WAIT_AT_LOW_PRIORITY](https://docs.microsoft.com/en-us/answers/questions/218043/alter-partition-function-with-wait-at-low-priority.html)  