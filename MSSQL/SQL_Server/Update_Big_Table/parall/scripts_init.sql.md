```sql
use [MDWH]
go

-- select @@version

---------------------------------------------------------------------------------------------------------
----------------- Создание партиционированной таблицы [core].[item_snapshot_tgt] --------------------
-------------------------------------------------------------------------------------------------------

create nonclustered index [ix_item_snapshot_item_key] on [core].[item_snapshot]
(
[item_key] asc,
[snapshot_date] asc
)
go

alter table [core].[item_snapshot] rebuild partition = all 
with (data_compression = row)
go

-- вычисление диапазона item_key
-- drop table if exists [core].[item_snapshot_item_key]
select [item_key], identity(int, 1, 1) as id
into [core].[item_snapshot_item_key]
from core.item_snapshot
group by [item_key]

-- будем контролировать конечный результат вставкой в другую таблицу
-- при параллельном запуске будут создаваться свои таблицы item_snapshot_item_key_check_@@spid под каждую сессию
-- truncate table [core].[item_snapshot_item_key_check]
-- select * from [core].[item_snapshot_item_key_check]
create table [core].[item_snapshot_item_key_check] (
	[item_key] [bigint] not null
)

-- создание целевой таблицы
-- вычисление списка дат
-- set statistics io on
select [snapshot_date] 
into [core].[item_snapshot_date] 
from core.item_snapshot 
group by [snapshot_date]
-- set statistics io off


-- создание функции партиционирования
-- Таблица или индекс в SQL Server могут содержать до 15 000 секций = 40 лет ежедневных партиций
-- DROP PARTITION SCHEME ps_item_snapshot
-- DROP PARTITION FUNCTION pf_item_snapshot

create partition function pf_item_snapshot (date)
as range right for values ('1900-01-01')

--set statistics io on
--set statistics time on

declare @sql nvarchar(4000),
		@dt nvarchar(10)

declare cursor_snapshot_date cursor
for
 select cast([snapshot_date] as nvarchar(10))
 from [core].[item_snapshot_date]
 --where [snapshot_date] >= '20200101'
 order by [snapshot_date]

open cursor_snapshot_date
fetch next from cursor_snapshot_date into @dt
while @@FETCH_STATUS = 0
begin
	set @sql =
	'ALTER PARTITION FUNCTION pf_item_snapshot ()
	SPLIT RANGE (''' + @dt + ''')'
	--select @sql
	exec sp_executesql
		@sql

	fetch next from cursor_snapshot_date into @dt
end
close cursor_snapshot_date
deallocate cursor_snapshot_date


alter partition function pf_item_snapshot ()
split range ('' + cast(cast(dateadd(d, 1, getdate()) as date) as nvarchar(10)) + '')

--set statistics io off 
--set statistics time off 

-- создание схемы партиционирования
CREATE PARTITION SCHEME ps_item_snapshot
AS PARTITION pf_item_snapshot
ALL TO ('STAT');

-- создание таблицы
-- drop table if exists [core].[item_snapshot_tgt]
-- select top(10) * from [core].[item_snapshot_tgt] 
create table [core].[item_snapshot_tgt] (
	[snapshot_date] [date] not null,
	[item_key] [bigint] not null,
	[offers_count] [int] not null,
	[is_item_with_offers] [bit] not null,
	[is_item_with_orders] [bit] not null,
	[is_exists_web_level] [bit] not null,
	[is_more_than_one_offer] [bit] not null,
	[is_new_active_sku] [bit] not null,
	[is_item_with_disappeared_offers] [bit] not null default 0
) on ps_item_snapshot (snapshot_date)

-- создание таблицы логирования
-- drop table [core].[item_snapshot_log]
-- truncate table [core].[item_snapshot_log]
-- select top(10) * from [core].[item_snapshot_log]
create table [core].[item_snapshot_log] (
	SPID int default @@SPID,
	iter_number int,
	inserted_item_key_check int,
	inserted_item_snapshot_tgt int,
	step nvarchar(100),
	comment nvarchar(100),
	insert_dt datetime
)





------------------------------------------------------------------------------------------------------------------
-------------------  Создание структуры таблиц для параллельного запуска  ----------------------------------------
------------------------------------------------------------------------------------------------------------------

-- карта задач
-- drop table [core].[item_snapshot_map_of_tasks]
-- truncate table [core].[item_snapshot_map_of_tasks] 
-- select * from [core].[item_snapshot_map_of_tasks] (nolock) order by part_id desc
create table [core].[item_snapshot_map_of_tasks] (
	id int not null identity (1, 1),
	SPID int default @@spid,
	part_id int,
	insert_dt datetime
)

-- лог дублей
-- truncate table [core].[item_snapshot_errors]
-- select * from [core].[item_snapshot_errors] (nolock)
create table [core].[item_snapshot_errors] (
	id int not null identity (1, 1),
	SPID int default @@spid,
	part_id int,
	ErrorNumber int,
	ErrorProcedure nvarchar(255),
	ErrorLine int,
	ErrorMessage nvarchar(255),
	insert_dt datetime
)







-- удаление вспомогательных объектов
drop index [ix_item_snapshot_item_key] on [core].[item_snapshot]
go

drop table if exists [core].[item_snapshot_item_key]
drop table if exists [core].[item_snapshot_item_key_check]

drop table if exists [core].[item_snapshot_date]
drop table if exists [core].[item_snapshot_log]

drop table if exists [core].[item_snapshot_map_of_tasks]
drop table if exists [core].[item_snapshot_errors]






-- select * from sys.tables where name like 'item_snapshot_tgt_%'

declare @sql nvarchar(4000),
		@name nvarchar(100)

declare cursor_name cursor
for
select name from sys.tables where name like 'item_snapshot_tgt_%'

open cursor_name
fetch next from cursor_name into @name
while @@FETCH_STATUS = 0
begin
	set @sql =
	'drop table if exists core.' + @name
	--select @sql
	exec sp_executesql @sql
	fetch next from cursor_name into @name
end
close cursor_name
deallocate cursor_name




-- select * from sys.tables where name like 'item_snapshot_item_key_check_%'


declare @sql nvarchar(4000),
		@name nvarchar(100)

declare cursor_name cursor
for
select name from sys.tables where name like 'item_snapshot_item_key_check_%'

open cursor_name
fetch next from cursor_name into @name
while @@FETCH_STATUS = 0
begin
	set @sql =
	'drop table if exists core.' + @name
	--select @sql
	exec sp_executesql @sql
	fetch next from cursor_name into @name
end
close cursor_name
deallocate cursor_name
```