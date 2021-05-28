```sql
use [MDWH]
go


create nonclustered index [ix_item_snapshot_item_key] on [core].[item_snapshot]
(
[item_key] asc
) 
go

-- вычисление диапазона item_key
select [item_key] into [core].[item_snapshot_item_key] from core.item_snapshot group by [item_key]

-- создание буфера диапазона item_key
select [item_key] into [core].[item_snapshot_item_key_buf] from core.item_snapshot where 1 = 0

-- создание буферной таблицы
select
	[snapshot_date],
	[item_key],
	[offers_count],
	[is_item_with_offers],
	[is_item_with_orders],
	[is_exists_web_level],
	[is_more_than_one_offer],
	[is_new_active_sku]
into [core].[item_snapshot_buf]
from
	core.item_snapshot
where
	1 = 0

drop index [ix_item_snapshot_buf] on [core].[item_snapshot_buf] 
create nonclustered index [ix_item_snapshot_buf] on [core].[item_snapshot_buf]
(
[item_key] asc,
[snapshot_date] asc
) 
go


-- создание целевой таблицы
-- вычисление списка дат
select [snapshot_date] into [core].[item_snapshot_item_snapshot_date] from core.item_snapshot group by [snapshot_date]

declare @list_dt nvarchar(4000) = ''',''' + cast(cast(getdate() as date) as nvarchar(10)) + ''''
select top (300)
	@list_dt = ''',''' + cast([snapshot_date] as nvarchar(10)) + @list_dt
from
	[core].[item_snapshot_item_snapshot_date]
order by
	[snapshot_date] desc

select @list_dt = '''1900-01-01' + @list_dt --substring(@list_dt, 3, len(@list_dt))
--select @list_dt

--DROP PARTITION SCHEME ps_item_snapshot
--DROP PARTITION FUNCTION pf_item_snapshot

declare @sql nvarchar(4000)
--Таблица или индекс в SQL Server могут содержать до 15 000 секций.
set @sql =
'CREATE PARTITION FUNCTION [pf_item_snapshot] (DATE)
AS RANGE RIGHT FOR VALUES 
(' + @list_dt + ')'

exec sp_executesql
	@sql

CREATE PARTITION SCHEME ps_item_snapshot
AS PARTITION pf_item_snapshot
ALL TO ('STAT');

--drop table if exists [core].[item_snapshot_tgt]
create table [core].[item_snapshot_tgt] (
	[snapshot_date] [date] not null,
	[item_key] [bigint] not null,
	[offers_count] [int] not null,
	[is_item_with_offers] [bit] not null,
	[is_item_with_orders] [bit] not null,
	[is_exists_web_level] [bit] not null,
	[is_more_than_one_offer] [bit] not null,
	[is_new_active_sku] [bit] not null,
	[is_item_with_disappeared_offers] [bit] not null
) ON ps_item_snapshot (snapshot_date)

-- создание таблицы логирования
--drop table [core].[item_snapshot_log]
truncate table [core].[item_snapshot_log]
create table [core].[item_snapshot_log] (
	iter_number int,
	deleted_item_key_count int,
	inserted_item_snapshot_tgt int,
	comment nvarchar(100),
	insert_dt datetime
	)

declare @item_snapshot_buf TABLE(
	[snapshot_date] [date] not null,
	[item_key] [bigint] not null,
	[offers_count] [int] not null,
	[is_item_with_offers] [bit] not null,
	[is_item_with_orders] [bit] not null,
	[is_exists_web_level] [bit] not null,
	[is_more_than_one_offer] [bit] not null,
	[is_new_active_sku] [bit] not null
	)

-- организация цикла добавления нового поля
declare @cnt int = 1, @rows_affected int, @iter_number int = 1

set @iter_number = (select max(iter_number)+1 from [core].[item_snapshot_log])

while @cnt > 0
begin
	begin tran
		-- заполнение буфера диапазона item_key
		delete top (1000000) from [core].[item_snapshot_item_key]
		output deleted.[item_key]
		into [core].[item_snapshot_item_key_buf]

		set @cnt = @@ROWCOUNT
		--select @cnt

		insert into [core].[item_snapshot_log] 
		(iter_number, 
		deleted_item_key_count,
		inserted_item_snapshot_tgt,
		comment,
		insert_dt)
		values (@iter_number, @cnt, 0, 'calc item_snapshot_buf', getdate())

		-- заполнение буферной таблицы
		insert into [core].[item_snapshot_buf]
			(
				[snapshot_date]
				,[item_key]
				,[offers_count]
				,[is_item_with_offers]
				,[is_item_with_orders]
				,[is_exists_web_level]
				,[is_more_than_one_offer]
				,[is_new_active_sku]
			)
		select 	[snapshot_date]
				,sn.[item_key]
				,[offers_count]
				,[is_item_with_offers]
				,[is_item_with_orders]
				,[is_exists_web_level]
				,[is_more_than_one_offer]
				,[is_new_active_sku]						
		from core.item_snapshot sn
			inner join [core].[item_snapshot_item_key_buf] buf on sn.item_key = buf.item_key		
		
		set @rows_affected = @@ROWCOUNT

		insert into [core].[item_snapshot_log] 
		(iter_number, 
		deleted_item_key_count,
		inserted_item_snapshot_tgt,
		comment,
		insert_dt)
		values (@iter_number, @cnt, @rows_affected, 'calc disappeared', getdate())

		;
		with
		disappeared as (
			select
				[snapshot_date]
				,[item_key]
				,[offers_count]
				,[is_item_with_offers]
				,[is_item_with_orders]
				,[is_exists_web_level]
				,[is_more_than_one_offer]
				,[is_new_active_sku]
				,case when 
					lag(cast(is_item_with_offers as tinyint), 1, 0) over (partition by item_key order by snapshot_date) - cast(cast(is_item_with_offers as tinyint) as int) = 1
					then 1 
					else 0 
				end as is_item_with_disappeared_offers
			from
				[core].[item_snapshot_buf]
		)
		insert into [core].[item_snapshot_tgt]
			(
				[snapshot_date]
				,[item_key]
				,[offers_count]
				,[is_item_with_offers]
				,[is_item_with_orders]
				,[is_exists_web_level]
				,[is_more_than_one_offer]
				,[is_new_active_sku]
				,[is_item_with_disappeared_offers]
			)
		select 
			[snapshot_date]
			,[item_key]
			,[offers_count]
			,[is_item_with_offers]
			,[is_item_with_orders]
			,[is_exists_web_level]
			,[is_more_than_one_offer]
			,[is_new_active_sku]
			,is_item_with_disappeared_offers
		from disappeared

		set @rows_affected = @@ROWCOUNT

		insert into [core].[item_snapshot_log] 
		(iter_number,
		deleted_item_key_count,
		inserted_item_snapshot_tgt,
		comment,
		insert_dt)
		values (@iter_number, @cnt, @rows_affected, 'commit', getdate())

	commit tran
	--rollback tran

	-- очистка буферных таблиц
	--delete @item_snapshot_buf
	truncate table [core].[item_snapshot_item_key_buf]
	truncate table [core].[item_snapshot_buf]
	set @iter_number = @iter_number + 1
end




-- удаление вспомогательных объектов
drop index [ix_item_snapshot_item_key] on [core].[item_snapshot]
go

drop table if exists [core].[item_snapshot_item_key]
drop table if exists [core].[item_snapshot_item_key_buf]
drop table if exists [core].[item_snapshot_item_snapshot_date]
drop table if exists [core].[item_snapshot_buf]
drop table if exists [core].[item_snapshot_log]

```