```sql
-- вычисление диапазона item_key
-- drop table [core].[item_snapshot_item_key]
-- select [item_key], IDENTITY(int, 1,1) as id into [core].[item_snapshot_item_key] from core.item_snapshot group by [item_key]

-- организация цикла добавления нового поля
declare @cnt int = 1

while @cnt > 0
begin
	begin tran
		
		delete top (10)
		from [core].[item_snapshot_item_key]
		where id > 0 and id < 1000000

		set @cnt = @@ROWCOUNT

		WAITFOR DELAY '00:00:05'

	commit
end
```