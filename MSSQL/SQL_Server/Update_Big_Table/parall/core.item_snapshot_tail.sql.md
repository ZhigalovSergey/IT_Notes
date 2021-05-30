```sql
-- Данные прогрузились без ошибок. Расхождений нет.
-- Но захватили данные текущего дня.
-- Чистим данные за сегодня.

-- select $PARTITION.pf_item_snapshot(cast(getdate() as date))
truncate table core.item_snapshot_tgt
with (PARTITIONS ($PARTITION.pf_item_snapshot(cast(getdate() as date))));
go


-- Для обработки расхождений

declare @time int, @delay nvarchar(8), @cnt int = 1

while @cnt > 0
begin
	begin try
		begin tran

					insert into [core].[item_snapshot_item_key_check] ([item_key])
					select [item_key]
					from [core].[item_snapshot_item_key] src
					where not exists (select top (1) 1
										from [core].[item_snapshot_item_key_check] tgt 
										where tgt.[item_key] = src.item_key)

					set @cnt = @@rowcount

					set @time = 10*rand()
					set @delay = '00:00:' + case 
												when len(cast(@time as nvarchar(2))) = 2 
														then cast(@time as nvarchar(2))
														else '0' + cast(@time as nvarchar(2)) 
												end
					-- время на выполнение задачи
					-- waitfor delay @delay

		commit tran
	end try
	begin catch

		-- select @@trancount
		if @@trancount > 0
			rollback tran

		insert into [core].[item_snapshot_errors] 
		(
		   [spid]
		  ,[part_id]
		  ,[ErrorNumber]
		  ,[ErrorProcedure]
		  ,[ErrorLine]
		  ,[ErrorMessage]
		  ,[insert_dt]
		)
		select 
			@@spid,
			-1,
			ERROR_NUMBER() as ErrorNumber,
			ERROR_PROCEDURE() as ErrorProcedure,
			ERROR_LINE() as ErrorLine,
			ERROR_MESSAGE() as ErrorMessage,
			getdate()

	end catch
end
```