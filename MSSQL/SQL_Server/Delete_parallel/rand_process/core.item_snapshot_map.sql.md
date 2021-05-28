```sql
-- exec core.item_snapshot_map

alter proc core.item_snapshot_map
as

declare @part_id int = 0, @part_cnt int = 1000000, @time int, @delay nvarchar(8), @cnt int = 1 --, @i int = 10

while @part_cnt > @part_id and @cnt > 0 -- and @i < 59
begin
	begin try
		begin tran
			set @part_id = 1 + 
				(select isnull(max(part_id), 0) from [core].[item_snapshot_map_of_tasks] (nolock))

			-- давай поймаем ошибку дублей - 'The part_id already exists'
			--set @delay = '14:28:' + cast(@i as nvarchar(2))
 			--waitfor time @delay

			insert into [core].[item_snapshot_map_of_tasks] 
				(
				[part_id]
				,[insert_dt]
				)
			select @part_id, getdate()

			if (select spid
				from (select spid, row_number() over (partition by part_id order by id) rn
						from [core].[item_snapshot_map_of_tasks] (nolock)
						where part_id = @part_id
						) t
				where rn = 1) <> @@spid

				THROW 51000, 'The part_id already exists', 1

			else 
				begin

					-- давай поймаем ошибку дублей - 'The part_id already exists'
					--waitfor delay '00:00:00.010'

					insert into [core].[item_snapshot_item_key_check] ([item_key])
					select [item_key]
					from [core].[item_snapshot_item_key]
					where $PARTITION.pf_item_snapshot_item_key (id) = $PARTITION.pf_item_snapshot_item_key(10000*(@part_id - 1))

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
				end
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
			@part_id,
			ERROR_NUMBER() as ErrorNumber,
			ERROR_PROCEDURE() as ErrorProcedure,
			ERROR_LINE() as ErrorLine,
			ERROR_MESSAGE() as ErrorMessage,
			getdate()

	end catch

	-- set @i = @i + 1
end
```