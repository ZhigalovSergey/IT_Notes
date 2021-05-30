
/*

exec core.item_snapshot_map

select @@trancount
		if @@trancount > 0
			rollback tran

-- удаление вспомогательных объектов
declare @sql nvarchar(4000)

set @sql = 
N'drop table if exists [core].[item_snapshot_item_key_buf_' + cast(@@SPID as nvarchar(10)) + ']
drop table if exists [core].[item_snapshot_buf_' + cast(@@SPID as nvarchar(10)) + ']'

exec sp_executesql @sql

*/

-- exec core.item_snapshot_map

alter proc core.item_snapshot_map
as


-- создание буфера диапазона item_key
declare @sql nvarchar(4000)

set @sql = 
		N'select [item_key] 
		into [core].[item_snapshot_item_key_buf_' + cast(@@SPID as nvarchar(10)) + '] 
		from core.item_snapshot 
		where 1 = 0'

exec sp_executesql @sql


-- создание буферной таблицы
set @sql = 
		N'select
			[snapshot_date],
			[item_key],
			[offers_count],
			[is_item_with_offers],
			[is_item_with_orders],
			[is_exists_web_level],
			[is_more_than_one_offer],
			[is_new_active_sku]
		into [core].[item_snapshot_buf_' + cast(@@SPID as nvarchar(10)) + ']
		from core.item_snapshot
		where 1 = 0'

exec sp_executesql @sql


-- создание check таблицы под сессию
set @sql = 
		N'if OBJECT_ID(''core.item_snapshot_item_key_check_' + cast(@@SPID as nvarchar(10)) + ''', ''U'') IS NULL
		create table [core].[item_snapshot_item_key_check_' + cast(@@SPID as nvarchar(10)) + '](
			[item_key] [bigint] NOT NULL
			)'
exec sp_executesql @sql


declare @part_id int = 0, @part_cnt int = 1000000, @cnt int = 1, @rows_affected int, @iter_number int = 1

while @part_cnt > @part_id and @cnt > 0
begin
	begin try
		begin tran
			set @part_id = 1 + 
				(select isnull(max(part_id), 0) from [core].[item_snapshot_map_of_tasks] (nolock))

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

					set @sql = 
					N'insert into [core].[item_snapshot_item_key_check_' + cast(@@SPID as nvarchar(10)) + '] with (tablock)
					([item_key])
					select [item_key]
					from [core].[item_snapshot_item_key]
					where id >= 10000*(' + cast(@part_id as nvarchar(10)) + ' - 1) and id < 10000*(' + cast(@part_id as nvarchar(10)) + ')'
					exec sp_executesql @sql

					set @cnt = @@rowcount

					-- заполнение буфера диапазона item_key
					set @sql = 
						N'insert into [core].[item_snapshot_item_key_buf_' + cast(@@SPID as nvarchar(10)) + '] with (tablock)
						([item_key])
						select [item_key]
						from [core].[item_snapshot_item_key]
						where id >= 10000*(' + cast(@part_id as nvarchar(10)) + ' - 1) and id < 10000*(' + cast(@part_id as nvarchar(10)) + ')'
		
					exec sp_executesql @sql

					insert into [core].[item_snapshot_log] 
					(
					iter_number, 
					inserted_item_key_check,
					inserted_item_snapshot_tgt,
					step,
					comment,
					insert_dt)
					values (@iter_number, @cnt, 0, 'filled item_snapshot_item_key_buf', 'filled item_snapshot_item_key_buf_' + cast(@@SPID as nvarchar(10)), getdate())

					-- заполнение буферной таблицы
					set @sql = 
					N'insert into [core].[item_snapshot_buf_' + cast(@@SPID as nvarchar(10)) + '] with (tablock)
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
						inner join [core].[item_snapshot_item_key_buf_' + cast(@@SPID as nvarchar(10)) + '] buf on sn.item_key = buf.item_key'	
		
					exec sp_executesql @sql

					set @rows_affected = @@ROWCOUNT

					insert into [core].[item_snapshot_log] 
					(
					iter_number, 
					inserted_item_key_check,
					inserted_item_snapshot_tgt,
					step,
					comment,
					insert_dt)
					values (@iter_number, 0, @rows_affected, 'filled item_snapshot_buf', 'filled item_snapshot_buf_' + cast(@@SPID as nvarchar(10)), getdate())

					-- заполнение цулевой таблицы
					set @sql = 
					N';with
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
							[core].[item_snapshot_buf_' + cast(@@SPID as nvarchar(10)) + ']
					)
					insert into [core].[item_snapshot_tgt] with -- (tablock)
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
					from disappeared'
	
					exec sp_executesql @sql

					set @rows_affected = @@ROWCOUNT

					insert into [core].[item_snapshot_log] 
					(
					iter_number,
					inserted_item_key_check,
					inserted_item_snapshot_tgt,
					step,
					comment,
					insert_dt)
					values (@iter_number, 0, @rows_affected, 'filled item_snapshot_tgt', 'filled item_snapshot_tgt_' + cast(@@SPID as nvarchar(10)), getdate())

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

	-- очистка буферных таблиц
	set @sql = 
	N'	truncate table [core].[item_snapshot_item_key_buf_' + cast(@@SPID as nvarchar(10)) + ']
		truncate table [core].[item_snapshot_buf_' + cast(@@SPID as nvarchar(10)) + ']'
	
	exec sp_executesql @sql

	set @iter_number = @iter_number + 1
	insert into [core].[item_snapshot_log] 
		(
		iter_number, 
		inserted_item_key_check,
		inserted_item_snapshot_tgt,
		step,
		comment,
		insert_dt)
		values (@iter_number, 0, 0, 'repeat of cycle', 'repeat of cycle SPID ' + cast(@@SPID as nvarchar(10)), getdate())

end


-- удаление вспомогательных объектов
set @sql = 
N'drop table if exists [core].[item_snapshot_item_key_buf_' + cast(@@SPID as nvarchar(10)) + ']
drop table if exists [core].[item_snapshot_buf_' + cast(@@SPID as nvarchar(10)) + ']'

exec sp_executesql @sql