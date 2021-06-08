# Синхронизации данных таблицы слоя **stage** в таблицу слоя **raw**

```sql
create procedure [raw_delivery].[payment_sync]
as begin
	set nocount on;

	declare @dt datetime = getdate()

-- Insert new rows
	insert into [raw_delivery].[payment] 
	(
	   [delivery_id]
      ,[delivery_operator]
      ,[delivery_date]
      ,[payment_amount]
      ,[mt_insert_dt]
	)
	SELECT [delivery_id]
      ,[delivery_operator]
      ,[delivery_date]
      ,[payment_amount]
      ,@dt
	FROM [stg_delivery].[payment] src
	where not exists 
			(
				select top (1) * 
				from [raw_delivery].[payment] tgt
				where tgt.delivery_id = src.delivery_id
			)

-- Update the old rows
	update tgt
	set 
		 tgt.[delivery_operator]= src.[delivery_operator]
		,tgt.[delivery_date]	= src.[delivery_date]
		,tgt.[payment_amount]	= src.[payment_amount]
		,tgt.[mt_update_dt]		= @dt
	from [raw_delivery].[payment] tgt
		inner join [stg_delivery].[payment] src on tgt.[delivery_id] = src.[delivery_id]
	where not
		(
			isnull(tgt.[delivery_operator],'') = isnull(src.[delivery_operator],'')
			and isnull(tgt.[delivery_date],'19000101')	= isnull(src.[delivery_date],'19000101')
			and tgt.[payment_amount] = src.[payment_amount]
		)
end
```

