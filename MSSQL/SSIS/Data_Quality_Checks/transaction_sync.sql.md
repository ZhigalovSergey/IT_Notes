```sql
use [MDWH_RAW]
go

-- truncate table MDWH_STG.stg_bank_reestr_sberspasibo.transaction_error
-- select top 100 * from MDWH_STG.stg_bank_reestr_sberspasibo.transaction_error
-- select count(*) cnt from MDWH_STG.stg_bank_reestr_sberspasibo.transaction_error
-- select top 100 * from [raw_bank_reestr_sberspasibo].[transaction]
-- select count(*) cnt from [raw_bank_reestr_sberspasibo].[transaction]
-- exec [raw_bank_reestr_sberspasibo].[transaction_sync]

create procedure [raw_bank_reestr_sberspasibo].[transaction_sync]
as begin
	set nocount on;

	declare @dt datetime = getdate()
	-- Checks new rows
	select
		[file_name],
		[order_queue_id],
		[tran_date],
		[accural_type],
		[fee_amount],
		[discount_amount],
		[sign_discount_amount],
		count(*) over (partition by [file_name], [order_queue_id], [sign_discount_amount] order by (select null)
		) cnt
	into #checks
	from
		MDWH_STG.stg_bank_reestr_sberspasibo.[transaction]

	-- Check duplicate key
	insert into MDWH_STG.stg_bank_reestr_sberspasibo.transaction_error (
		[file_name],
		[row],
		[description]
	)
	select
		[file_name],
		isnull(cast([order_queue_id] as nvarchar), '') + ';' + isnull(cast([tran_date] as nvarchar), '') + ';' + isnull([accural_type], '') + ';' + isnull(cast([fee_amount] as nvarchar), '') + ';' + isnull(cast([discount_amount] as nvarchar), '') as [row],
		'insert duplicate key'
	from
		#checks
	where
		cnt > 1

	-- Check null key
	insert into MDWH_STG.stg_bank_reestr_sberspasibo.transaction_error (
		[file_name],
		[row],
		[description]
	)
	select
		[file_name],
		isnull(cast([order_queue_id] as nvarchar), '') + ';' + isnull(cast([tran_date] as nvarchar), '') + ';' + isnull([accural_type], '') + ';' + isnull(cast([fee_amount] as nvarchar), '') + ';' + isnull(cast([discount_amount] as nvarchar), '') as [row],
		'insert null key'
	from
		#checks
	where
		cnt = 1 and
		[order_queue_id] is null

	-- Insert new rows
	insert into [raw_bank_reestr_sberspasibo].[transaction] (
		[file_name],
		[order_queue_id],
		[tran_date],
		[accural_type],
		[fee_amount],
		[discount_amount],
		[sign_discount_amount],
		[mt_insert_dt],
		[mt_update_dt]
	)
	select
		[file_name],
		[order_queue_id],
		[tran_date],
		[accural_type],
		[fee_amount],
		[discount_amount],
		[sign_discount_amount],
		@dt,
		@dt
	from
		#checks src
	where
		cnt = 1 and
		[order_queue_id] is not null and
		not exists (
			select top (1)
				*
			from
				[raw_bank_reestr_sberspasibo].[transaction] tgt
			where
				tgt.[file_name] = src.[file_name] and
				tgt.[order_queue_id] = src.[order_queue_id] and
				tgt.[sign_discount_amount] = src.[sign_discount_amount]
		)

	-- Update the old rows
	update tgt
	set
		tgt.[tran_date] = src.[tran_date],
		tgt.[accural_type] = src.[accural_type],
		tgt.[fee_amount] = src.[fee_amount],
		tgt.[discount_amount] = src.[discount_amount],
		tgt.[mt_update_dt] = @dt
	from
		[raw_bank_reestr_sberspasibo].[transaction] tgt
		inner join #checks src on
			tgt.[file_name] = src.[file_name] and
			tgt.[order_queue_id] = src.[order_queue_id] and
			tgt.[sign_discount_amount] = src.[sign_discount_amount]
	where
		src.cnt = 1
		and src.[order_queue_id] is not null
		and not
		(
			isnull(cast(tgt.[tran_date] as nvarchar), '||') = isnull(cast(src.[tran_date] as nvarchar), '||')
			and isnull(tgt.[accural_type], '||') = isnull(src.[accural_type], '||')
			and isnull(cast(tgt.[fee_amount] as nvarchar), '||') = isnull(cast(src.[fee_amount] as nvarchar), '||')
			and isnull(cast(tgt.[discount_amount] as nvarchar), '||') = isnull(cast(src.[discount_amount] as nvarchar), '||')
		)
end
	go
```