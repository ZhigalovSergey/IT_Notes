
create procedure [core].[#anchor#_sync]
as begin
	set nocount on

	begin try
		begin tran

		declare @mt_dt datetime2(0) = sysdatetime()
		declare @load_dt date = dateadd(dd, -3, @mt_dt)

		drop table if exists ##anchor#_s_#src_name#
		create table ##anchor#_s_#src_name# (
			#anchor#_key bigint not null,
			#anchor#_id [NVARCHAR](64) not null,
			#anchor#_source [NVARCHAR](4000) null,
			#anchor#_medium [NVARCHAR](4000) null,
			#anchor#_campaign [NVARCHAR](4000) null
		)

		insert into ##anchor#_s_#src_name# (
			#anchor#_key,
			#anchor#_id,
			#anchor#_source,
			#anchor#_medium,
			#anchor#_campaign
		)
		select
			next value for core.#anchor#_sequence as #anchor#_key,
			[id],
			[source],
			[medium],
			[campaign]
		from
			[MDWH_RAW].[raw_gbq168810_g161014_dwh_output].[dimUtmExtended] src
		where
			[mt_update_dt] >= @load_dt and
			not exists (
				select
					1
				from
					core.#anchor#_s_#src_name# as tgt
				where
					tgt.#anchor#_id = src.[id]
			)

		insert into core.#anchor# (
			#anchor#_key,
			#anchor#_source_key,
			mt_insert_dt
		)
		select 
			#anchor#_key, 
			1 as #anchor#_source_key, 
			@mt_dt as mt_insert_dt 
		from 
			##anchor#_s_#src_name# as src

		insert into core.#anchor#_s_#src_name# (
			#anchor#_key,
			#anchor#_id,
			#anchor#_source,
			#anchor#_medium,
			#anchor#_campaign,
			mt_insert_dt
		)
		select
			#anchor#_key,
			#anchor#_id,
			#anchor#_source,
			#anchor#_medium,
			#anchor#_campaign,
			@mt_dt as mt_insert_dt
		from
			##anchor#_s_#src_name# as src

		commit tran
	end try
	begin catch
		rollback tran

		declare @errormessage nvarchar(4000);
		declare @errorseverity int;
		declare @errorstate int;

		select
			@errorseverity = error_severity(),
			@errorstate	   = error_state(),
			@errormessage  = error_message()

		raiserror (@errormessage,
		@errorseverity,
		@errorstate
		);
	end catch

end
	go
