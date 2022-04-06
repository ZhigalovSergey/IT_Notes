
create procedure [core].[reward_sber_channel_sync]
as begin
	set nocount on

	begin try
		begin tran

		declare @mt_dt datetime2(0) = sysdatetime()
		declare @load_dt date = dateadd(dd, -3, @mt_dt)

		drop table if exists #reward_sber_channel_s_gbq
		create table #reward_sber_channel_s_gbq (
			reward_sber_channel_key bigint not null,
			reward_sber_channel_id [NVARCHAR](64) not null,
			reward_sber_channel_source [NVARCHAR](4000) null,
			reward_sber_channel_medium [NVARCHAR](4000) null,
			reward_sber_channel_campaign [NVARCHAR](4000) null,
			reward_sber_channel_content [NVARCHAR](4000) null,
			reward_sber_channel_term [NVARCHAR](4000) null
		)

		insert into #reward_sber_channel_s_gbq (
			reward_sber_channel_key,
			reward_sber_channel_id,
			reward_sber_channel_source,
			reward_sber_channel_medium,
			reward_sber_channel_campaign,
			reward_sber_channel_content,
			reward_sber_channel_term
		)
		select
			next value for core.reward_sber_channel_sequence as reward_sber_channel_key,
			[id],
			[source],
			[medium],
			[campaign],
			[content],
			[term]
		from
			[MDWH_RAW].[raw_gbq168810_g161014_dwh_output].[dimUtmExtended] src
		where
			[mt_update_dt] >= @load_dt and
			not exists (
				select
					1
				from
					core.reward_sber_channel_s_gbq as tgt
				where
					tgt.reward_sber_channel_hash = src.[id]
			)

		insert into core.reward_sber_channel (
			reward_sber_channel_key,
			reward_sber_channel_source_key,
			mt_insert_dt
		)
		select 
			reward_sber_channel_key, 
			1 as reward_sber_channel_source_key, 
			@mt_dt as mt_insert_dt 
		from 
			#reward_sber_channel_s_gbq as src

		insert into core.reward_sber_channel_s_gbq (
			reward_sber_channel_key,
			reward_sber_channel_hash,
			reward_sber_channel_source,
			reward_sber_channel_medium,
			reward_sber_channel_campaign,
			reward_sber_channel_content,
			reward_sber_channel_term,
			mt_insert_dt
		)
		select
			reward_sber_channel_key,
			reward_sber_channel_id,
			reward_sber_channel_source,
			reward_sber_channel_medium,
			reward_sber_channel_campaign,
			reward_sber_channel_content,
			reward_sber_channel_term,
			@mt_dt as mt_insert_dt
		from
			#reward_sber_channel_s_gbq as src

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
