
create procedure [core].[utm_extended_sync]
as begin
	set nocount on

	begin try
		begin tran

		declare @mt_dt datetime2(0) = sysdatetime()
		declare @load_dt date = dateadd(dd, -3, @mt_dt)

		drop table if exists #utm_extended_s_gbq
		create table #utm_extended_s_gbq (
			utm_extended_key bigint not null,
			utm_extended_id [NVARCHAR](64) not null,
			utm_extended_source [NVARCHAR](4000) null,
			utm_extended_medium [NVARCHAR](4000) null,
			utm_extended_campaign [NVARCHAR](4000) null,
			utm_extended_content [NVARCHAR](4000) null,
			utm_extended_term [NVARCHAR](4000) null
		)

		insert into #utm_extended_s_gbq (
			utm_extended_key,
			utm_extended_id,
			utm_extended_source,
			utm_extended_medium,
			utm_extended_campaign,
			utm_extended_content,
			utm_extended_term
		)
		select
			next value for core.utm_extended_sequence as utm_extended_key,
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
					core.utm_extended_s_gbq as tgt
				where
					tgt.utm_extended_hash = src.[id]
			)

		insert into core.utm_extended (
			utm_extended_key,
			utm_extended_source_key,
			mt_insert_dt
		)
		select 
			utm_extended_key, 
			1 as utm_extended_source_key, 
			@mt_dt as mt_insert_dt 
		from 
			#utm_extended_s_gbq as src

		insert into core.utm_extended_s_gbq (
			utm_extended_key,
			utm_extended_hash,
			utm_extended_source,
			utm_extended_medium,
			utm_extended_campaign,
			utm_extended_content,
			utm_extended_term,
			mt_insert_dt
		)
		select
			utm_extended_key,
			utm_extended_id,
			utm_extended_source,
			utm_extended_medium,
			utm_extended_campaign,
			utm_extended_content,
			utm_extended_term,
			@mt_dt as mt_insert_dt
		from
			#utm_extended_s_gbq as src

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
