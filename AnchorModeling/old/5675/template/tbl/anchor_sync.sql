
create procedure [core].[###_sync]
as begin
	set nocount on

	begin try
		begin tran

		declare @mt_dt datetime2(0) = sysdatetime()
		declare @load_dt date = dateadd(dd, -3, @mt_dt)

		drop table if exists #utm_gbq
		create table #utm_gbq (
			utm_key bigint not null,
			[source] [NVARCHAR](4000) null,
			[medium] [NVARCHAR](4000) null,
			[campaign] [NVARCHAR](4000) null,
			[hash] [NVARCHAR](64) not null
		)

		insert into #utm_gbq (
			utm_key,
			[source],
			[medium],
			[campaign],
			[hash]
		)
		select
			next value for core.utm_sequence as utm_key,
			[source],
			[medium],
			[campaign],
			[id]
		from
			[MDWH_RAW].[raw_gbq168810_g161014_dwh_output].[dimUtm] src
		where
			[mt_update_dt] >= @load_dt and
			not exists (
				select
					1
				from
					[core].[utm_gbq] as tgt
				where
					tgt.[hash] = src.[id]
			)

		insert into [core].[utm] (
			utm_key,
			utm_source_system_key,
			mt_insert_dt
		)
		select utm_key, 1 as utm_source_system_key, @mt_dt as mt_insert_dt from #utm_gbq as src

		insert into [core].[utm_gbq] (
			utm_key,
			[medium],
			[source],
			[campaign],
			[hash],
			mt_insert_dt
		)
		select
			utm_key,
			[medium],
			[source],
			[campaign],
			[hash],
			@mt_dt as mt_insert_dt
		from
			#utm_gbq as src

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
