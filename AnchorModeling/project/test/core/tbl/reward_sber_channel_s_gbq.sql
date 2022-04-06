
create table [core].[reward_sber_channel_s_gbq] (
	[reward_sber_channel_key] [bigint] not null,
	[reward_sber_channel_channel] [nvarchar](64) null,
	[mt_insert_dt] [datetime2](0) not null,
	constraint [pk_reward_sber_channel_s_gbq] primary key clustered
	(
	[reward_sber_channel_key]
	)
)
go

create nonclustered index [mt_insert_dt] on [core].[reward_sber_channel_s_gbq]
(
[mt_insert_dt]
)
go
