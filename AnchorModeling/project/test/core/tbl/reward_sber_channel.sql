
create table [core].[reward_sber_channel] (
	[reward_sber_channel_key] [int] not null,
	[reward_sber_channel_source_key] [tinyint] not null,
	[mt_insert_dt] [datetime2](0) not null,
	constraint [pk_reward_sber_channel] primary key clustered
	(
	[reward_sber_channel_key]
	) on [core]
) on [core]
go

create nonclustered index [mt_insert_dt] on [core].[reward_sber_channel]
(
[mt_insert_dt]
)
go