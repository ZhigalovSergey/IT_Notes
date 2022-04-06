
create table [core].[reward_sber_channel_source_ref] (
	[reward_sber_channel_source_key] [tinyint] not null,
	[reward_sber_channel_source_name] [nvarchar](255) not null,
	constraint [pk_reward_sber_channel_source_ref] primary key clustered
	(
	[reward_sber_channel_source_key] asc
	) on [core]
) on [core]
go