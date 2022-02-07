
create table [core].[utm_extended_x_campaign_target_web_level_4] (
	[utm_extended_key] [bigint] not null,
	[utm_extended_campaign_target_web_level_4] [nvarchar](4000) null,
	[mt_insert_dt] [datetime2](0) not null,
	[mt_update_dt] [datetime2](0) not null,
	constraint [pk_utm_extended_campaign_target_web_level_4] primary key clustered
	(
	[utm_extended_key]
	)
)
go

create nonclustered index [mt_update_dt] on [core].[utm_extended_x_campaign_target_web_level_4]
(
[mt_update_dt]
)
go
