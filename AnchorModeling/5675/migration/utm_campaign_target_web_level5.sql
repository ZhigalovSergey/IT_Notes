
create table [core].[utm_campaign_target_web_level5] (
	[utm_key] [bigint] not null,
	[campaign_target_web_level5] [nvarchar](4000) null,
	[mt_insert_dt] [datetime2](0) not null,
	[mt_update_dt] [datetime2](0) not null,
	constraint [pk_utm_campaign_target_web_level5] primary key clustered
	(
	[utm_key]
	)
)
go

create nonclustered index [ix$mt_update_dt] on [core].[utm_campaign_target_web_level5]
(
[mt_insert_dt]
)
go
