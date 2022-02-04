
create table [dbo].[utm_extended] (
	[utm_extended_key] [bigint] not null,
	[utm_extended_hash] nvarchar(64),
	[utm_extended_source] nvarchar(4000),
	[utm_extended_medium] nvarchar(4000),
	[utm_extended_campaign] nvarchar(4000),
	[utm_extended_content] nvarchar(4000),
	[utm_extended_term] nvarchar(4000),
	[utm_extended_traffic_type] nvarchar(4000),
	[utm_extended_traffic_category] nvarchar(4000),
	[utm_extended_traffic_subcategory] nvarchar(4000),
	[utm_extended_traffic_division] nvarchar(4000),
	[utm_extended_traffic_channel] nvarchar(4000),
	[utm_extended_traffic_is_paid] nvarchar(4000),
	[utm_extended_campaign_target_category_id] nvarchar(4000),
	[utm_extended_campaign_target_web_level_1] nvarchar(4000),
	[utm_extended_campaign_target_web_level_2] nvarchar(4000),
	[utm_extended_campaign_target_web_level_3] nvarchar(4000),
	[utm_extended_campaign_target_web_level_4] nvarchar(4000),
	[utm_extended_campaign_target_web_level_5] nvarchar(4000),
	[utm_extended_campaign_target_web_level_6] nvarchar(4000),
	[mt_insert_dt] [datetime2](0) not null,
	[mt_update_dt] [datetime2](0) not null,
	constraint [pk$utm_extended] primary key nonclustered
	(
	[utm_extended_key] asc
	) on [DATA_MART]
) on [DATA_MART]
go

create clustered columnstore index [ccix] on [dbo].[utm_extended] on [DATA_MART]
go