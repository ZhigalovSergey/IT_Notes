
-- drop table dbo.utm

create table dbo.utm (
	[utm_key] [bigint] not null,
	[medium] [nvarchar](4000) null,
	[source] [nvarchar](4000) null,
	[campaign] [nvarchar](4000) null,
	[hash] [nvarchar](64) null,
	[traffic_type] [nvarchar](4000) null,
	[traffic_category] [nvarchar](4000) null,
	[traffic_subcategory] [nvarchar](4000) null,
	[traffic_division] [nvarchar](4000) null,
	[traffic_channel] [nvarchar](4000) null,
	[is_paid_traffic] [nvarchar](4000) null,
	[campaign_target_category_id] [nvarchar](4000) null,
	[campaign_target_web_level1] [nvarchar](4000) null,
	[campaign_target_web_level2] [nvarchar](4000) null,
	[campaign_target_web_level3] [nvarchar](4000) null,
	[campaign_target_web_level4] [nvarchar](4000) null,
	[campaign_target_web_level5] [nvarchar](4000) null,
	[campaign_target_web_level6] [nvarchar](4000) null,
	[mt_insert_dt] [datetime2](0) NOT NULL,
	[mt_update_dt] [datetime2](0) NOT NULL
	constraint [pk$utm] primary key nonclustered
	(
	[utm_key]
	) on [data_mart]
) on [data_mart]
go

create clustered columnstore index [ccix] on [dbo].[utm] on [data_mart]
go
