
create table [raw_gbq168810_g161014_dwh_output].[dimUtmExtended] (
	[id] [NVARCHAR](64) not null,
	[source] [NVARCHAR](4000) null,
	[medium] [NVARCHAR](4000) null,
	[campaign] [NVARCHAR](4000) null,
	[content] [NVARCHAR](4000) null,
	[term] [NVARCHAR](4000) null,
	[TrafficType] [NVARCHAR](4000) null,
	[TrafficCategory] [NVARCHAR](4000) null,
	[TrafficSubcategory] [NVARCHAR](4000) null,
	[TrafficDivision] [NVARCHAR](4000) null,
	[TrafficChannel] [NVARCHAR](4000) null,
	[IsPaidTraffic] [NVARCHAR](4000) null,
	[CampaignTargetCategoryId] [NVARCHAR](4000) null,
	[CampaignTargetWebLevel1] [NVARCHAR](4000) null,
	[CampaignTargetWebLevel2] [NVARCHAR](4000) null,
	[CampaignTargetWebLevel3] [NVARCHAR](4000) null,
	[CampaignTargetWebLevel4] [NVARCHAR](4000) null,
	[CampaignTargetWebLevel5] [NVARCHAR](4000) null,
	[CampaignTargetWebLevel6] [NVARCHAR](4000) null,
	[update_dt] [bigint] null,
	[mt_insert_dt] [DATETIME] not null,
	[mt_update_dt] [DATETIME] not null,
	constraint [pk_dimUtmExtended] primary key nonclustered
	(
	[id] asc
	) on [USERDATA]
) on [USERDATA]
go

create clustered columnstore index [ccix_dimUtmExtended] on [raw_gbq168810_g161014_dwh_output].[dimUtmExtended] on [USERDATA]
go

