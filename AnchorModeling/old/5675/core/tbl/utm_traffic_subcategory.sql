
create table [core].[utm_traffic_subcategory] (
	[utm_key] [bigint] not null,
	[traffic_subcategory] [nvarchar](4000) null,
	[mt_insert_dt] [datetime2](0) not null,
	[mt_update_dt] [datetime2](0) not null,
	constraint [pk_utm_traffic_subcategory] primary key clustered
	(
	[utm_key]
	)
)
go

create nonclustered index [ix$mt_update_dt] on [core].[utm_traffic_subcategory]
(
[mt_insert_dt]
)
go
