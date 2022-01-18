
create table [core].[utm_extended_x_traffic_channel] (
	[utm_extended_key] [bigint] not null,
	[utm_extended_traffic_channel] [nvarchar](4000) null,
	[mt_insert_dt] [datetime2](0) not null,
	[mt_update_dt] [datetime2](0) not null,
	constraint [pk_utm_extended_traffic_channel] primary key clustered
	(
	[utm_extended_key]
	)
)
go

create nonclustered index [mt_update_dt] on [core].[utm_extended_x_traffic_channel]
(
[mt_update_dt]
)
go
