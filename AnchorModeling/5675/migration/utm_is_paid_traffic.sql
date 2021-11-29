
create table [core].[utm_is_paid_traffic] (
	[utm_key] [bigint] not null,
	[is_paid_traffic] [nvarchar](4000) null,
	[mt_insert_dt] [datetime2](0) not null,
	[mt_update_dt] [datetime2](0) not null,
	constraint [pk_utm_is_paid_traffic] primary key clustered
	(
	[utm_key]
	)
)
go

create nonclustered index [ix$mt_update_dt] on [core].[utm_is_paid_traffic]
(
[mt_insert_dt]
)
go
