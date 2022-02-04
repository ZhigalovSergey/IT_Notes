
create table [core].[utm_extended] (
	[utm_extended_key] [bigint] not null,
	[utm_extended_source_key] [tinyint] not null,
	[mt_insert_dt] [datetime2](0) not null,
	constraint [pk_utm_extended] primary key clustered
	(
	[utm_extended_key]
	) on [core]
) on [core]
go

create nonclustered index [mt_insert_dt] on [core].[utm_extended]
(
[mt_insert_dt]
)
go