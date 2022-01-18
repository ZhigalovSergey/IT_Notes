
create table [core].[utm_extended_s_gbq] (
	[utm_extended_key] [bigint] not null,
	[utm_extended_hash] [nvarchar](64) null,
	[utm_extended_source] [nvarchar](4000) null,
	[utm_extended_medium] [nvarchar](4000) null,
	[utm_extended_campaign] [nvarchar](4000) null,
	[utm_extended_content] [nvarchar](4000) null,
	[utm_extended_term] [nvarchar](4000) null,
	[mt_insert_dt] [datetime2](0) not null,
	constraint [pk_utm_extended_s_gbq] primary key clustered
	(
	[utm_extended_key]
	)
)
go

create nonclustered index [mt_insert_dt] on [core].[utm_extended_s_gbq]
(
[mt_insert_dt]
)
go
