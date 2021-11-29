
create table [core].[utm_###] (
	[utm_key] [bigint] not null,
	[###] [nvarchar](4000) null,
	[mt_insert_dt] [datetime2](0) not null,
	[mt_update_dt] [datetime2](0) not null,
	constraint [pk_utm_###] primary key clustered
	(
	[utm_key]
	)
)
go

create nonclustered index [ix$mt_update_dt] on [core].[utm_###]
(
[mt_insert_dt]
)
go
