
create table [core].[#anchor#] (
	[#anchor#_key] [bigint] not null,
	[#anchor#_source_key] [tinyint] not null,
	[mt_insert_dt] [datetime2](0) not null,
	constraint [pk_#anchor#] primary key clustered
	(
	[#anchor#_key]
	) on [core]
) on [core]
go

create nonclustered index [mt_update_dt] on [core].[#anchor#]
(
[mt_insert_dt]
)
go