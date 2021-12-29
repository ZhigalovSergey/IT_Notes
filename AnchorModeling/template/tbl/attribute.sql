
create table [core].[#anchor#_x_#attr#] (
	[#anchor#_key] [bigint] not null,
	[#anchor#_#attr#] [nvarchar](4000) null,
	[mt_insert_dt] [datetime2](0) not null,
	[mt_update_dt] [datetime2](0) not null,
	constraint [pk_#anchor#_#attr#] primary key clustered
	(
	[#anchor#_key]
	)
)
go

create nonclustered index [mt_update_dt] on [core].[#anchor#_x_#attr#]
(
[mt_insert_dt]
)
go
