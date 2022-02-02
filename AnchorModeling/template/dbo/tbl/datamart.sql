
create table [dbo].[#anchor#] (
	[#anchor#_key] [bigint] not null,
#attrs#
	[mt_insert_dt] [datetime2](0) not null,
	[mt_update_dt] [datetime2](0) not null,
	constraint [pk$#anchor#] primary key nonclustered
	(
	[#anchor#_key] asc
	) on [DATA_MART]
) on [DATA_MART]
go

