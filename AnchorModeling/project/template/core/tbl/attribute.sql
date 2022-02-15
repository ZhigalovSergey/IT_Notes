
create table [core].[#anchor#_x_#attr#] (
	[#anchor#_key] bigint not null,
	[#anchor#_#attr#] #type# not null,
	[mt_insert_dt] datetime2(0) not null,
	[mt_update_dt] datetime2(0) not null,
	constraint [pk_#anchor#_#attr#] primary key clustered
	(
	[#anchor#_key]
	) on [core]
) on [core]
go

create nonclustered index [mt_update_dt] include(#anchor#_#attr#) on [core].[#anchor#_x_#attr#]
(
[mt_update_dt]
) include (#anchor#_#attr#) on [core]
go
