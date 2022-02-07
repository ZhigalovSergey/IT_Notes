
create table [core].[collection_x_is_active] (
	[collection_key] bigint not null,
	[collection_is_active] bit not null,
	[mt_insert_dt] datetime2(0) not null,
	[mt_update_dt] datetime2(0) not null,
	constraint [pk_collection_is_active] primary key clustered
	(
	[collection_key]
	)
)
go

create nonclustered index [mt_update_dt] on [core].[collection_x_is_active]
(
[mt_update_dt]
)
go
