
create table [core].[collection_x_parent_id] (
	[collection_key] bigint not null,
	[collection_parent_id] bigint not null,
	[mt_insert_dt] datetime2(0) not null,
	[mt_update_dt] datetime2(0) not null,
	constraint [pk_collection_parent_id] primary key clustered
	(
	[collection_key]
	)
)
go

create nonclustered index [mt_update_dt] on [core].[collection_x_parent_id]
(
[mt_update_dt]
)
go
