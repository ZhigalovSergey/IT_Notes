
create table [core].[collection_x_collection_type_id] (
	[collection_key] bigint not null,
	[collection_collection_type_id] int not null,
	[mt_insert_dt] datetime2(0) not null,
	[mt_update_dt] datetime2(0) not null,
	constraint [pk_collection_collection_type_id] primary key clustered
	(
	[collection_key]
	)
)
go

create nonclustered index [mt_update_dt] on [core].[collection_x_collection_type_id]
(
[mt_update_dt]
)
go
