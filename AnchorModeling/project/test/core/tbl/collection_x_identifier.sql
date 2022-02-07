
create table [core].[collection_x_identifier] (
	[collection_key] bigint not null,
	[collection_identifier] nvarchar(255) not null,
	[mt_insert_dt] datetime2(0) not null,
	[mt_update_dt] datetime2(0) not null,
	constraint [pk_collection_identifier] primary key clustered
	(
	[collection_key]
	)
)
go

create nonclustered index [mt_update_dt] on [core].[collection_x_identifier]
(
[mt_update_dt]
)
go
