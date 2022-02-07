
create table [dbo].[collection] (
	[collection_key] [bigint] not null,
	[collection_collection_id] bigint,
	[collection_collection_type_id] int,
	[collection_parent_id] bigint,
	[collection_identifier] nvarchar(255),
	[collection_parent_identifier] nvarchar(255),
	[collection_collection_name] nvarchar(255),
	[collection_collection_display_name] nvarchar(255),
	[collection_is_active] bit,
	[mt_insert_dt] [datetime2](0) not null,
	[mt_update_dt] [datetime2](0) not null,
	constraint [pk$collection] primary key nonclustered
	(
	[collection_key] asc
	) on [DATA_MART]
) on [DATA_MART]
go

