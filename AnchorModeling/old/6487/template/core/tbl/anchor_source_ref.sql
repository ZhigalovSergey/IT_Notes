
create table [core].[#anchor#_source_ref] (
	[#anchor#_source_key] [tinyint] not null,
	[#anchor#_source_name] [nvarchar](255) not null,
	constraint [pk_#anchor#_source_ref] primary key clustered
	(
	[#anchor#_source_key] asc
	) on [core]
) on [core]
go