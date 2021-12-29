
create table [core].[#anchor#_s_#src_name#] (
	[#anchor#_key] [bigint] not null,
	[#anchor#_#attr_bk_1#] [nvarchar](4000) null,
	[#anchor#_#attr_bk_2#] [nvarchar](4000) null,
	[#anchor#_#attr_bk_3#] [nvarchar](4000) null,
	[#anchor#_#attr_bk_4#] [nvarchar](4000) null,
	[mt_insert_dt] [datetime2](0) not null,
	constraint [pk_#anchor#_s_#src_name#] primary key clustered
	(
	[#anchor#_key]
	)
)
go

create nonclustered index [mt_update_dt] on [core].[#anchor#_s_#src_name#]
(
[mt_insert_dt]
)
go
