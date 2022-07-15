
create table [maintenance].[discarded_rows](
	[id] int identity(1,1) not null,
	[table_full_name] [nvarchar](1000) not null,
	[error_type_id] int not null,
	[error_json] [nvarchar](max) not null,
	[error_count_rows] int not null,
	[mt_insert_dt] [datetime2](0) not null
) on ps_month_dtm2_0(mt_insert_dt)
go

create clustered columnstore index [ccix] on [maintenance].[discarded_rows] on ps_month_dtm2_0(mt_insert_dt)
go
