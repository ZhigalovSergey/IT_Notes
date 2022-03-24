
create table [maintenance].[executionlog](
	[executionlinekey] [int] identity(1,1) not null,
	[executionguid] [uniqueidentifier] not null,
	[procedurename] [nvarchar](250) null,
	[stepname] [nvarchar](250) null,
	[eventdate] [datetime2](3) null,
	[rowsaffected] [int] null
)

