create table [maintenance].[error_type](
	error_type_id int identity(1,1),
	error_type_desc [nvarchar](255)
)
go


--insert into [maintenance].[error_type](error_type_desc) values (N'insert duplicate key')
--insert into [maintenance].[error_type](error_type_desc) values (N'key is null')
--select * from [maintenance].[error_type]