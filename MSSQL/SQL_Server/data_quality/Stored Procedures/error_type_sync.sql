create procedure [maintenance].[error_type_sync] @error_type_desc nvarchar(255), @error_type_id int output 
as begin

	if (not exists (select 1 from [maintenance].[error_type] where error_type_desc = @error_type_desc)) 
		insert into [maintenance].[error_type] (error_type_desc) values (@error_type_desc)

	set @error_type_id = (select error_type_id from [maintenance].[error_type] where error_type_desc = @error_type_desc)
end
go