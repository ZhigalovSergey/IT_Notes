```sql
-- Пользователь из под которого работает SQL Agent
execute as login = 'CORP\msamir-sdb-005$'

declare @cnt_parall_process int = 2, @iter int = 0

while @iter < @cnt_parall_process
begin 
	-- execute as login = 'CORP\msamir-sdb-005$'

	declare @execution_id bigint,
			@msg nvarchar(max)

	exec SSISDB.catalog.create_execution
		@package_name = N'load_parallel.dtsx',
		@folder_name = N'Load',
		@project_name = N'Load_Big_Data',
		@use32bitruntime = false,
		@reference_id = null,
		@execution_id = @execution_id output

	exec [catalog].[set_execution_property_override_value]
		@execution_id,
		@property_path = N'\Package.Variables[User::loop]',
		@property_value = 26,
		@sensitive = 0

	exec SSISDB.catalog.start_execution
		@execution_id

	select @execution_id

	set @iter = @iter + 1
end


-- 1560767

/*

select *
from [SSISDB].[catalog].[executions]
where package_name = 'load_parallel.dtsx' and end_time IS NULL

*/

/*

declare @execution_id bigint = 1560396

select status from SSISDB.catalog.executions where execution_id = @execution_id


select [message]
from SSISDB.catalog.operation_messages
where
	-- message_type = 120 and
	operation_id = @execution_id

*/

/*

declare @execution_id bigint = 1560397
exec [SSISDB].[catalog].[stop_operation] @operation_id

*/
```