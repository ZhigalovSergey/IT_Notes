```sql
-- ѕользователь из под которого работает SQL Agent
use SSISDB
go

execute as login = 'CORP\MSAsbi-sdb-002$' --'CORP\msamir-sdb-005$'

declare @cnt_parall_process int = 4, @iter int = 0

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
		@property_value = 30,
		@sensitive = 0

	exec [catalog].[set_execution_property_override_value]
		@execution_id,
		@property_path = N'\Package.Variables[User::start]',
		@property_value = 0,
		@sensitive = 0

	exec SSISDB.catalog.start_execution
		@execution_id

	select @execution_id

	set @iter = @iter + 1
end




-- 104

/*

select *
from [SSISDB].[catalog].[executions]
where package_name = 'load_parallel.dtsx' and end_time IS NULL

*/


/*

declare @execution_id bigint = 152

select status from SSISDB.catalog.executions where execution_id = @execution_id

--created (1), 
--running (2), 
--canceled (3), 
--failed (4), 
--pending (5), 
--ended unexpectedly (6), 
--succeeded (7), 
--stopping (8), 
--completed (9).

select [message]
from SSISDB.catalog.operation_messages
where
	 --message_type = 120 and
	operation_id = @execution_id

*/

/*

declare @execution_id bigint = 128
exec [SSISDB].[catalog].[stop_operation] @execution_id

*/
```