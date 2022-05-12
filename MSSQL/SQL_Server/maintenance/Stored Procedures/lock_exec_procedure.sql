
/*
Процедура установки блокировки параллельного запуска процедур путем установки блокировки уровня приложения на уровне сессии
Если убить сессию то все ее блоировки снимаются автоматом.

Для контроля наличия установленных блокировок по процедуре используем запрос:
SELECT TOP (100) * FROM sys.dm_tran_locks
WHERE resource_type = 'APPLICATION' and resource_description like '%'+left('raw_oms._InfoRg771_sync',32)+'%'  

В случае попытки запуска с уже установленной блокировкой получаем ошибку такого вида(она также записывается в лог с тем же @exec_guid что и запуск процедуры):
Ошибка получения блокировки для процедуры  raw_gbq168810_g1548765586214_analytics_196794017.app_sessions_final_v2_sync (-1)


Пример использования:
1)в самом начале- после определения  параметров вставляем блок установки блокировки:
	--region Установка блокировки(в случае невозможности прекращается работа с ошибкой)
	declare @lock_result int=0;  
	exec @lock_result = maintenance.lock_exec_procedure @proc_id = @proc_id, @exec_guid = @exec_guid, @mode = 1, @lock_timeout = 0
	if @lock_result < 0  return;
	--endregion Установка блокировки

2)в самом конце ставим блок сняития блокировки
	--region Снятие блокировки (в случае невозможности прекращается работа с ошибкой)
	EXEC @lock_result = maintenance.lock_exec_procedure @proc_id = @proc_id, @exec_guid = @exec_guid, @mode = 0, @lock_timeout = 0
	--endregion Снятие блокировки


*/
create procedure [maintenance].[lock_exec_procedure](
	@proc_id bigint, --идентификатор процедуры(используется у нас во всех процедурах для логирования declare @proc_id bigint = @@procid)
	@exec_guid uniqueidentifier, --идентификатор запуска(используется у нас во всех процедурах для логирования declare @exec_guid uniqueidentifier = newid())
	@mode bit, --режим запуска : 1 -установка, 0 снятие
	@lock_timeout bigint = 0 --таймаут ожидания блокировки, мс
) as 
begin
	set nocount on
	declare @comment nvarchar(4000)
	declare @proc_name sysname = OBJECT_SCHEMA_NAME(@proc_id)+ N'.' + OBJECT_NAME(@proc_id);
	declare @lock_result int=0; 

	if @proc_name is null
	begin
		return 0;
	end

	begin try
		if @mode = 1
		begin
			exec @lock_result = sp_getapplock @Resource = @proc_name,           @LockMode = 'Exclusive', @LockTimeout = @lock_timeout,	@LockOwner ='Session';
			if @lock_result < 0
			begin
				raiserror (	'Ошибка получения блокировки для процедуры  %s (%d)',16	,1	,@proc_name, @lock_result	);
			end;
		end
		else begin
			exec @lock_result = sp_releaseapplock @Resource = @proc_name,	@LockOwner ='Session';
			if @lock_result < 0
			begin
				raiserror (	'Ошибка снятия блокировки для процедуры  %s (%d)',16	,1	,@proc_name, @lock_result	);
			end;

		end;
	end try
	begin catch

		declare @errormessage nvarchar(4000);
		declare @errorseverity int;
		declare @errorstate int;

		select
			@errorseverity = error_severity(),
			@errorstate	   = error_state(),
			@errormessage  = error_message()

		execute maintenance.InsertExecution
			@Step = @errormessage,
			@ExecGUID = @exec_guid,
			@ProcID = @proc_id,
			@Rows = 0;

		raiserror (@errormessage,
		@errorseverity,
		@errorstate
		);
	end catch

	return @lock_result;
end;

