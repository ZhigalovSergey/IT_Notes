create procedure [maintenance].[InsertExecution]
	@ExecGUID uniqueidentifier
  , @ProcID bigint
  , @Step nvarchar(1000)
  , @Rows int = null
as
begin
	set nocount on;
	set datefirst 1;
	set dateformat ymd;
	begin try

			declare @schema SYSNAME = OBJECT_SCHEMA_NAME(@ProcID)
					,@proc_name SYSNAME = OBJECT_NAME(@ProcID);
			declare @timestamp CHAR(8) = CAST(CONVERT(TIME(0), GETDATE()) AS VARCHAR);
			declare @separ CHAR(2) = '  ';
			declare @is_write_in_log_acceptable BIT = CASE 
					WHEN @schema IS NOT NULL
						AND @proc_name IS NOT NULL
						THEN 1
					ELSE 0
					END;

			declare @proc_simplified CHAR(20) = CASE 
					WHEN @proc_name IS NULL -- случай запуска кода руками 
						THEN 'БЕЗ ЗАПИСИ В ЛОГ    '
					WHEN LEN(@proc_name) <= 20
						THEN LEFT(@proc_name + '                    ', 20)
					ELSE LEFT(@proc_name, 12) + '..' + RIGHT(@proc_name, 6)
					END;

			declare @message_details VARCHAR(512) = ISNULL(' (' + CAST(	@Rows AS VARCHAR) + ' строк)'  , '');
			declare @print_text VARCHAR(MAX) = @timestamp + @separ + @proc_simplified + 
				@separ +   @Step + 
				@message_details;

			set @print_text = REPLACE(@print_text, '%', 'проц.');

			if @is_write_in_log_acceptable = 1
			begin
				insert into maintenance.ExecutionLog
				( ExecutionGUID
				, ProcedureName
				, StepName
				, EventDate
				, RowsAffected
				)
				values
				( @ExecGUID
				, @schema + N'.' + @proc_name
				, @Step
				, sysdatetime()
				, @Rows
				);
			end;


			raiserror (
					@print_text
					,- 1
					,- 1
					)
			with nowait;


	end try

	begin catch
		declare @error_state int = error_state();
		declare @error_severity int = iif(error_severity() > 16, 16, error_severity());
		declare @error_message varchar(max) = isnull('ошибка #'
		    + nullif(cast(nullif(error_number(), 50000) as varchar), '') + ': ', '')
		    + isnull(nullif(error_message(), ''), 'неизвестная ошибка') +
		    + isnull(char(10) + '        ' + 'at    ' + isnull(error_procedure()
		    + '    :    ', '') + cast(error_line() as varchar(10)), '');

		raiserror (
				@error_message
				,@error_severity
				,@error_state
				);
	end catch
end
GO


