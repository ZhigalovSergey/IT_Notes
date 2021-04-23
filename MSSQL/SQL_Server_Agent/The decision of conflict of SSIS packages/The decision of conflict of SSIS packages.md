# Описание проблемы

*[Синтаксис MarkDown](https://www.markdownguide.org/basic-syntax/)*  
[Заметки по SQL Server Agent](../SQLAgent_note.md)  

В процессе прогрузки данных в DWH некоторые Job'ы запускают одинаковые SSIS пакеты. Если по времен эти запуски пересекаются, то возникают взаимные блокировки на сервере, конфликты и как следствие один из Job'ов падает по ошибке.

## Варианты Решения

- Выделить критичный для выполнения **Job A** и выполнить в нем первым шагом (перед запуском пакета SSIS) отключение другого Job'а (**Job B**). После выполнения, последним шагом (после отработки пакета SSIS) включить **Job B**. Минус этого решения в том, что в процессе работы пакета SSIS **Job A** может упасть и **Job B** останется выключенным. Поэтому шаг отработки пакета SSIS нужно настроить так, чтобы даже при завершении с ошибкой всегда запускался шаг включить **Job B**.

- Добавить в пакет SSIS проверку статуса выполнения пакета, например на основе таблицы **SSISDB.catalog.executions**. Это позволит не отключать **Job B**, а только остановить для выполнения более приоритетного **Job A**.
- Также можно добавить шаг, который проверяет статус **Job A** и генерирует ошибку, если джоб в данный момент работает. Но этот вариант хуже, чем доработка пакета SSIS, т.к.  **Job B** может быть не один, и дополнительный шаг нужно добавлять во все подобные Job'ы

## Подходы к Реализации

### Через управление Job'ми

Для отключения Job'а будем использовать хранимую процедуру [msdb.dbo.sp_update_job](https://docs.microsoft.com/ru-ru/sql/relational-databases/system-stored-procedures/sp-update-job-transact-sql?view=sql-server-ver15)

```sql
exec msdb.dbo.sp_update_job
	@job_id = N'f70fb8f6-bf56-4898-aab4-6754b1c0b450',
	@enabled = 0
go
```

**Примечание**. Рекомендую использовать именно параметр job_id, так как имя Job'а может меняться.

Для остановки Job'а будем использовать хранимую процедуру [msdb.dbo.sp_stop_job](https://docs.microsoft.com/ru-ru/sql/relational-databases/system-stored-procedures/sp-stop-job-transact-sql?view=sql-server-ver15)

```sql
exec msdb.dbo.sp_stop_job
	@job_id = N'f70fb8f6-bf56-4898-aab4-6754b1c0b450'
go
```

Перед остановкой Job'а нужно проверить работает ли он в данный момент. Иначе msdb.dbo.sp_stop_job вернёт ошибку и наш **Job A** упадёт и не будет выполнен!!!

Проверить текущий статус **Job B** можно с помощью [запросы из документации](https://docs.microsoft.com/ru-ru/sql/relational-databases/system-tables/dbo-sysjobactivity-transact-sql?view=sql-server-ver15) или хранимой процедуры [msdb.dbo.sp_help_job](https://docs.microsoft.com/ru-ru/sql/relational-databases/system-stored-procedures/sp-help-job-transact-sql?view=sql-server-ver15). Хранимая процедура информативна, в частности есть поле [current_execution_status](./current_execution_status.md) (Текущее состояние выполнения). Но чтобы использовать результат процедуры в условном операторе IF..ELSE нужно поместить его например во временную таблицу, что усложняет решение. Поэтому будем использовать запрос.

```sql
if exists (
			SELECT sj.Name, sj.job_id
			FROM msdb.dbo.sysjobs sj
			JOIN msdb.dbo.sysjobactivity sja
			ON sj.job_id = sja.job_id
			WHERE session_id = (
				SELECT MAX(session_id) FROM msdb.dbo.sysjobactivity)	-- make sure this is the most recent run
			 and sj.job_id = N'4ED2BA29-42B0-43C9-BD6C-9FE297753B60'
			 and sja.start_execution_date IS NOT NULL	-- job is currently running
			 AND sja.stop_execution_date IS NULL		-- job hasn't stopped running
	)
	exec msdb.dbo.sp_stop_job
			@job_id = N'4ED2BA29-42B0-43C9-BD6C-9FE297753B60'
go
```

### Через управление пакетами SSIS

Если пойти через анализ статуса пакета, то для остановки выполнения пакета SSIS можно использовать процедуру [SSISDB.catalog.stop_operation](https://docs.microsoft.com/ru-ru/sql/integration-services/system-stored-procedures/catalog-stop-operation-ssisdb-database?view=sql-server-ver15). Для поиска аргумента используем вьюху [SSISDB.catalog.executions](https://docs.microsoft.com/ru-ru/sql/integration-services/system-views/catalog-executions-ssisdb-database?view=sql-server-ver15). Этот подход лучше, т.к. не нужно искать все Job'ы, которые запустили данный пакет.

```sql
declare @operation_id bigint, @cnt int = 1

while @cnt > 0
begin
	select top (1) @operation_id = execution_id
	from [SSISDB].[catalog].[executions]
	where 
	package_name = 'core_web.dtsx' and end_time IS NULL

	set @cnt =  @@ROWCOUNT

	IF @operation_id is not null
		exec [SSISDB].[catalog].[stop_operation] @operation_id

	set @operation_id = NULL
end
```

### **Вывод**  

Таким образом лучшее решение задачи - это реализовать в пакете SSIS проверку статуса выполнения пакета. А в приоритетном  **Job A** перед запуском пакета остановить его с помощью процедуры  **SSISDB.catalog.stop_operation**.