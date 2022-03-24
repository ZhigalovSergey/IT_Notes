
create procedure maintenance_idx.maintenance_columnstore_table_job as 
begin
	set nocount on
	set dateformat ymd
	set datefirst 1

	declare @waitfor_dt datetime2(0),--время запуска
			@expiration_dt datetime2(0), --запустить операцию не позже - имеет выше приоритет над @expiration_minutes
			@dw tinyint =  datepart(dw, getdate())-- день недели
	
	
	--постановка в очередь на будний день
	if @dw < 6
	begin
		select
			@waitfor_dt	   = cast(cast(getdate() as date) as datetime) + 21 / 24.,--время запуска
			@expiration_dt = cast(cast(getdate() as date) as datetime) + 1. --запустить операцию не позже - имеет выше приоритет над @expiration_minutes


		exec maintenance_idx.maintenance_columnstore_table
			@dbase_name  = 'MDWH',--база
			@schema_name  = null,--схема, можно маску с %
			@tab_name  = null,--имя таблицы , можно маску с %
			@fragmentation_threshold = 30,--порог фрагментации для перестроения
			@mode = 1,--режим: 0-непосредственное выполнение, 1- постановка в очередь
			@priority_lvl = 100,--приоритет
			@lock_timeout = 300000,--лимит блокировки
			@time_limit = 30,--лимит на выполнение в минутах
			@waitfor_dt = @waitfor_dt,--время запуска
			--@expiration_minutes = 30,--запустить операцию не позже 
			@expiration_dt = @expiration_dt,--запустить операцию не позже - имеет выше приоритет над @expiration_minutes
			@obj_identifier = null --более подробная идентификация объекта,, например при ослуживании таблицы помимо имени указываем индекс и партицию

	end

	else if @dw = 6 --постановка в очередь на субботу
	begin

		select
			@waitfor_dt	   = cast(cast(getdate() as date) as datetime) + 10 / 24.,--время запуска
			@expiration_dt = cast(cast(getdate() as date) as datetime) + 1. --запустить операцию не позже - имеет выше приоритет над @expiration_minutes


		exec maintenance_idx.maintenance_columnstore_table
			@dbase_name  = 'MDWH',--база
			@schema_name  = null,--схема, можно маску с %
			@tab_name  = null,--имя таблицы , можно маску с %
			@fragmentation_threshold = 10,--порог фрагментации для перестроения
			@mode = 1,--режим: 0-непосредственное выполнение, 1- постановка в очередь
			@priority_lvl = 100,--приоритет
			@lock_timeout = 1200000,--лимит блокировки
			@time_limit = 45,--лимит на выполнение в минутах
			@waitfor_dt = @waitfor_dt,--время запуска
			--@expiration_minutes = 30,--запустить операцию не позже 
			@expiration_dt = @expiration_dt,--запустить операцию не позже - имеет выше приоритет над @expiration_minutes
			@obj_identifier = null --более подробная идентификация объекта,, например при ослуживании таблицы помимо имени указываем индекс и партицию

		exec maintenance_idx.maintenance_columnstore_table
			@dbase_name  = 'MDWH_RAW',--база
			@schema_name  = null,--схема, можно маску с %
			@tab_name  = null,--имя таблицы , можно маску с %
			@fragmentation_threshold = 10,--порог фрагментации для перестроения
			@mode = 1,--режим: 0-непосредственное выполнение, 1- постановка в очередь
			@priority_lvl = 100,--приоритет
			@lock_timeout = 1200000,--лимит блокировки
			@time_limit = 45,--лимит на выполнение в минутах
			@waitfor_dt = @waitfor_dt,--время запуска
			--@expiration_minutes = 30,--запустить операцию не позже 
			@expiration_dt = @expiration_dt,--запустить операцию не позже - имеет выше приоритет над @expiration_minutes
			@obj_identifier = null --более подробная идентификация объекта,, например при ослуживании таблицы помимо имени указываем индекс и партицию

	end

end