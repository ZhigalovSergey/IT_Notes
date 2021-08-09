## [Заметки по Extended Events](./ExtendedEvents_note.md)  

### Мониторинг таблицы. Автоматизация.  

#### Описание проблемы  

Часто таблица используется в других объектах: представления, процедуры, функции. И для отслеживания вызовов нужно перечислить все эти объекты при создании Extended Events. Задача автоматизировать этот процесс.

#### Варианты решения  

Напишем запрос для автоматизированного создания скрипта для Extended Events.

#### Реализация  

Возьмем за образец 

```sql
CREATE EVENT SESSION [item_snapshot] ON SERVER 
ADD EVENT sqlserver.sql_statement_completed(SET collect_statement=(1)
    ACTION(sqlserver.client_app_name,sqlserver.client_hostname,sqlserver.server_instance_name,sqlserver.server_principal_name,sqlserver.username)
    WHERE (
			[sqlserver].[like_i_sql_unicode_string]([sqlserver].[sql_text],N'%get_articles_drop%') OR 
			[sqlserver].[like_i_sql_unicode_string]([sqlserver].[sql_text],N'%vw_fact_product_statistic%') OR 
			[sqlserver].[like_i_sql_unicode_string]([sqlserver].[sql_text],N'%item_snapshot_sync_drop%') OR 
			[sqlserver].[like_i_sql_unicode_string]([sqlserver].[sql_text],N'%FactProductStatistic%') OR 
			[sqlserver].[like_i_sql_unicode_string]([sqlserver].[sql_text],N'%tvf_AssortmentCube%') OR 
			[sqlserver].[like_i_sql_unicode_string]([sqlserver].[sql_text],N'%vw_item_snapshot%') OR 
			[sqlserver].[like_i_sql_unicode_string]([sqlserver].[sql_text],N'%items_sync%') OR 
			[sqlserver].[like_i_sql_unicode_string]([sqlserver].[sql_text],N'%update_fact_assortment_statistic%') OR 
			[sqlserver].[like_i_sql_unicode_string]([sqlserver].[sql_text],N'%item_snapshot_sync%')
		)
	)
ADD TARGET package0.event_file(SET filename=N'F:\TRC\item_snapshot.xel',max_file_size=(10))
WITH (MAX_MEMORY=4096 KB,EVENT_RETENTION_MODE=NO_EVENT_LOSS,MAX_DISPATCH_LATENCY=3 SECONDS,MAX_EVENT_SIZE=0 KB,MEMORY_PARTITION_MODE=NONE,TRACK_CAUSALITY=OFF,STARTUP_STATE=OFF)
GO
```

Для перечисления зависимых объектов таблицы **item_snapshot** будем использовать запрос

```sql
select
	string_agg('[sqlserver].[like_i_sql_unicode_string]([sqlserver].[sql_text],N''%' + o.name + '%'')', ' OR ')
from
	sys.objects o
	left join sys.sql_modules sm on
		o.object_id = sm.object_id
where
	sm.definition like '%item_snapshot%'
```

Если на вашем сервере не установлена функция агрегирования строк, то можно использовать следующий запрос

```sql
select
	'[sqlserver].[like_i_sql_unicode_string]([sqlserver].[sql_text],N''%' + o.name + '%'') OR '
from
	sys.objects o
	left join sys.sql_modules sm on
		o.object_id = sm.object_id
where
	sm.definition like '%item_snapshot%'
for xml path (''), type
```

Так как наше хранилище распределено по нескольким БД на сервере, то нужно пробежаться по всем БД

```sql
declare @db_name as nvarchar(255),
		@sql as nvarchar(4000),
		@par_object as nvarchar(255) = N'items_collections_places_snapshot'
declare list_db cursor
for
 select
	 name as DBName
 from
	 sys.databases
 where
	 name like '%MDWH%'
 order by
	 name;

drop table if exists #res
create table #res (
	DB_nm nvarchar(255),
	schema_nm nvarchar(255),
	object_nm nvarchar(255),
	type_desc nvarchar(255)
)

open list_db

fetch next from list_db into @db_name

while @@fetch_status = 0
begin
	set @sql = N'use ' + @db_name + ';
		insert into #res (DB_nm, schema_nm, object_nm, type_desc) 
		select ''' + @db_name + ''' as DB, sch.name, o.name, type_desc 
		from
			sys.objects o
			left join sys.schemas sch on o.schema_id = sch.schema_id
			left join sys.sql_modules sm on
				o.object_id = sm.object_id
		where
			lower(sm.definition) like lower(''%' + @par_object + '%'')'
	exec (@sql)

	set @sql = N'use ' + @db_name + ';
		insert into #res (DB_nm, schema_nm, object_nm, type_desc) 
		select ''' + @db_name + ''' as DB, sch.name, o.name, type_desc 
		from
			sys.objects o
			left join sys.schemas sch on o.schema_id = sch.schema_id
		where
			lower(o.name) like lower(''' + @par_object + ''')'
	exec (@sql)

	fetch next from list_db into @db_name
end
close list_db
deallocate list_db

-- select * from #res as r

select string_agg('[sqlserver].[like_i_sql_unicode_string]([sqlserver].[sql_text],N''%' + object_nm + '%'')', ' OR ')
from 
(select object_nm from #res group by object_nm) t
```

Далее остается собрать параметризированный динамический SQL запрос и оформить в виде процедуры с параметром создание Extended Event.

#### Полезные ссылки:  

- [Database alias in Microsoft SQL Server](https://www.baud.cz/blog/database-alias-in-microsoft-sql-server)  
- [mssqltips.com - SQL Server Extended Events Tutorial](https://www.mssqltips.com/sqlservertutorial/9194/sql-server-extended-events-tutorial/)  
- [Capture Executions of Stored Procedures in SQL Server](https://www.mssqltips.com/sqlservertip/6550/capture-executions-of-stored-procedures-in-sql-server/)  

