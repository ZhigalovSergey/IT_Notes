### [Заметки по SQL Server](../SQLServer_note.md)  

## Поиск зависимостей между объектами Хранилища данных  

### Описание проблемы  

Удобно иметь наглядную картину того, как формируется анализируемая таблица DWH. Если вся логика трансформаций описана в процедурах и инструменты ETL только из запускают, то можно графически изобразить как формируется таблица. Возьмем [Atlas](https://atlas.apache.org/#/) в качестве инструмента для отрисовки зависимостей.  Тогда остается решить задачу нахождения зависимостей между объектами и определить направление потока данных (какие таблицы использует процедура и для формирования какой таблицы).

### Варианты решения  

- Создадим таблицу зависимостей на основе системных таблиц sys.sql_expression_dependencies и sys.objects. На основе этих данных можно построить неориентированный граф.
- Для определения направления, используем регулярные выражения для анализа текста процедур. В качестве шаблона используем команды **insert into** и **merge**.  
- Также можно добавить информацию о том в каких ETL пакетах запускаются процедуры. Для этого также могут помочь регулярные выражения.

### Реализация  

Запрос для таблицы зависимостей

```sql
select
	@@servername										   as server_name,
	db_name()											   as dbase_name,
	object_schema_name(sed.referencing_id)				   as schm_name,
	object_name(sed.referencing_id)						   as obj_name,
	o.type_desc											   as obj_type,
	col_name(sed.referencing_id, sed.referencing_minor_id) as column_name,
	sed.referencing_class_desc							   as ref_class,
	coalesce(
	sed.referenced_server_name, @@servername
	)													   as ref_server_name,
	coalesce(
	sed.referenced_database_name, db_name()
	)													   as ref_dbase_name,
	case
		when sed.referenced_schema_name is null
			or sed.referenced_schema_name = ''''
			then object_schema_name(
			object_id(
			concat(
			coalesce(
			sed.referenced_database_name, db_name()
			), '..', sed.referenced_entity_name
			)
			), coalesce(
			db_id(sed.referenced_database_name), db_id()
			)
			)
		else sed.referenced_schema_name
	end													   as ref_schm_name,
	sed.referenced_entity_name							   as ref_obj_name,
	col_name(sed.referenced_id, sed.referenced_minor_id)   as ref_column_name,
	sed.is_caller_dependent								   as is_caller_dependent,
	sed.is_ambiguous									   as is_ambiguous
from
	sys.sql_expression_dependencies as sed
	join sys.objects as o on
		sed.referencing_id = o.object_id
order by
	server_name
	, dbase_name
	, schm_name
	, obj_name
```



### Полезные ссылки  
- [sp_MSforeachdb](https://www.mssqltips.com/sqlservertip/1414/run-same-command-on-all-sql-server-databases-without-cursors/)  

- [Making a more reliable and flexible sp_MSforeachdb](https://www.mssqltips.com/sqlservertip/2201/making-a-more-reliable-and-flexible-spmsforeachdb/)  

