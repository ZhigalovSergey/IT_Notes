### [Заметки по SQL Server](../SQLServer_note.md)  

## Поиск зависимостей между объектами Хранилища данных  

### Описание проблемы  

Удобно иметь наглядную картину того, как формируется анализируемая таблица DWH. Если вся логика трансформаций описана в процедурах и инструменты ETL только из запускают, то можно графически изобразить как формируется таблица. Возьмем [Atlas](https://atlas.apache.org/#/) в качестве инструмента для отрисовки зависимостей.  Тогда остается решить задачу нахождения зависимостей между объектами и определить направление потока данных (какие таблицы использует процедура и для формирования какой таблицы). 

Для работы с метаданными существуют следующие инструменты: https://datahubproject.io/, https://open-metadata.org/

### Варианты решения  

- Создадим таблицу зависимостей на основе системных таблиц **sys.sql_expression_dependencies** и **sys.objects**. На основе этих данных можно построить неориентированный граф.
- Для определения направления, используем регулярные выражения для анализа текста процедур. В качестве шаблона используем команды **insert into** и **merge**.  
- Также можно добавить информацию о том в каких ETL пакетах запускаются процедуры. Для этого также могут помочь регулярные выражения. Рассмотреть возможность извлечения XML-файла пакетов из БД **SSISDB** (Предварительно файлы [шифруются](https://ask.sqlservercentral.com/questions/100651/querying-ssisdb-text-search-in-the-packet-definiti.html#) и проще анализировать пакете в GITе) 
- Заполним таблицу связей для Atlas  

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

Определение процедуры можно получить из **sys.sql_modules** или через **object_definition**

```sql
select object_definition(object_id('core.transaction_v2_sync'))

select top 100 *
from sys.sql_modules
where object_id = object_id('core.transaction_v2_sync')
```

Для поиска по шаблону используем функцию на C#

```c#
[Microsoft.SqlServer.Server.SqlFunction(IsDeterministic = true)]
public static SqlBoolean IsMatch(String input, String pattern)
{
	if (String.IsNullOrEmpty(input) || String.IsNullOrEmpty(pattern))
	{
		return new SqlBoolean(0);
	}
	else
	{
		return Regex.IsMatch(input, pattern);
	}
}
```

После сборки создадим функцию 

```sql
use [MDWH]
go

create function [maintenance].[regex_is_match]
(
	@input [NVARCHAR](max),
	@pattern [NVARCHAR](max)
)
returns [BIT] with execute as caller
as external name [RegexAssembly].[UDF].[IsMatch]
go
```

Проверяем работу на примере core.transaction_v2_sync которая формирует данные для таблицы core.transaction_v2

```sql
select
	s.name sch,
	o.name nm
from
	sys.objects o
	inner join sys.schemas s on
		o.schema_id = s.schema_id
	inner join sys.sql_modules M on
		o.object_id = M.object_id
where
	o.object_id = object_id('core.transaction_v2_sync') and
	maintenance.regex_is_match(M.definition, N'(?i).*(merge\s+[[]?core[]]?.[[]?transaction_v2[]]?|insert\s+into\s+[[]?core[]]?.[[]?transaction_v2[]]?).*') = 1
```

Для заполнения таблицы связей в Atlas создадим функцию с параметром - название процедуры. 

### Исходный код скриптов  

- [Проект RegEx на C#](./SqlRegex.7z)

### Полезные ссылки  
- [sp_MSforeachdb](https://www.mssqltips.com/sqlservertip/1414/run-same-command-on-all-sql-server-databases-without-cursors/)  

- [Making a more reliable and flexible sp_MSforeachdb](https://www.mssqltips.com/sqlservertip/2201/making-a-more-reliable-and-flexible-spmsforeachdb/)  

- [LIKE (Transact-SQL)](https://docs.microsoft.com/ru-ru/sql/t-sql/language-elements/like-transact-sql?view=sql-server-ver15)  

- [SQL Server Regex CLR Function](https://www.mssqltips.com/sqlservertip/6529/sql-server-regex-clr-function/)  

- [Regular Expressions Make Pattern Matching And Data Extraction Easier](https://docs.microsoft.com/en-us/archive/msdn-magazine/2007/february/sql-server-regular-expressions-for-efficient-sql-querying)  

- [sys.sql_expression_dependencies](https://docs.microsoft.com/ru-ru/sql/relational-databases/system-catalog-views/sys-sql-expression-dependencies-transact-sql?view=sql-server-ver15)  

