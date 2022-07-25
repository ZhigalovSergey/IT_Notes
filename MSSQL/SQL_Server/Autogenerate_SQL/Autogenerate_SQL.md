### [Заметки по SQL Server](../SQLServer_note.md)  

## Генерация SQL кода по названию таблицы  

### Генерация Select

```sql
declare @temp_table_name_for_check nvarchar(max) = '#tt'
declare @sql nvarchar(max) = ''
set @sql = 'select '+char(13)
set @sql = @sql + stuff((select ','+c.name+char(13)
from tempdb.sys.columns c
join tempdb.sys.types tp with (nowait) on c.user_type_id = tp.user_type_id
where object_id = object_id(concat('tempdb.dbo.',@temp_table_name_for_check))
order by c.column_id
for xml path(''), type).value('.', 'nvarchar(max)'),1,1,' ')
set @sql = @sql + 'from '+@temp_table_name_for_check+char(13)

print @sql
```

### Генерация Create Table

Для написания скрипта можно взять основу в 

```sql

select ac.name column_name
	,ac.system_type_id
	,name_type = T.NAME
	,ac.max_length
	,ac.precision
	,ac.scale
	,ac.is_nullable
	,ac.is_identity
	,type_desc
	,s.name + '.' + o.name obj_name
	,type
	,o.create_date
	,o.modify_date
	,sql_null_count = case 
		when ac.is_nullable = 1
			then ',sum(iif([' + ac.name + '] is null, 1,0)) as [cn_null__' + ac.name + '] '
		else null
		end
	,sql_len_char = case 
		when T.NAME like '%char'
			then ',max(len([' + ac.name + '] )) as [length__' + ac.name + '] '
		else null
		end
from tempdb.sys.all_columns ac
join tempdb.sys.all_objects o
	on ac.object_id = o.[object_id]
left join tempdb.sys.schemas s
	on o.schema_id = s.schema_id
left join tempdb.sys.types t
	on t.system_type_id = ac.system_type_id
		and T.NAME not in ('sysname')
where 1 = 1
	and s.name + '.' + o.name = 'dbo.#tt'
	and type_desc like 'USER_TABLE'
order by type_desc
	,s.name
	,o.name
```

Или процедуру [prcGetTableCreateScript.sql](./prcGetTableCreateScript.sql.md)