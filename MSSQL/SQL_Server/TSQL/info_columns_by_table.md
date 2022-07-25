# Информация о колонках таблицы


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
from sys.all_columns ac
join sys.all_objects o
	on ac.object_id = o.[object_id]
left join sys.schemas s
	on o.schema_id = s.schema_id
left join sys.types t
	on t.system_type_id = ac.system_type_id
		and T.NAME not in ('sysname')
where 1 = 1
	and s.name + '.' + o.name = 'dbo.customer'
	and type_desc like 'USER_TABLE'
order by type_desc
	,s.name
	,o.name

```

