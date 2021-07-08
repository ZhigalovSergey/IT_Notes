# Сравнение двух таблиц по столбцам



```sql
declare list_columns cursor
for
 select
	 c.name
 from
	 sys.objects o
	 join sys.columns c on
		 o.object_id = c.object_id
	 join sys.schemas sch on
		 sch.schema_id = o.schema_id
 where
	 o.name = 'product_sort_attributes' and
	 sch.name = 'marketing' and
	 c.name <> 'article_number'

declare @column_name nvarchar(155), @sql nvarchar(4000)

open list_columns

fetch next from list_columns
into @column_name

while @@FETCH_STATUS = 0
begin
	select @column_name
	set @sql = 'select new.item_key ,old.article_number, new.article_number
		from marketing.product_sort_attributes old
		full join dbo.product_sort_attributes new
		on old.article_number = new.article_number
		where old.' +@column_name+ ' <> new.' +@column_name

	exec sp_executesql @sql

	fetch next from list_columns
	into @column_name
end
close list_columns
deallocate list_columns
```

