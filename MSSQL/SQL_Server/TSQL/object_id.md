# Проверка существования объекта БД

```sql
IF (OBJECT_ID('dbo.PK_ConstraintName', 'PK') IS NOT NULL)
```

> ```sql
> AF = Aggregate function (CLR)
> C = CHECK constraint
> D = DEFAULT (constraint or stand-alone)
> F = FOREIGN KEY constraint
> FN = SQL scalar function
> FS = Assembly (CLR) scalar-function
> FT = Assembly (CLR) table-valued function
> IF = SQL inline table-valued function
> IT = Internal table
> P = SQL Stored Procedure
> PC = Assembly (CLR) stored-procedure
> PG = Plan guide
> PK = PRIMARY KEY constraint
> R = Rule (old-style, stand-alone)
> RF = Replication-filter-procedure
> S = System base table
> SN = Synonym
> SO = Sequence object
> ```



### Переключение индексов

```sql
/*
/////////////  create rowstore index for join //////////////////////////////
*/

begin tran
	drop index if exists [ccix_product_sort_attributes] on [dbo].[product_sort_attributes]
		
	if not exists
	(
		select 1 from sys.indexes i
		where i.object_id = object_id('dbo.product_sort_attributes')
			and i.name = 'PK_product_sort_attributes'
			and i.type_desc = 'CLUSTERED'
			)
	begin
		alter table [dbo].[product_sort_attributes] drop constraint if exists [PK_product_sort_attributes]
		alter table [dbo].[product_sort_attributes] add constraint [PK_product_sort_attributes] primary key clustered
		(
		[item_key]
		) 
	end
commit


/*
///////  create columnstore index for analytical queries ////////////////////////
*/


begin tran 
	if not exists
	(
		select 1 from sys.indexes i
		where i.object_id = object_id('dbo.product_sort_attributes')
			and i.name = 'PK_product_sort_attributes'
			and i.type_desc = 'NONCLUSTERED'
			)
	begin
		alter table [dbo].[product_sort_attributes] drop constraint if exists [PK_product_sort_attributes]
		alter table [dbo].[product_sort_attributes] add constraint [PK_product_sort_attributes] primary key nonclustered
		(
		[item_key]
		)
	end

	create clustered columnstore index [ccix_product_sort_attributes] on [dbo].[product_sort_attributes]
commit
```



### Полезные ссылки:  

- [OBJECT_ID](https://docs.microsoft.com/ru-ru/sql/t-sql/functions/object-id-transact-sql?view=sql-server-ver15)  
- [sys.objects](https://docs.microsoft.com/ru-ru/sql/relational-databases/system-catalog-views/sys-objects-transact-sql?view=sql-server-ver15)  