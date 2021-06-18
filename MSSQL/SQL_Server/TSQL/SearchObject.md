# Поиск объекта по Серверу

Поиск по БД в MS SQL Server

```sql
select sch.name as schema_name, ob.name as object_name, *
from sys.schemas sch
left join sys.objects ob on ob.schema_id = sch.schema_id
where --sch.name like '%gbq%' or 
		ob.name like '%BlockStatistic%'
order by sch.name, ob.name, ob.type
```



Поиск в Google Big Query

```sql
SELECT *, DDL
FROM `goods-161014`.dwh_output.INFORMATION_SCHEMA.TABLES
where table_name like '%orders_sber_attribution%'
```

