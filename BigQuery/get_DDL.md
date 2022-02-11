### [Заметки по BigQuery](./big_query.md)

```sql
SELECT * 
FROM `goods-161014`.`region-us`.INFORMATION_SCHEMA.TABLES
where TABLE_NAME like 'oms_FactOrderPosition%'
```

[INFORMATION_SCHEMA](https://cloud.google.com/bigquery/docs/reference/standard-sql/dml-syntax)