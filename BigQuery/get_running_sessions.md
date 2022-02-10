### [Заметки по BigQuery](./big_query.md)

```sql
select user_email, query, priority
from `goods-161014`.`region-us`.INFORMATION_SCHEMA.JOBS_BY_PROJECT
where state = 'RUNNING'
```

[INFORMATION_SCHEMA](https://cloud.google.com/bigquery/docs/information-schema-jobs)
