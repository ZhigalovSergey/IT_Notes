

```sql
declare @st date = '20170101', @fin date = '20210101'

;
with cte_month as
(
select 1 as iter, @st as m
union all 
select iter + 1 as iter, dateadd(DD, 1, m) as m
from cte_month
where dateadd(DD, 1, m) < @fin
)
select *
from cte_month
OPTION (MAXRECURSION 10000)
```

