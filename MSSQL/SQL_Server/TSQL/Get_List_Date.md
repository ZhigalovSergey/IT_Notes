# Получить даты за указанный период

```sql
declare @st date = '20180101', @fin date = getdate()

;
with cte_days as
(
select 1 as iter, @st as dt
union all 
select iter + 1 as iter, dateadd(DD, 1, dt) as dt
from cte_days
where dateadd(DD, 1, dt) < @fin
)
select *
from cte_days
OPTION (MAXRECURSION 10000)
```

