### Получить даты за указанный период

```sql
declare @st date = '20180101', @fin date = getdate()

;with cte_days as
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

### Получить даты начала недель за указанный период

```sql
declare @st date = '20180101', @fin date = getdate()

;with cte_weeks as
(
select 1 as iter, cast(dateadd(week, datediff(week, '19000101', @st), '19000101')  as date) as dt
union all 
select iter + 1 as iter, dateadd(week, 1, dt) as dt
from cte_weeks
where dateadd(week, 1, dt) < @fin
)
select dt
from cte_weeks
order by dt
OPTION (MAXRECURSION 10000)
```

### Получить даты начала месяца за указанный период

```sql
declare @st date = '20180101', @fin date = getdate()

;with cte_month as
(
select 1 as iter, dateadd(day, 1, eomonth(@st, -1)) as dt
union all 
select iter + 1 as iter, dateadd(month, 1, dt) as dt
from cte_month
where dateadd(month, 1, dt) < @fin
)
select dt
from cte_month
order by dt
OPTION (MAXRECURSION 10000)
```

